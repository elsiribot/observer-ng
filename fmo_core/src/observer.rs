use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::ensure;
use deadpool_postgres::{Pool, Runtime};
use fedimint_connectors::ConnectorRegistry;
use fedimint_core::config::FederationId;
use fedimint_core::encoding::Encodable;
use fedimint_core::invite_code::InviteCode;
use fedimint_core::task::TaskGroup;
use tokio::sync::watch;
use tokio_postgres::NoTls;
use tracing::{error, info_span, Instrument};

use crate::db::migrations::{setup_core_schema, setup_module_schema};
use crate::federation::Federation;
use crate::live::Watermark;
use crate::module::ModuleTaskCtx;
use crate::query::{query, query_opt};
use crate::registry::ModuleRegistry;
use crate::services::meta::{ConsensusMetaCache, MetaOverrideCache};
use crate::services::CoreServices;

/// Module-agnostic observer core: owns the DB pool, the module registry and
/// the per-federation fetcher/processor/module tasks.
#[derive(Clone)]
pub struct FederationObserver {
    pool: Pool,
    registry: Arc<ModuleRegistry>,
    services: Arc<CoreServices>,
    connectors: ConnectorRegistry,
    admin_auth: String,
    task_group: TaskGroup,
    consensus_meta_cache: ConsensusMetaCache,
    /// Per-federation live-poll watermark receivers, populated as each
    /// federation's fetcher is spawned; consumed by the SSE handler (Task 5)
    /// via [`FederationObserver::live_watch`].
    live_states: Arc<Mutex<HashMap<FederationId, watch::Receiver<Watermark>>>>,
    /// Cached result of [`FederationObserver::compute_totals`], refreshed on
    /// the same cycle as the materialized views (`refresh_views_inner`) so
    /// the `/federations/totals` handler avoids a full-table scan on every
    /// request. `None` until the first refresh cycle completes.
    cached_totals: Arc<tokio::sync::RwLock<Option<fmo_api_types::FedimintTotals>>>,
    /// Cached fleet-wide guardian health, refreshed on the same cycle as the
    /// materialized views (`refresh_views_inner`). Recomputing it is a full
    /// scan of the append-only `guardian_health` table, and it's read on every
    /// home-page and `/summary` load, so serve it from cache. `None` until the
    /// first refresh cycle completes (callers fall back to computing on
    /// demand).
    cached_health_summary: Arc<
        tokio::sync::RwLock<
            Option<std::collections::BTreeMap<FederationId, fmo_api_types::FederationHealth>>,
        >,
    >,
}

impl FederationObserver {
    /// Builds the observer without spawning any background tasks — for
    /// tests (and embedding) that only exercise query methods against a
    /// pre-seeded database.
    pub async fn new_without_tasks(
        database: &str,
        admin_auth: &str,
        mempool_url: &str,
        registry: ModuleRegistry,
    ) -> anyhow::Result<FederationObserver> {
        let pool = {
            // Sized for many federations processing concurrently; the
            // deadpool default (10) starves the per-federation tasks. Every
            // federation keeps roughly two connections busy while catching up
            // (fetcher + processor), so scale via FO_DB_POOL_SIZE together
            // with PostgreSQL's max_connections when observing many
            // federations.
            let pool_size = std::env::var("FO_DB_POOL_SIZE")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(32);
            let pool_config = deadpool_postgres::Config {
                url: Some(database.to_owned()),
                pool: Some(deadpool_postgres::PoolConfig::new(pool_size)),
                ..Default::default()
            };
            pool_config.create_pool(Some(Runtime::Tokio1), NoTls)
        }?;

        setup_core_schema(&pool).await?;
        for (kind, module) in registry.iter() {
            setup_module_schema(&pool, kind.as_str(), module.version(), module.migrations())
                .await?;
        }

        let connectors = ConnectorRegistry::build_from_client_env()?.bind().await?;

        // Build the meta caches once and share the same instances between the
        // observer (consensus cache, read by `/summary` etc.), the API state
        // (override cache, `AppState::meta_override_cache`) and `CoreServices`
        // (both, exposed to module tasks via `merged_meta`), so meta is fetched
        // once per federation/URL across all of them.
        let consensus_meta_cache = ConsensusMetaCache::default();
        let meta_override_cache = MetaOverrideCache::default();

        let observer = FederationObserver {
            pool: pool.clone(),
            registry: Arc::new(registry),
            services: Arc::new(CoreServices::new(
                mempool_url.to_owned(),
                pool,
                consensus_meta_cache.clone(),
                meta_override_cache,
            )),
            connectors,
            admin_auth: admin_auth.to_owned(),
            task_group: Default::default(),
            consensus_meta_cache,
            live_states: Arc::new(Mutex::new(HashMap::new())),
            cached_totals: Arc::new(tokio::sync::RwLock::new(None)),
            cached_health_summary: Arc::new(tokio::sync::RwLock::new(None)),
        };

        Ok(observer)
    }

    pub async fn new(
        database: &str,
        admin_auth: &str,
        mempool_url: &str,
        registry: ModuleRegistry,
    ) -> anyhow::Result<FederationObserver> {
        let observer = Self::new_without_tasks(database, admin_auth, mempool_url, registry).await?;

        // One-time, idempotent index build for the consensus explorer, run
        // before spawning per-federation tasks so it doesn't race the
        // fetchers/processors for the pool. This IS awaited before serving,
        // so it blocks startup for its duration (potentially minutes, on the
        // 127M-row consensus_items table); `CREATE INDEX CONCURRENTLY` only
        // means the build itself is non-blocking to concurrent writers, not
        // that it's non-blocking to startup.
        crate::api::consensus::ensure_explorer_indexes(&observer.pool).await;

        observer.seed_block_times().await?;

        for federation in observer.list_federations().await? {
            observer.spawn_federation(federation);
        }

        observer
            .task_group
            .spawn_cancellable("fetch block times", observer.clone().fetch_block_times());
        observer
            .task_group
            .spawn_cancellable("sync nostr events", observer.clone().sync_nostr_events());
        observer
            .task_group
            .spawn_cancellable("refresh views", observer.clone().refresh_views());
        // Build the amount-inference partial indexes in the background (see
        // consensus::ensure_infer_indexes): awaiting them could let a slow
        // infer cycle stall startup.
        observer
            .task_group
            .spawn_cancellable("build infer indexes", {
                let pool = observer.pool.clone();
                async move { crate::api::consensus::ensure_infer_indexes(&pool).await }
            });

        Ok(observer)
    }

    pub fn pool(&self) -> &Pool {
        &self.pool
    }

    pub fn registry(&self) -> &Arc<ModuleRegistry> {
        &self.registry
    }

    pub fn services(&self) -> &Arc<CoreServices> {
        &self.services
    }

    pub fn connectors(&self) -> &ConnectorRegistry {
        &self.connectors
    }

    pub fn task_group(&self) -> &TaskGroup {
        &self.task_group
    }

    pub fn consensus_meta_cache(&self) -> &ConsensusMetaCache {
        &self.consensus_meta_cache
    }

    pub(crate) fn cached_totals(
        &self,
    ) -> &Arc<tokio::sync::RwLock<Option<fmo_api_types::FedimintTotals>>> {
        &self.cached_totals
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn cached_health_summary(
        &self,
    ) -> &Arc<
        tokio::sync::RwLock<
            Option<std::collections::BTreeMap<FederationId, fmo_api_types::FederationHealth>>,
        >,
    > {
        &self.cached_health_summary
    }

    /// Live-poll watermark receiver for `federation_id`, if its fetcher has
    /// been spawned (i.e. it's a known federation). Cloning a `watch`
    /// receiver is cheap and independent -- each caller (e.g. an SSE
    /// connection) gets its own cursor into the same broadcast state.
    pub fn live_watch(&self, federation_id: FederationId) -> Option<watch::Receiver<Watermark>> {
        self.live_states
            .lock()
            .unwrap()
            .get(&federation_id)
            .cloned()
    }

    pub(crate) async fn connection(&self) -> anyhow::Result<deadpool_postgres::Object> {
        Ok(self.pool.get().await?)
    }

    // FIXME: use middleware for auth and get it out of here
    pub fn check_auth(&self, bearer_token: &str) -> anyhow::Result<()> {
        ensure!(self.admin_auth == bearer_token, "Invalid bearer token");
        Ok(())
    }

    pub async fn list_federations(&self) -> anyhow::Result<Vec<Federation>> {
        query(&self.connection().await?, "SELECT * FROM federations", &[]).await
    }

    pub async fn get_federation(
        &self,
        federation_id: FederationId,
    ) -> anyhow::Result<Option<Federation>> {
        query_opt(
            &self.connection().await?,
            "SELECT * FROM federations WHERE federation_id = $1",
            &[&federation_id.consensus_encode_to_vec()],
        )
        .await
    }

    pub async fn add_federation(&self, invite: &InviteCode) -> anyhow::Result<FederationId> {
        let federation_id = invite.federation_id();

        if self.get_federation(federation_id).await?.is_some() {
            return Ok(federation_id);
        }

        let (config, _api) =
            fedimint_api_client::download_from_invite_code(&self.connectors, invite).await?;

        self.connection()
            .await?
            .execute(
                "INSERT INTO federations VALUES ($1, $2)",
                &[
                    &federation_id.consensus_encode_to_vec(),
                    &config.consensus_encode_to_vec(),
                ],
            )
            .await?;

        self.spawn_federation(Federation {
            federation_id,
            config,
        });

        Ok(federation_id)
    }

    /// Spawns the fetcher, processor and module background tasks for one
    /// federation. Fetcher and processor restart with backoff on errors.
    pub(crate) fn spawn_federation(&self, federation: Federation) {
        let federation_id = federation.federation_id;

        {
            let (wm_tx, wm_rx) = watch::channel(Watermark::default());
            self.live_states
                .lock()
                .unwrap()
                .insert(federation_id, wm_rx);

            let observer = self.clone();
            let config = federation.config.clone();
            self.task_group.spawn_cancellable(
                format!("Fetcher for {federation_id}"),
                async move {
                    // `wm_tx` is captured by this `async move` block and
                    // lives for the lifetime of the restart loop below
                    // (`watch::Sender` isn't `Clone`); each `run_fetcher`
                    // call just borrows it, so the watermark channel
                    // survives fetcher restarts.
                    loop {
                        let e = crate::fetch::run_fetcher(
                            observer.pool.clone(),
                            observer.connectors.clone(),
                            federation_id,
                            config.clone(),
                            observer.registry.clone(),
                            observer.services.clone(),
                            &wm_tx,
                        )
                        .await
                        .expect_err("fetcher task exited unexpectedly");
                        error!("Fetcher errored, restarting in 30s: {e}");
                        tokio::time::sleep(Duration::from_secs(30)).await;
                    }
                }
                .instrument(info_span!("fetcher", fed = %federation_id.to_prefix())),
            );
        }

        {
            let observer = self.clone();
            let config = federation.config.clone();
            self.task_group.spawn_cancellable(
                format!("Processor for {federation_id}"),
                async move {
                    loop {
                        let e = crate::dispatch::run_processor(
                            observer.pool.clone(),
                            observer.registry.clone(),
                            observer.services.clone(),
                            federation_id,
                            config.clone(),
                        )
                        .await
                        .expect_err("processor task exited unexpectedly");
                        error!("Processor errored, restarting in 30s: {e}");
                        tokio::time::sleep(Duration::from_secs(30)).await;
                    }
                }
                .instrument(info_span!("processor", fed = %federation_id.to_prefix())),
            );
        }

        {
            let observer = self.clone();
            let config = federation.config.clone();
            self.task_group.spawn_cancellable(
                format!("Health Monitor for {federation_id}"),
                async move {
                    loop {
                        let e = observer
                            .monitor_health(federation_id, config.clone())
                            .await
                            .expect_err("health monitor task exited unexpectedly");
                        error!("Health Monitor errored, restarting in 30s: {e}");
                        tokio::time::sleep(Duration::from_secs(30)).await;
                    }
                }
                .instrument(info_span!("health", fed = %federation_id.to_prefix())),
            );
        }

        {
            let observer = self.clone();
            self.task_group.spawn_cancellable(
                format!("gold {federation_id}"),
                async move {
                    loop {
                        let e = crate::gold::run_gold_processor(observer.clone(), federation_id)
                            .await
                            .expect_err("gold processor task exited unexpectedly");
                        error!("Gold processor errored, restarting in 30s: {e}");
                        tokio::time::sleep(Duration::from_secs(30)).await;
                    }
                }
                .instrument(info_span!("gold", fed = %federation_id.to_prefix())),
            );
        }

        {
            let observer = self.clone();
            self.task_group.spawn_cancellable(
                format!("session stats backfill {federation_id}"),
                async move {
                    let federation_id_bytes = federation_id.consensus_encode_to_vec();
                    // Self-terminating: once no gaps remain the backfill is
                    // done for good (ingest keeps new sessions' stats up to
                    // date), so the task simply idles/ends instead of
                    // looping forever like the other per-federation tasks.
                    loop {
                        match crate::session_stats::backfill_session_stats(
                            &observer.pool,
                            &federation_id_bytes,
                        )
                        .await
                        {
                            Ok(()) => break,
                            Err(e) => {
                                error!("Session stats backfill errored, restarting in 30s: {e}");
                                tokio::time::sleep(Duration::from_secs(30)).await;
                            }
                        }
                    }
                }
                .instrument(info_span!("session_stats_backfill", fed = %federation_id.to_prefix())),
            );
        }

        for (kind, module) in self.registry.iter() {
            let module = module.clone();
            let ctx = ModuleTaskCtx {
                federation_id,
                config: federation.config.clone(),
                pool: self.pool.clone(),
                services: self.services.clone(),
                connectors: self.connectors.clone(),
            };
            self.task_group.spawn_cancellable(
                format!("Module {kind} task for {federation_id}"),
                async move {
                    module.run_federation_task(ctx).await;
                }
                .instrument(info_span!("module_task", fed = %federation_id.to_prefix())),
            );
        }
    }
}
