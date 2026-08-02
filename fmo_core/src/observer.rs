use std::sync::Arc;
use std::time::Duration;

use anyhow::ensure;
use deadpool_postgres::{Pool, Runtime};
use fedimint_connectors::ConnectorRegistry;
use fedimint_core::config::FederationId;
use fedimint_core::encoding::Encodable;
use fedimint_core::invite_code::InviteCode;
use fedimint_core::task::TaskGroup;
use tokio_postgres::NoTls;
use tracing::{error, info_span, Instrument};

use crate::db::migrations::{setup_core_schema, setup_module_schema};
use crate::federation::Federation;
use crate::module::ModuleTaskCtx;
use crate::query::{query, query_opt};
use crate::registry::ModuleRegistry;
use crate::services::meta::ConsensusMetaCache;
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
}

impl FederationObserver {
    pub async fn new(
        database: &str,
        admin_auth: &str,
        mempool_url: &str,
        registry: ModuleRegistry,
    ) -> anyhow::Result<FederationObserver> {
        let pool = {
            let pool_config = deadpool_postgres::Config {
                url: Some(database.to_owned()),
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

        let observer = FederationObserver {
            pool: pool.clone(),
            registry: Arc::new(registry),
            services: Arc::new(CoreServices::new(mempool_url.to_owned(), pool)),
            connectors,
            admin_auth: admin_auth.to_owned(),
            task_group: Default::default(),
            consensus_meta_cache: Default::default(),
        };

        observer.seed_block_times().await?;

        for federation in observer.list_federations().await? {
            observer.spawn_federation(federation);
        }

        observer.task_group.spawn_cancellable(
            "fetch block times",
            observer.clone().fetch_block_times(),
        );
        observer.task_group.spawn_cancellable(
            "sync nostr events",
            observer.clone().sync_nostr_events(),
        );

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
            let observer = self.clone();
            let config = federation.config.clone();
            self.task_group.spawn_cancellable(
                format!("Fetcher for {federation_id}"),
                async move {
                    loop {
                        let e = crate::fetch::run_fetcher(
                            observer.pool.clone(),
                            observer.connectors.clone(),
                            federation_id,
                            config.clone(),
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

        for (kind, module) in self.registry.iter() {
            let module = module.clone();
            let ctx = ModuleTaskCtx {
                federation_id,
                config: federation.config.clone(),
                pool: self.pool.clone(),
                services: self.services.clone(),
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
