pub mod config;
pub mod consensus;
pub mod federations;
pub mod live;
pub mod sessions;
mod sql_fragments;
mod time_estimate;
pub mod transactions;
pub mod user_transactions;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use axum::extract::State;
use axum::routing::{get, put};
use axum::{Json, Router};
pub use config::FederationConfigCache;
use deadpool_postgres::Pool;
use fedimint_core::config::FederationId;
use fedimint_core::invite_code::InviteCode;
use tower_http::cors::CorsLayer;
use tracing::{debug, info, warn};

use crate::observer::FederationObserver;
use crate::services::meta::MetaOverrideCache;
use crate::services::CoreServices;

/// State available to core API handlers.
#[derive(Clone)]
pub struct AppState {
    pub observer: FederationObserver,
    pub federation_config_cache: FederationConfigCache,
    pub meta_override_cache: MetaOverrideCache,
}

/// State available to module-provided API routers.
#[derive(Clone)]
pub struct ModuleApiState {
    pub pool: Pool,
    pub services: Arc<CoreServices>,
}

/// Assembles the full API router: core routes plus every registered module's
/// router mounted under `/federations/:federation_id/modules/<kind>`, plus
/// compat shims that expose selected module routes under their pre-TODO
/// legacy paths.
///
/// `compat_routes` maps a public path prefix (e.g.
/// `/federations/:federation_id/utxos`) to a (module kind, module route
/// prefix) pair; the module's router is mounted a second time under the
/// public prefix.
pub fn build_router(observer: FederationObserver, compat_routes: &[(String, String)]) -> Router {
    let module_state = ModuleApiState {
        pool: observer.pool().clone(),
        services: observer.services().clone(),
    };

    let mut router = Router::new()
        .route("/health", get(|| async { "Server is up and running!" }))
        .nest("/config", config::get_config_routes())
        .nest("/federations", federations::get_federations_routes())
        // TODO: move into nostr service/module
        .route("/nostr/federations", get(get_nostr_federations))
        .route("/nostr/federations", put(publish_federation_event));

    let module_routers: BTreeMap<String, Router<ModuleApiState>> = observer
        .registry()
        .iter()
        .filter_map(|(kind, module)| {
            module
                .api_router()
                .map(|module_router| (kind.to_string(), module_router))
        })
        .collect();

    for (kind, module_router) in &module_routers {
        router = router.nest(
            &format!("/federations/:federation_id/modules/{kind}"),
            module_router.clone().with_state(module_state.clone()),
        );
    }

    for (public_prefix, kind) in compat_routes {
        let Some(module_router) = module_routers.get(kind) else {
            warn!("Compat route {public_prefix} points at module {kind} which has no API router");
            continue;
        };
        router = router.nest(
            public_prefix,
            module_router.clone().with_state(module_state.clone()),
        );
    }

    // Share the same override cache instance the observer/CoreServices use, so
    // override meta files are fetched once per URL across API handlers and
    // module tasks.
    let meta_override_cache = observer.services().meta_override_cache().clone();

    router.layer(CorsLayer::permissive()).with_state(AppState {
        observer,
        federation_config_cache: Default::default(),
        meta_override_cache,
    })
}

async fn get_nostr_federations(
    State(state): State<AppState>,
) -> crate::error::Result<Json<BTreeMap<FederationId, InviteCode>>> {
    let federation_map = state
        .observer
        .list_nostr_federations()
        .await?
        .into_iter()
        .map(|federation| (federation.federation_id, federation.invite_code))
        .collect();

    Ok(Json(federation_map))
}

async fn publish_federation_event(
    State(state): State<AppState>,
    Json(event): Json<nostr_sdk::Event>,
) -> crate::error::Result<()> {
    Ok(state.observer.submit_federation(event).await?)
}

impl FederationObserver {
    /// Periodically refreshes the core `session_times` table (incrementally)
    /// and every materialized view registered by a module. Interval
    /// configurable via `FO_REFRESH_INTERVAL_SECS` (default 60).
    pub(crate) async fn refresh_views(self) {
        let interval_secs = std::env::var("FO_REFRESH_INTERVAL_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(60);
        loop {
            let start = SystemTime::now();
            debug!("Refreshing views...");
            if let Err(e) = self.refresh_views_inner().await {
                warn!("Error while refreshing views: {e:?}");
            }
            let elapsed = start.elapsed().unwrap_or_default().as_secs_f64();
            info!("Views refresh completed in {elapsed:.2}s. Waiting for next refresh window");
            tokio::time::sleep(Duration::from_secs(interval_secs)).await;
        }
    }

    async fn refresh_views_inner(&self) -> anyhow::Result<()> {
        let conn = self.connection().await?;

        // Before refreshing the aggregates, fill in amounts that only balance
        // inference can provide, so histograms/totals include them.
        let (inferred_inputs, inferred_outputs) =
            crate::amounts::infer_missing_amounts(&conn).await?;
        if inferred_inputs > 0 || inferred_outputs > 0 {
            debug!("Inferred amounts for {inferred_inputs} inputs and {inferred_outputs} outputs");
        }

        // `session_times` (the source of `first_timestamp`) must be refreshed
        // BEFORE the gold self-heal reads it, and the gold layer's
        // `user_tx_daily` rollup must be refreshed AFTER the heal fills in the
        // timestamps/amounts the async enrichment above produced. So the order
        // is fixed: session_times -> heal_gold -> user_tx_daily (+ module
        // matviews). `session_times` is now an incrementally maintained table
        // (see db::session_times), not a matview.
        self.refresh_session_times().await?;

        // Repair gold rows the processor folded before their timestamp /
        // inferred amount existed (see gold::heal_gold).
        crate::gold::heal_gold(&conn).await?;

        // `federation_tx_daily` depends only on `session_times` (refreshed
        // above) + structural tables, so it's safe to refresh alongside the
        // gold rollup here.
        let mut matviews = vec!["user_tx_daily".to_owned(), "federation_tx_daily".to_owned()];
        for (_, module) in self.registry().iter() {
            matviews.extend(module.matviews().iter().map(|view| (*view).to_owned()));
        }
        for matview in matviews {
            conn.batch_execute(&format!("REFRESH MATERIALIZED VIEW CONCURRENTLY {matview}"))
                .await?;
        }

        // Refresh the fleet-wide guardian-health cache BEFORE totals, since
        // compute_totals reads it (to discount offline federations). Best-effort:
        // handlers fall back to computing on demand if the cache is empty.
        match self.compute_guardian_health_summary().await {
            Ok(health) => *self.cached_health_summary().write().await = Some(health),
            Err(e) => debug!("Error while refreshing cached guardian health: {e:?}"),
        }

        // Refresh the `/federations/totals` cache. Best-effort: a failure
        // here shouldn't abort the matview refresh cycle, since the totals
        // handler falls back to computing on demand if the cache is empty.
        match self.compute_totals().await {
            Ok(totals) => *self.cached_totals().write().await = Some(totals),
            Err(e) => debug!("Error while refreshing cached totals: {e:?}"),
        }

        // Refresh the fleet-wide federation-summary list (the home page).
        // Depends on the guardian-health cache refreshed above, so it runs
        // last. Best-effort: the handler falls back to computing on demand if
        // the cache is empty.
        match self.compute_federation_summaries().await {
            Ok(summaries) => *self.cached_federation_summaries().write().await = Some(summaries),
            Err(e) => debug!("Error while refreshing cached federation summaries: {e:?}"),
        }

        Ok(())
    }
}
