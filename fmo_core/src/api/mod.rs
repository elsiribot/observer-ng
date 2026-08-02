use std::sync::Arc;

use axum::routing::get;
use axum::Router;
use deadpool_postgres::Pool;

use crate::observer::FederationObserver;
use crate::services::CoreServices;

/// State available to module-provided API routers.
#[derive(Clone)]
pub struct ModuleApiState {
    pub pool: Pool,
    pub services: Arc<CoreServices>,
}

/// Assembles the full API router: core routes plus every registered module's
/// router mounted under `/federations/:federation_id/modules/<kind>`.
pub fn build_router(observer: FederationObserver) -> Router {
    let module_state = ModuleApiState {
        pool: observer.pool().clone(),
        services: observer.services().clone(),
    };

    let mut router = Router::new().route("/health", get(|| async { "Server is up and running!" }));

    for (kind, module) in observer.registry().iter() {
        if let Some(module_router) = module.api_router() {
            router = router.nest(
                &format!("/federations/:federation_id/modules/{kind}"),
                module_router.with_state(module_state.clone()),
            );
        }
    }

    router
}
