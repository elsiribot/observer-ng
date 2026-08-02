use std::sync::Arc;

use deadpool_postgres::Pool;

use crate::services::CoreServices;

/// State available to module-provided API routers.
#[derive(Clone)]
pub struct ModuleApiState {
    pub pool: Pool,
    pub services: Arc<CoreServices>,
}
