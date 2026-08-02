use std::sync::Arc;

use anyhow::Context;

use crate::module::ObserverModule;
use crate::observer::FederationObserver;
use crate::registry::ModuleRegistry;

/// Server options, typically populated from `FO_*` environment variables by
/// the binary's CLI layer.
#[derive(Debug, Clone)]
pub struct ServerOpts {
    pub bind: String,
    pub database: String,
    pub admin_auth: String,
    pub mempool_url: String,
}

/// Builds a Fedimint Observer instance from a set of observer modules:
///
/// ```ignore
/// FedimintObserverBuilder::new()
///     .with_module(MintObserver)
///     .with_module(WalletObserver)
///     .with_module(LnObserver)
///     .run(opts)
///     .await
/// ```
#[derive(Default)]
pub struct FedimintObserverBuilder {
    modules: Vec<Arc<dyn ObserverModule>>,
}

impl FedimintObserverBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_module(mut self, module: impl ObserverModule) -> Self {
        self.modules.push(Arc::new(module));
        self
    }

    /// The module registry resulting from the registered modules. Used by
    /// auxiliary commands (e.g. import) that need decoders but no server.
    pub fn registry(&self) -> ModuleRegistry {
        ModuleRegistry::new(self.modules.clone())
    }

    /// Sets up the database, spawns all observer tasks and serves the API.
    pub async fn run(self, opts: ServerOpts) -> anyhow::Result<()> {
        let registry = ModuleRegistry::new(self.modules);
        let observer = FederationObserver::new(
            &opts.database,
            &opts.admin_auth,
            &opts.mempool_url,
            registry,
        )
        .await?;

        let app = crate::api::build_router(observer);

        let listener = tokio::net::TcpListener::bind(&opts.bind)
            .await
            .context("Binding to port")?;

        axum::serve(listener, app)
            .await
            .context("Starting axum server")?;

        Ok(())
    }
}
