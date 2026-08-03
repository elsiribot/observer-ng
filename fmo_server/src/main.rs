use clap::Parser;
use fmo_core::{FedimintObserverBuilder, ServerOpts};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
enum Cmd {
    /// Run the observer server
    Serve(ServeArgs),
    /// Import federations, raw sessions and block times from a pre-v0.2
    /// (schema v8) Fedimint Observer database. Derived data is rebuilt by
    /// the regular replay machinery on the next `serve`.
    Import {
        /// Connection string of the old database to import from
        #[arg(long)]
        from: String,

        /// Connection string of the new database to import into
        #[arg(long, env = "FO_DATABASE")]
        database: String,
    },
}

#[derive(Parser, Debug)]
struct ServeArgs {
    #[arg(long, env = "FO_BIND", default_value = "127.0.0.1:3000")]
    bind: String,

    #[arg(long, env = "FO_DATABASE")]
    database: String,

    #[arg(long, env = "FO_ADMIN_AUTH")]
    admin_auth: String,

    #[arg(
        long,
        env = "FO_MEMPOOL_URL",
        default_value = "https://mempool.space/api"
    )]
    mempool_url: String,
}

/// The default Fedimint Observer: core observer plus the standard fedimint
/// modules. Module authors can build their own FMO flavor by depending on
/// `fmo_core` and their module crates; see `examples/custom_fmo.rs`.
fn builder() -> FedimintObserverBuilder {
    FedimintObserverBuilder::new()
        .with_module(fmo_module_mint::MintObserver)
        .with_module(fmo_module_wallet::WalletObserver)
        .with_module(fmo_module_walletv2::WalletV2Observer)
        .with_module(fmo_module_ln::LnObserver)
        .with_module(fmo_module_lnv2::LnV2Observer)
        // Legacy paths kept for the React frontend: module routers are
        // mounted a second time directly under /federations/:federation_id,
        // so e.g. wallet's "/utxos" also answers at its historical path.
        .with_compat_route("/federations/:federation_id", "wallet")
        .with_compat_route("/federations/:federation_id", "mint")
        .with_compat_route("/federations/:federation_id", "ln")
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(
            EnvFilter::builder()
                .with_default_directive("info".parse().unwrap())
                .from_env()
                .unwrap(),
        )
        .init();

    match Cmd::parse() {
        Cmd::Serve(args) => {
            tracing::info!("Starting API server on {}", args.bind);
            builder()
                .run(ServerOpts {
                    bind: args.bind,
                    database: args.database,
                    admin_auth: args.admin_auth,
                    mempool_url: args.mempool_url,
                })
                .await
        }
        Cmd::Import { from, database } => {
            fmo_core::import::import(&from, &database, &builder().registry()).await
        }
    }
}
