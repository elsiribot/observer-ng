//! Example of a custom Fedimint Observer build.
//!
//! Third-party module authors don't need to fork FMO: depend on `fmo_core`
//! (plus the module crates you want) and write a `main` like this one,
//! adding your own `ObserverModule` implementation via `with_module`. Your
//! module gets its own Postgres schema, replays the already-fetched session
//! history on first start and can expose API routes under
//! `/federations/:federation_id/modules/<kind>`.

use clap::Parser;
use fmo_core::{FedimintObserverBuilder, ServerOpts};

#[derive(Parser, Debug)]
struct Args {
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    FedimintObserverBuilder::new()
        .with_module(fmo_module_mint::MintObserver)
        .with_module(fmo_module_wallet::WalletObserver)
        .with_module(fmo_module_ln::LnObserver)
        .with_module(fmo_module_lnv2::LnV2Observer)
        // .with_module(MyCustomObserver)   // <- your module here
        .run(ServerOpts {
            bind: args.bind,
            database: args.database,
            admin_auth: args.admin_auth,
            mempool_url: args.mempool_url,
        })
        .await
}
