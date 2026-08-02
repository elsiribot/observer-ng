//! Test fixtures for observer module crates. Only available with the
//! `test-util` feature; not part of the stable API.

use std::collections::BTreeMap;
use std::sync::Arc;

use deadpool_postgres::{Config, Pool, Runtime};
use fedimint_core::config::{ClientConfig, GlobalClientConfig, PeerUrl};
use fedimint_core::config::FederationId;
use fedimint_core::encoding::Encodable;
use fedimint_core::module::CoreConsensusVersion;
use fedimint_core::PeerId;
use tokio_postgres::NoTls;

use crate::services::CoreServices;

/// Connection pool for the database given by `FMO_TEST_DATABASE`, if set.
pub fn test_pool() -> Option<Pool> {
    let url = std::env::var("FMO_TEST_DATABASE").ok()?;
    let cfg = Config {
        url: Some(url),
        ..Default::default()
    };
    Some(cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap())
}

/// Drops and recreates the public schema, then applies the core schema.
pub async fn reset_db(pool: &Pool) {
    let conn = pool.get().await.unwrap();
    conn.batch_execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        .await
        .unwrap();
    crate::db::migrations::setup_core_schema(pool).await.unwrap();
}

/// Minimal client config without any modules; enough for `ProcessCtx`.
pub fn minimal_config() -> (ClientConfig, FederationId) {
    let config = ClientConfig {
        global: GlobalClientConfig {
            api_endpoints: BTreeMap::from([(
                PeerId::from(0),
                PeerUrl {
                    url: "wss://example.com/".parse().expect("valid url"),
                    name: "peer0".to_owned(),
                },
            )]),
            broadcast_public_keys: None,
            consensus_version: CoreConsensusVersion::new(2, 0),
            meta: BTreeMap::new(),
        },
        modules: BTreeMap::new(),
    };
    let federation_id = config.global.calculate_federation_id();
    (config, federation_id)
}

/// Inserts the federation row required by foreign keys.
pub async fn insert_federation(pool: &Pool, config: &ClientConfig, federation_id: FederationId) {
    pool.get()
        .await
        .unwrap()
        .execute(
            "INSERT INTO federations VALUES ($1, $2) ON CONFLICT DO NOTHING",
            &[
                &federation_id.consensus_encode_to_vec(),
                &config.consensus_encode_to_vec(),
            ],
        )
        .await
        .unwrap();
}

/// Core services instance pointing at a dummy mempool URL.
pub fn test_services(pool: &Pool) -> Arc<CoreServices> {
    Arc::new(CoreServices::new("http://unused".to_owned(), pool.clone()))
}
