#![allow(dead_code)]

use std::collections::BTreeMap;
use std::str::FromStr;

use deadpool_postgres::{Config, Runtime};
use fedimint_core::config::{
    ClientConfig, ClientModuleConfig, FederationId, GlobalClientConfig, PeerUrl,
};
use fedimint_core::core::{IntoDynInstance, ModuleKind};
use fedimint_core::encoding::Encodable;
use fedimint_core::epoch::ConsensusItem;
use fedimint_core::module::{AmountUnit, CoreConsensusVersion, ModuleConsensusVersion};
use fedimint_core::session_outcome::{AcceptedItem, SessionOutcome};
use fedimint_core::transaction::{Transaction, TransactionSignature};
use fedimint_core::{Amount, PeerId};
use fedimint_dummy_common::config::DummyClientConfig;
use fedimint_dummy_common::{
    DummyConsensusItem, DummyInput, DummyInputV1, DummyOutput, DummyOutputV1,
};
use tokio_postgres::NoTls;

/// Tests share one database; serialize DB-touching tests within a binary.
pub static DB_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub const DUMMY_INSTANCE_ID: u16 = 0;

pub fn test_pool() -> Option<deadpool_postgres::Pool> {
    let url = std::env::var("FMO_TEST_DATABASE").ok()?;
    let cfg = Config {
        url: Some(url),
        ..Default::default()
    };
    Some(cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap())
}

/// Drops and recreates the public schema, then applies the core schema.
pub async fn reset_db(pool: &deadpool_postgres::Pool) {
    let conn = pool.get().await.unwrap();
    conn.batch_execute(
        "DROP SCHEMA public CASCADE; CREATE SCHEMA public;
         DROP SCHEMA IF EXISTS fmo_dummy CASCADE;
         DROP SCHEMA IF EXISTS fmo_dummy2 CASCADE;",
    )
    .await
    .unwrap();
    fmo_core::db::migrations::setup_core_schema(pool)
        .await
        .unwrap();
}

/// Minimal client config with a single dummy module instance.
pub fn dummy_config() -> (ClientConfig, FederationId) {
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
        modules: BTreeMap::from([(
            DUMMY_INSTANCE_ID,
            ClientModuleConfig::from_typed(
                DUMMY_INSTANCE_ID,
                ModuleKind::from_static_str("dummy"),
                ModuleConsensusVersion::new(2, 0),
                DummyClientConfig {
                    tx_fee: Amount::ZERO,
                },
            )
            .expect("valid module config"),
        )]),
    };
    let federation_id = config.global.calculate_federation_id();
    (config, federation_id)
}

fn account_key() -> fedimint_core::secp256k1::PublicKey {
    fedimint_core::secp256k1::PublicKey::from_str(
        "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798",
    )
    .expect("valid pubkey")
}

/// A session with one dummy transaction (1 input, 1 output) and one dummy CI.
pub fn dummy_session(amount_msat: u64) -> SessionOutcome {
    let transaction = Transaction {
        inputs: vec![DummyInput::V0(DummyInputV1 {
            amount: Amount::from_msats(amount_msat),
            unit: AmountUnit::BITCOIN,
            account: account_key(),
        })
        .into_dyn(DUMMY_INSTANCE_ID.into())],
        outputs: vec![DummyOutput::V0(DummyOutputV1 {
            amount: Amount::from_msats(amount_msat),
            unit: AmountUnit::BITCOIN,
            account: account_key(),
        })
        .into_dyn(DUMMY_INSTANCE_ID.into())],
        nonce: amount_msat.to_le_bytes(),
        signatures: TransactionSignature::NaiveMultisig(vec![]),
    };

    SessionOutcome {
        items: vec![
            AcceptedItem {
                item: ConsensusItem::Transaction(transaction),
                peer: PeerId::from(0),
            },
            AcceptedItem {
                item: ConsensusItem::Module(DummyConsensusItem.into_dyn(DUMMY_INSTANCE_ID.into())),
                peer: PeerId::from(0),
            },
        ],
    }
}

/// Inserts the federation row required by foreign keys.
pub async fn insert_federation(
    pool: &deadpool_postgres::Pool,
    config: &ClientConfig,
    federation_id: FederationId,
) {
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
