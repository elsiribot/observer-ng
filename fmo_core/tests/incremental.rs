mod common;

use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;

use common::{dummy_config, dummy_session, insert_federation, reset_db, test_pool, DB_LOCK};
use fedimint_core::config::{
    ClientConfig, ClientModuleConfig, FederationId, GlobalClientConfig, PeerUrl,
};
use fedimint_core::core::{
    Decoder, DynInput, DynModuleConsensusItem, DynOutput, IntoDynInstance, ModuleKind,
};
use fedimint_core::encoding::Encodable;
use fedimint_core::epoch::ConsensusItem;
use fedimint_core::module::{AmountUnit, CoreConsensusVersion, ModuleConsensusVersion};
use fedimint_core::session_outcome::AcceptedItem;
use fedimint_core::transaction::{Transaction, TransactionSignature};
use fedimint_core::{Amount, PeerId};
use fedimint_dummy_common::config::DummyClientConfig;
use fedimint_dummy_common::{DummyConsensusItem, DummyInput, DummyOutput};
use fmo_core::dispatch::dispatch_items_to_module;
use fmo_core::ingest::ingest_items;
use fmo_core::module::{CiMeta, ItemMeta, Migration, ObserverModule, ProcessCtx, ProcessedItem};
use fmo_core::registry::ModuleRegistry;
use fmo_core::services::CoreServices;
use serde_json::json;

/// Same shape as `pipeline.rs`'s `TestModule`: records every item it sees in
/// its own `seen` table so tests can assert whether dispatch actually ran.
struct TestModule;

#[async_trait::async_trait]
impl ObserverModule for TestModule {
    fn kind(&self) -> ModuleKind {
        ModuleKind::from_static_str("dummy")
    }

    fn decoder(&self) -> Decoder {
        use fedimint_core::module::CommonModuleInit;
        fedimint_dummy_common::DummyCommonInit::decoder()
    }

    fn version(&self) -> u32 {
        1
    }

    fn migrations(&self) -> &'static [Migration] {
        &[Migration {
            sql: "CREATE TABLE seen (session_index INTEGER NOT NULL, what TEXT NOT NULL);",
        }]
    }

    async fn process_input(
        &self,
        ctx: &mut ProcessCtx<'_>,
        _input: &DynInput,
        meta: &ItemMeta,
    ) -> anyhow::Result<ProcessedItem> {
        ctx.dbtx
            .execute(
                "INSERT INTO seen VALUES ($1, 'input')",
                &[&(meta.session_index as i32)],
            )
            .await?;
        Ok(ProcessedItem {
            amount: Some(fedimint_core::Amount::from_msats(42)),
            details: Some(json!({"t": "i"})),
        })
    }

    async fn process_output(
        &self,
        ctx: &mut ProcessCtx<'_>,
        _output: &DynOutput,
        meta: &ItemMeta,
    ) -> anyhow::Result<ProcessedItem> {
        ctx.dbtx
            .execute(
                "INSERT INTO seen VALUES ($1, 'output')",
                &[&(meta.session_index as i32)],
            )
            .await?;
        Ok(ProcessedItem {
            amount: Some(fedimint_core::Amount::from_msats(42)),
            details: Some(json!({"t": "o"})),
        })
    }

    async fn process_ci(
        &self,
        ctx: &mut ProcessCtx<'_>,
        _ci: &DynModuleConsensusItem,
        meta: &CiMeta,
    ) -> anyhow::Result<Option<serde_json::Value>> {
        ctx.dbtx
            .execute(
                "INSERT INTO seen VALUES ($1, 'ci')",
                &[&(meta.session_index as i32)],
            )
            .await?;
        Ok(Some(json!({"t": "ci"})))
    }
}

/// Foundation test for the SP-2 live view: an open (data-NULL) session is a
/// valid row (FK-wise other silver facts, like a consensus item, can already
/// reference it) but `process_pending` must not touch it until the session
/// signs and `data` is filled in. Once `data` is set, the exact same call
/// picks it up and processes it -- live ingestion and historical replay stay
/// the same code path.
#[tokio::test]
async fn process_pending_skips_open_session_until_data_is_set() {
    let _guard = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;
    pool.get()
        .await
        .unwrap()
        .batch_execute("DROP SCHEMA IF EXISTS fmo_dummy CASCADE;")
        .await
        .unwrap();

    let (config, federation_id) = dummy_config();
    insert_federation(&pool, &config, federation_id).await;
    let fed = federation_id.consensus_encode_to_vec();

    // Open session: data is NULL, as a live-ingested, not-yet-signed session
    // would be. A consensus item can already reference it via the
    // (federation_id, session_index) FK, since other structural facts can be
    // known before the session signs.
    let conn = pool.get().await.unwrap();
    conn.execute(
        "INSERT INTO sessions (federation_id, session_index, data) VALUES ($1, 0, NULL)",
        &[&fed],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO consensus_items (federation_id, session_index, item_index, peer_id, kind)
         VALUES ($1, 0, 0, 0, 'dummy')",
        &[&fed],
    )
    .await
    .unwrap();
    drop(conn);

    let services = Arc::new(CoreServices::new("http://unused".to_owned(), pool.clone()));
    let module: Arc<dyn ObserverModule> = Arc::new(TestModule);
    let registry = ModuleRegistry::new(vec![module]);
    fmo_core::db::migrations::setup_module_schema(
        &pool,
        "dummy",
        1,
        &[Migration {
            sql: "CREATE TABLE seen (session_index INTEGER NOT NULL, what TEXT NOT NULL);",
        }],
    )
    .await
    .unwrap();

    // The open session must be entirely skipped: no (module, session) units
    // processed.
    let processed = fmo_core::dispatch::process_pending(
        &pool,
        &registry,
        &services,
        federation_id,
        &config,
        100,
    )
    .await
    .unwrap();
    assert_eq!(processed, 0, "open session must not be processed");

    let conn = pool.get().await.unwrap();
    let progress: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM module_progress WHERE module_kind = 'dummy' AND federation_id = $1",
            &[&fed],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        progress, 0,
        "cursor must not advance past an open session (no row yet == still at 0)"
    );
    let seen: i64 = conn
        .query_one("SELECT COUNT(*) FROM fmo_dummy.seen", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(seen, 0, "module must not have processed anything");
    drop(conn);

    // The session signs: data is filled in with a real, decodable
    // SessionOutcome.
    let session = dummy_session(1_000);
    let data = session.consensus_encode_to_vec();
    let conn = pool.get().await.unwrap();
    conn.execute(
        "UPDATE sessions SET data = $1 WHERE federation_id = $2 AND session_index = 0",
        &[&data, &fed],
    )
    .await
    .unwrap();
    drop(conn);

    // The same call now picks the session up: live ingestion and historical
    // replay are the same code path.
    let processed = fmo_core::dispatch::process_pending(
        &pool,
        &registry,
        &services,
        federation_id,
        &config,
        100,
    )
    .await
    .unwrap();
    assert_eq!(processed, 1, "one (module, session) unit now processed");

    let conn = pool.get().await.unwrap();
    let cursor: i32 = conn
        .query_one(
            "SELECT next_session_index FROM module_progress
             WHERE module_kind = 'dummy' AND federation_id = $1",
            &[&fed],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(cursor, 1, "cursor advanced past the now-signed session");
    let seen: i64 = conn
        .query_one("SELECT COUNT(*) FROM fmo_dummy.seen", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(seen, 3, "1 session x (input, output, ci) now processed");
}

/// A federation config with two dummy-backed module instances under
/// different kinds ("dummy" at instance 0, "dummy2" at instance 1), so a
/// fixture can contain consensus items of two distinct kinds. `url`
/// distinguishes the federation id (`calculate_federation_id` hashes
/// `api_endpoints`), so two calls with different urls give two federations
/// that can coexist in the same database.
fn two_module_config(url: &str) -> (ClientConfig, FederationId) {
    let config = ClientConfig {
        global: GlobalClientConfig {
            api_endpoints: BTreeMap::from([(
                PeerId::from(0),
                PeerUrl {
                    url: url.parse().expect("valid url"),
                    name: "peer0".to_owned(),
                },
            )]),
            broadcast_public_keys: None,
            consensus_version: CoreConsensusVersion::new(2, 0),
            meta: BTreeMap::new(),
        },
        modules: BTreeMap::from([
            (
                0,
                ClientModuleConfig::from_typed(
                    0,
                    ModuleKind::from_static_str("dummy"),
                    ModuleConsensusVersion::new(2, 0),
                    DummyClientConfig,
                )
                .expect("valid module config"),
            ),
            (
                1,
                ClientModuleConfig::from_typed(
                    1,
                    ModuleKind::from_static_str("dummy2"),
                    ModuleConsensusVersion::new(2, 0),
                    DummyClientConfig,
                )
                .expect("valid module config"),
            ),
        ]),
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

/// 2 transactions (instance 0, kind "dummy") + 2 module CIs of two kinds
/// ("dummy" at instance 0, "dummy2" at instance 1) -- the fixture the
/// equivalence test below ingests/dispatches in one shot vs. in two slices.
fn two_tx_two_ci_items() -> Vec<AcceptedItem> {
    let make_tx = |amount_msat: u64| Transaction {
        inputs: vec![DummyInput {
            amount: Amount::from_msats(amount_msat),
            unit: AmountUnit::BITCOIN,
            pub_key: account_key(),
        }
        .into_dyn(0)],
        outputs: vec![DummyOutput {
            amount: Amount::from_msats(amount_msat),
            unit: AmountUnit::BITCOIN,
        }
        .into_dyn(0)],
        nonce: amount_msat.to_le_bytes(),
        signatures: TransactionSignature::NaiveMultisig(vec![]),
    };

    vec![
        AcceptedItem {
            item: ConsensusItem::Transaction(make_tx(9_001)),
            peer: PeerId::from(0),
        },
        AcceptedItem {
            item: ConsensusItem::Transaction(make_tx(9_002)),
            peer: PeerId::from(0),
        },
        AcceptedItem {
            item: ConsensusItem::Module(DummyConsensusItem.into_dyn(0)),
            peer: PeerId::from(0),
        },
        AcceptedItem {
            item: ConsensusItem::Module(DummyConsensusItem.into_dyn(1)),
            peer: PeerId::from(0),
        },
    ]
}

/// Records every item it processes, scoped by `federation_id`, so the same
/// module schema can be shared by two federations being compared in the
/// equivalence test below without their rows colliding on `session_index`
/// alone (both federations use session_index 0).
struct RecordingModule {
    kind: &'static str,
}

#[async_trait::async_trait]
impl ObserverModule for RecordingModule {
    fn kind(&self) -> ModuleKind {
        ModuleKind::from_static_str(self.kind)
    }

    fn decoder(&self) -> Decoder {
        use fedimint_core::module::CommonModuleInit;
        fedimint_dummy_common::DummyCommonInit::decoder()
    }

    fn version(&self) -> u32 {
        1
    }

    fn migrations(&self) -> &'static [Migration] {
        &[Migration {
            sql: "CREATE TABLE seen (
                federation_id BYTEA NOT NULL,
                session_index INTEGER NOT NULL,
                item_index INTEGER NOT NULL,
                what TEXT NOT NULL
            );",
        }]
    }

    async fn process_input(
        &self,
        ctx: &mut ProcessCtx<'_>,
        _input: &DynInput,
        meta: &ItemMeta,
    ) -> anyhow::Result<ProcessedItem> {
        ctx.dbtx
            .execute(
                "INSERT INTO seen VALUES ($1, $2, $3, 'input')",
                &[
                    &ctx.federation_id.consensus_encode_to_vec(),
                    &(meta.session_index as i32),
                    &(meta.item_index as i32),
                ],
            )
            .await?;
        Ok(ProcessedItem {
            amount: Some(Amount::from_msats(7)),
            details: Some(json!({"t": "i"})),
        })
    }

    async fn process_output(
        &self,
        ctx: &mut ProcessCtx<'_>,
        _output: &DynOutput,
        meta: &ItemMeta,
    ) -> anyhow::Result<ProcessedItem> {
        ctx.dbtx
            .execute(
                "INSERT INTO seen VALUES ($1, $2, $3, 'output')",
                &[
                    &ctx.federation_id.consensus_encode_to_vec(),
                    &(meta.session_index as i32),
                    &(meta.item_index as i32),
                ],
            )
            .await?;
        Ok(ProcessedItem {
            amount: Some(Amount::from_msats(7)),
            details: Some(json!({"t": "o"})),
        })
    }

    async fn process_ci(
        &self,
        ctx: &mut ProcessCtx<'_>,
        _ci: &DynModuleConsensusItem,
        meta: &CiMeta,
    ) -> anyhow::Result<Option<serde_json::Value>> {
        ctx.dbtx
            .execute(
                "INSERT INTO seen VALUES ($1, $2, $3, 'ci')",
                &[
                    &ctx.federation_id.consensus_encode_to_vec(),
                    &(meta.session_index as i32),
                    &(meta.item_index as i32),
                ],
            )
            .await?;
        Ok(Some(json!({"t": "ci"})))
    }
}

/// Dispatches `items[start..]` to `module` in its own transaction, setting
/// `search_path` to the module's schema the way `process_module_batch` does
/// in `dispatch.rs`.
async fn dispatch_in_own_tx(
    pool: &deadpool_postgres::Pool,
    module: &dyn ObserverModule,
    services: &Arc<CoreServices>,
    federation_id: FederationId,
    config: &ClientConfig,
    items: &[AcceptedItem],
    start: usize,
) {
    let mut conn = pool.get().await.unwrap();
    let dbtx = conn.transaction().await.unwrap();
    dbtx.batch_execute(&format!(
        "SET LOCAL search_path TO {}, public",
        fmo_core::db::migrations::schema_name(module.kind().as_str())
    ))
    .await
    .unwrap();
    dispatch_items_to_module(
        &dbtx,
        module,
        services,
        federation_id,
        config,
        0,
        items,
        start,
    )
    .await
    .unwrap();
    dbtx.commit().await.unwrap();
}

/// Core equivalence test for the SP-2 live-ingest refactor: processing a
/// session's items in one shot (`start = 0` over the whole list) must
/// produce byte-for-byte identical rows to processing it in two slices
/// (`items[..k]` then `items` from `k`), the way the live poller will call
/// these functions as new items arrive within a still-open session.
///
/// Run twice in separate federations (whole-list vs. split), then diff the
/// resulting `transactions`/`transaction_inputs`/`transaction_outputs`/
/// `consensus_items`/`session_stats` rows and the modules' own silver rows.
#[tokio::test]
async fn ingest_items_and_dispatch_items_are_start_aware_equivalents() {
    let _guard = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;
    pool.get()
        .await
        .unwrap()
        .batch_execute(
            "DROP SCHEMA IF EXISTS fmo_dummy CASCADE; DROP SCHEMA IF EXISTS fmo_dummy2 CASCADE;",
        )
        .await
        .unwrap();

    let (config_a, fed_a) = two_module_config("wss://example-a.com/");
    let (config_b, fed_b) = two_module_config("wss://example-b.com/");
    assert_ne!(
        fed_a, fed_b,
        "test setup: the two scenarios must use distinct federations"
    );
    insert_federation(&pool, &config_a, fed_a).await;
    insert_federation(&pool, &config_b, fed_b).await;

    let module_dummy: Arc<dyn ObserverModule> = Arc::new(RecordingModule { kind: "dummy" });
    let module_dummy2: Arc<dyn ObserverModule> = Arc::new(RecordingModule { kind: "dummy2" });
    fmo_core::db::migrations::setup_module_schema(&pool, "dummy", 1, module_dummy.migrations())
        .await
        .unwrap();
    fmo_core::db::migrations::setup_module_schema(&pool, "dummy2", 1, module_dummy2.migrations())
        .await
        .unwrap();
    let services = Arc::new(CoreServices::new("http://unused".to_owned(), pool.clone()));

    let items = two_tx_two_ci_items();
    // Splits the two module CIs across the two `ingest_items`/dispatch
    // calls, so the boundary crosses tx-vs-ci and dummy-vs-dummy2.
    let k = 3;

    // (a) whole-list ingest + dispatch, one call each.
    {
        let mut conn = pool.get().await.unwrap();
        let dbtx = conn.transaction().await.unwrap();
        ingest_items(&dbtx, &config_a, fed_a, 0, &items, 0)
            .await
            .unwrap();
        dbtx.commit().await.unwrap();

        for module in [&module_dummy, &module_dummy2] {
            dispatch_in_own_tx(
                &pool,
                module.as_ref(),
                &services,
                fed_a,
                &config_a,
                &items,
                0,
            )
            .await;
        }
    }

    // (b) split ingest + dispatch: items[..k] first, then the full list
    // starting at k -- the shape the live poller will use as a session
    // fills in.
    {
        let mut conn = pool.get().await.unwrap();
        let dbtx = conn.transaction().await.unwrap();
        ingest_items(&dbtx, &config_b, fed_b, 0, &items[..k], 0)
            .await
            .unwrap();
        dbtx.commit().await.unwrap();

        // Mid-stream: session_stats reflects only items[..k] so far -- this
        // is expected, not a bug (see the ingest_items doc comment).
        {
            let conn = pool.get().await.unwrap();
            let fed_b_bytes = fed_b.consensus_encode_to_vec();
            let row = conn
                .query_one(
                    "SELECT tx_count, ci_count FROM session_stats
                     WHERE federation_id = $1 AND session_index = 0",
                    &[&fed_b_bytes],
                )
                .await
                .unwrap();
            assert_eq!(
                row.get::<_, i32>(0),
                2,
                "partial tx_count reflects items[..k]"
            );
            assert_eq!(
                row.get::<_, i32>(1),
                1,
                "partial ci_count reflects items[..k]"
            );
        }

        let mut conn = pool.get().await.unwrap();
        let dbtx = conn.transaction().await.unwrap();
        ingest_items(&dbtx, &config_b, fed_b, 0, &items, k)
            .await
            .unwrap();
        dbtx.commit().await.unwrap();

        for module in [&module_dummy, &module_dummy2] {
            dispatch_in_own_tx(
                &pool,
                module.as_ref(),
                &services,
                fed_b,
                &config_b,
                &items[..k],
                0,
            )
            .await;
            dispatch_in_own_tx(
                &pool,
                module.as_ref(),
                &services,
                fed_b,
                &config_b,
                &items,
                k,
            )
            .await;
        }
    }

    let conn = pool.get().await.unwrap();
    let fed_a_bytes = fed_a.consensus_encode_to_vec();
    let fed_b_bytes = fed_b.consensus_encode_to_vec();

    // transactions: same txid set, same session/item indices, same encoded
    // data between the whole-list and split runs.
    let tx_query = "SELECT txid, session_index, item_index, data FROM transactions
                     WHERE federation_id = $1 ORDER BY txid";
    let tx_a = conn
        .query(tx_query, &[&fed_a_bytes])
        .await
        .unwrap()
        .iter()
        .map(|r| {
            (
                r.get::<_, Vec<u8>>(0),
                r.get::<_, i32>(1),
                r.get::<_, i32>(2),
                r.get::<_, Vec<u8>>(3),
            )
        })
        .collect::<Vec<_>>();
    let tx_b = conn
        .query(tx_query, &[&fed_b_bytes])
        .await
        .unwrap()
        .iter()
        .map(|r| {
            (
                r.get::<_, Vec<u8>>(0),
                r.get::<_, i32>(1),
                r.get::<_, i32>(2),
                r.get::<_, Vec<u8>>(3),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        tx_a, tx_b,
        "transactions differ between whole-list and split runs"
    );
    assert_eq!(tx_a.len(), 2);

    // transaction_inputs / transaction_outputs: amounts + details written
    // back by dispatch must match too.
    for (table, index_col) in [
        ("transaction_inputs", "in_index"),
        ("transaction_outputs", "out_index"),
    ] {
        let query = format!(
            "SELECT txid, {index_col}, kind, amount_msat, details FROM {table}
             WHERE federation_id = $1 ORDER BY txid, {index_col}"
        );
        let rows_a = conn
            .query(&query, &[&fed_a_bytes])
            .await
            .unwrap()
            .iter()
            .map(|r| {
                (
                    r.get::<_, Vec<u8>>(0),
                    r.get::<_, i32>(1),
                    r.get::<_, String>(2),
                    r.get::<_, Option<i64>>(3),
                    r.get::<_, Option<serde_json::Value>>(4),
                )
            })
            .collect::<Vec<_>>();
        let rows_b = conn
            .query(&query, &[&fed_b_bytes])
            .await
            .unwrap()
            .iter()
            .map(|r| {
                (
                    r.get::<_, Vec<u8>>(0),
                    r.get::<_, i32>(1),
                    r.get::<_, String>(2),
                    r.get::<_, Option<i64>>(3),
                    r.get::<_, Option<serde_json::Value>>(4),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            rows_a, rows_b,
            "{table} differs between whole-list and split runs"
        );
        assert_eq!(rows_a.len(), 2, "{table} row count");
        assert!(
            rows_a.iter().all(|r| r.3 == Some(7)),
            "{table} amounts written back by dispatch"
        );
    }

    // consensus_items: both CIs, same kind + details.
    let ci_query = "SELECT item_index, kind, details FROM consensus_items
                     WHERE federation_id = $1 ORDER BY item_index";
    let ci_a = conn
        .query(ci_query, &[&fed_a_bytes])
        .await
        .unwrap()
        .iter()
        .map(|r| {
            (
                r.get::<_, i32>(0),
                r.get::<_, String>(1),
                r.get::<_, Option<serde_json::Value>>(2),
            )
        })
        .collect::<Vec<_>>();
    let ci_b = conn
        .query(ci_query, &[&fed_b_bytes])
        .await
        .unwrap()
        .iter()
        .map(|r| {
            (
                r.get::<_, i32>(0),
                r.get::<_, String>(1),
                r.get::<_, Option<serde_json::Value>>(2),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        ci_a, ci_b,
        "consensus_items differ between whole-list and split runs"
    );
    assert_eq!(ci_a.len(), 2);
    assert_eq!(ci_a[0].1, "dummy");
    assert_eq!(ci_a[1].1, "dummy2");
    assert!(ci_a.iter().all(|r| r.2 == Some(json!({"t": "ci"}))));

    // session_stats: full counts in both, now that the split run has passed
    // the whole list in its second `ingest_items` call.
    for bytes in [&fed_a_bytes, &fed_b_bytes] {
        let row = conn
            .query_one(
                "SELECT tx_count, ci_count, items_by_kind FROM session_stats
                 WHERE federation_id = $1 AND session_index = 0",
                &[bytes],
            )
            .await
            .unwrap();
        assert_eq!(row.get::<_, i32>(0), 2, "final tx_count");
        assert_eq!(row.get::<_, i32>(1), 2, "final ci_count");
        assert_eq!(
            row.get::<_, serde_json::Value>(2),
            json!({"dummy": 1, "dummy2": 1}),
            "final items_by_kind"
        );
    }

    // Module silver rows: each module's own `seen` table must end up
    // identical between the two federations too.
    for schema in ["fmo_dummy", "fmo_dummy2"] {
        let query = format!(
            "SELECT session_index, item_index, what FROM {schema}.seen
             WHERE federation_id = $1 ORDER BY item_index, what"
        );
        let seen_a = conn
            .query(&query, &[&fed_a_bytes])
            .await
            .unwrap()
            .iter()
            .map(|r| {
                (
                    r.get::<_, i32>(0),
                    r.get::<_, i32>(1),
                    r.get::<_, String>(2),
                )
            })
            .collect::<Vec<_>>();
        let seen_b = conn
            .query(&query, &[&fed_b_bytes])
            .await
            .unwrap()
            .iter()
            .map(|r| {
                (
                    r.get::<_, i32>(0),
                    r.get::<_, i32>(1),
                    r.get::<_, String>(2),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            seen_a, seen_b,
            "{schema}.seen differs between whole-list and split runs"
        );
    }
}
