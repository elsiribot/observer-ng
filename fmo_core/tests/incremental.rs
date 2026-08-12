mod common;

use std::sync::Arc;

use common::{dummy_config, dummy_session, insert_federation, reset_db, test_pool, DB_LOCK};
use fedimint_core::core::{Decoder, DynInput, DynModuleConsensusItem, DynOutput, ModuleKind};
use fedimint_core::encoding::Encodable;
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
