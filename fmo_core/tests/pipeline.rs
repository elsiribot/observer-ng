mod common;

use std::sync::Arc;

use fedimint_core::core::{Decoder, DynInput, DynModuleConsensusItem, DynOutput, ModuleKind};
use fedimint_core::encoding::Encodable;
use fedimint_core::module::registry::ModuleDecoderRegistry;
use fedimint_core::Amount;
use fmo_core::module::{
    CiMeta, ItemMeta, Migration, ObserverModule, ProcessCtx, ProcessedItem,
};
use fmo_core::registry::ModuleRegistry;
use fmo_core::services::CoreServices;
use serde_json::json;

use common::{dummy_config, dummy_session, insert_federation, reset_db, test_pool, DB_LOCK};

/// Observer module for the dummy fedimint module used in tests. Its kind is
/// configurable so tests can register "two different" modules.
struct TestModule {
    kind: &'static str,
}

#[async_trait::async_trait]
impl ObserverModule for TestModule {
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
            amount: Some(Amount::from_msats(42)),
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
            amount: Some(Amount::from_msats(42)),
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

// The dummy fixture uses instance id 0 with kind "dummy"; a TestModule with a
// different kind never matches any items but still advances its cursor.
#[tokio::test]
async fn dispatch_processes_and_replays() {
    let _guard = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;

    let (config, federation_id) = dummy_config();
    insert_federation(&pool, &config, federation_id).await;
    let fed = federation_id.consensus_encode_to_vec();

    // ingest 3 sessions
    for session_index in 0..3u64 {
        let session = dummy_session(1_000 + session_index);
        let mut conn = pool.get().await.unwrap();
        let dbtx = conn.transaction().await.unwrap();
        fmo_core::ingest::ingest_session(&dbtx, &config, federation_id, session_index, &session)
            .await
            .unwrap();
        dbtx.commit().await.unwrap();
    }

    let services = Arc::new(CoreServices::new("http://unused".to_owned(), pool.clone()));

    let module: Arc<dyn ObserverModule> = Arc::new(TestModule { kind: "dummy" });
    let registry = ModuleRegistry::new(vec![module]);
    fmo_core::db::migrations::setup_module_schema(&pool, "dummy", 1, &[Migration {
        sql: "CREATE TABLE seen (session_index INTEGER NOT NULL, what TEXT NOT NULL);",
    }])
    .await
    .unwrap();

    let processed =
        fmo_core::dispatch::process_pending(&pool, &registry, &services, federation_id, &config, 100)
            .await
            .unwrap();
    assert_eq!(processed, 3);

    let conn = pool.get().await.unwrap();
    // module tables written
    let seen: i64 = conn
        .query_one("SELECT COUNT(*) FROM fmo_dummy.seen", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(seen, 9, "3 sessions x (input, output, ci)");
    // amounts + details written back into core tables by core, not the module
    let amounts: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM transaction_inputs
             WHERE federation_id = $1 AND amount_msat = 42 AND details->>'t' = 'i'",
            &[&fed],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(amounts, 3);
    let ci_details: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM consensus_items
             WHERE federation_id = $1 AND details->>'t' = 'ci'",
            &[&fed],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(ci_details, 3);
    // cursor advanced
    let cursor: i32 = conn
        .query_one(
            "SELECT next_session_index FROM module_progress
             WHERE module_kind = 'dummy' AND federation_id = $1",
            &[&fed],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(cursor, 3);
    drop(conn);

    // caught up: second run is a no-op
    let processed =
        fmo_core::dispatch::process_pending(&pool, &registry, &services, federation_id, &config, 100)
            .await
            .unwrap();
    assert_eq!(processed, 0);

    // a module registered later replays from session 0
    let late_module: Arc<dyn ObserverModule> = Arc::new(TestModule { kind: "dummy2" });
    let registry = ModuleRegistry::new(vec![
        Arc::new(TestModule { kind: "dummy" }) as Arc<dyn ObserverModule>,
        late_module,
    ]);
    fmo_core::db::migrations::setup_module_schema(&pool, "dummy2", 1, &[Migration {
        sql: "CREATE TABLE seen (session_index INTEGER NOT NULL, what TEXT NOT NULL);",
    }])
    .await
    .unwrap();

    let processed =
        fmo_core::dispatch::process_pending(&pool, &registry, &services, federation_id, &config, 100)
            .await
            .unwrap();
    assert_eq!(processed, 3, "late module replays all 3 sessions");

    let conn = pool.get().await.unwrap();
    let cursor: i32 = conn
        .query_one(
            "SELECT next_session_index FROM module_progress
             WHERE module_kind = 'dummy2' AND federation_id = $1",
            &[&fed],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(cursor, 3);
    // no items of kind dummy2 exist, so its tables stay empty
    let seen: i64 = conn
        .query_one("SELECT COUNT(*) FROM fmo_dummy2.seen", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(seen, 0);
    let _ = ModuleDecoderRegistry::default();
}

#[tokio::test]
async fn ingest_fills_structural_tables() {
    let _guard = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;

    let (config, federation_id) = dummy_config();
    insert_federation(&pool, &config, federation_id).await;

    let session = dummy_session(1_000);
    let mut conn = pool.get().await.unwrap();
    let dbtx = conn.transaction().await.unwrap();
    fmo_core::ingest::ingest_session(&dbtx, &config, federation_id, 0, &session)
        .await
        .unwrap();
    // idempotent within the same session
    fmo_core::ingest::ingest_session(&dbtx, &config, federation_id, 0, &session)
        .await
        .unwrap();
    dbtx.commit().await.unwrap();

    let conn = pool.get().await.unwrap();
    let fed = federation_id.consensus_encode_to_vec();
    let sessions: i64 = conn
        .query_one("SELECT COUNT(*) FROM sessions WHERE federation_id = $1", &[&fed])
        .await
        .unwrap()
        .get(0);
    assert_eq!(sessions, 1);
    let txs: i64 = conn
        .query_one("SELECT COUNT(*) FROM transactions WHERE federation_id = $1", &[&fed])
        .await
        .unwrap()
        .get(0);
    assert_eq!(txs, 1);
    for table in ["transaction_inputs", "transaction_outputs"] {
        let row = conn
            .query_one(
                &format!(
                    "SELECT COUNT(*)::bigint AS n,
                            COUNT(amount_msat)::bigint AS amounts,
                            MIN(kind) AS kind
                     FROM {table} WHERE federation_id = $1"
                ),
                &[&fed],
            )
            .await
            .unwrap();
        assert_eq!(row.get::<_, i64>("n"), 1, "{table} row count");
        assert_eq!(row.get::<_, i64>("amounts"), 0, "{table} amounts stay NULL");
        assert_eq!(row.get::<_, String>("kind"), "dummy");
    }
    let cis = conn
        .query_one(
            "SELECT COUNT(*)::bigint AS n, MIN(kind) AS kind FROM consensus_items WHERE federation_id = $1",
            &[&fed],
        )
        .await
        .unwrap();
    assert_eq!(cis.get::<_, i64>("n"), 1);
    assert_eq!(cis.get::<_, String>("kind"), "dummy");
}
