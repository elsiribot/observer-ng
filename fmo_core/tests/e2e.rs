mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use common::{dummy_config, dummy_session, insert_federation, reset_db, test_pool, DB_LOCK};
use fedimint_core::core::{Decoder, DynInput, DynModuleConsensusItem, DynOutput, ModuleKind};
use fedimint_core::encoding::Encodable;
use fedimint_core::module::CommonModuleInit;
use fedimint_core::Amount;
use fmo_core::module::{CiMeta, ItemMeta, Migration, ObserverModule, ProcessCtx, ProcessedItem};
use fmo_core::registry::ModuleRegistry;
use fmo_core::services::CoreServices;
use serde_json::json;

struct DummyObserver;

#[async_trait::async_trait]
impl ObserverModule for DummyObserver {
    fn kind(&self) -> ModuleKind {
        ModuleKind::from_static_str("dummy")
    }

    fn decoder(&self) -> Decoder {
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
            amount: Some(Amount::from_msats(21)),
            details: Some(json!({"kind": "dummy-input"})),
        })
    }

    async fn process_output(
        &self,
        _ctx: &mut ProcessCtx<'_>,
        _output: &DynOutput,
        _meta: &ItemMeta,
    ) -> anyhow::Result<ProcessedItem> {
        Ok(ProcessedItem {
            amount: Some(Amount::from_msats(21)),
            details: Some(json!({"kind": "dummy-output"})),
        })
    }

    async fn process_ci(
        &self,
        _ctx: &mut ProcessCtx<'_>,
        _ci: &DynModuleConsensusItem,
        _meta: &CiMeta,
    ) -> anyhow::Result<Option<serde_json::Value>> {
        Ok(Some(json!({"kind": "dummy-ci"})))
    }
}

async fn table_counts(pool: &deadpool_postgres::Pool) -> BTreeMap<String, i64> {
    let conn = pool.get().await.unwrap();
    let mut counts = BTreeMap::new();
    for table in [
        "sessions",
        "transactions",
        "transaction_inputs",
        "transaction_outputs",
        "consensus_items",
        "module_progress",
        "session_time_votes",
        "fmo_dummy.seen",
    ] {
        let count: i64 = conn
            .query_one(&format!("SELECT COUNT(*) FROM {table}"), &[])
            .await
            .unwrap()
            .get(0);
        counts.insert(table.to_owned(), count);
    }
    counts
}

/// Full pipeline: ingest -> dispatch -> matview refresh, then verify that
/// replaying everything is a strict no-op (idempotency).
#[tokio::test]
async fn e2e_pipeline_and_replay_idempotency() {
    let _guard = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;
    {
        let conn = pool.get().await.unwrap();
        conn.batch_execute("DROP SCHEMA IF EXISTS fmo_dummy CASCADE")
            .await
            .unwrap();
    }

    let (config, federation_id) = dummy_config();
    insert_federation(&pool, &config, federation_id).await;
    let services = Arc::new(CoreServices::new("http://unused".to_owned(), pool.clone()));

    let module = DummyObserver;
    fmo_core::db::migrations::setup_module_schema(
        &pool,
        "dummy",
        module.version(),
        module.migrations(),
    )
    .await
    .unwrap();
    let registry = ModuleRegistry::new(vec![Arc::new(module) as Arc<dyn ObserverModule>]);

    // ingest 5 sessions
    for session_index in 0..5u64 {
        let session = dummy_session(3_000 + session_index);
        let mut conn = pool.get().await.unwrap();
        let dbtx = conn.transaction().await.unwrap();
        fmo_core::ingest::ingest_session(&dbtx, &config, federation_id, session_index, &session)
            .await
            .unwrap();
        dbtx.commit().await.unwrap();
    }

    // process everything
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
    assert_eq!(processed, 5);

    // session_times maintenance works on the populated schema
    fmo_core::db::session_times::recompute_full(&pool.get().await.unwrap())
        .await
        .unwrap();

    let before = table_counts(&pool).await;
    assert_eq!(before["sessions"], 5);
    assert_eq!(before["transactions"], 5);
    assert_eq!(before["fmo_dummy.seen"], 5, "one input per session");
    assert_eq!(before["module_progress"], 1);

    // amounts written back
    let amounts: i64 = pool
        .get()
        .await
        .unwrap()
        .query_one(
            "SELECT COUNT(*) FROM transaction_inputs WHERE amount_msat = 21",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(amounts, 5);

    // REPLAY: re-ingest the same sessions and reset the module cursor to 0,
    // then process again. Every insert must be idempotent -> identical counts.
    for session_index in 0..5u64 {
        let session = dummy_session(3_000 + session_index);
        let mut conn = pool.get().await.unwrap();
        let dbtx = conn.transaction().await.unwrap();
        fmo_core::ingest::ingest_session(&dbtx, &config, federation_id, session_index, &session)
            .await
            .unwrap();
        dbtx.commit().await.unwrap();
    }
    pool.get()
        .await
        .unwrap()
        .execute("UPDATE module_progress SET next_session_index = 0", &[])
        .await
        .unwrap();
    let replayed = fmo_core::dispatch::process_pending(
        &pool,
        &registry,
        &services,
        federation_id,
        &config,
        100,
    )
    .await
    .unwrap();
    assert_eq!(replayed, 5);

    let after = table_counts(&pool).await;
    // fmo_dummy.seen uses plain INSERT (no ON CONFLICT) to prove the module
    // was really re-invoked; every other table must be unchanged.
    for (table, count_before) in &before {
        if table == "fmo_dummy.seen" {
            assert_eq!(
                after[table],
                count_before * 2,
                "module re-invoked on replay"
            );
        } else {
            assert_eq!(
                after[table], *count_before,
                "table {table} changed during replay"
            );
        }
    }
    let _ = federation_id.consensus_encode_to_vec();
}
