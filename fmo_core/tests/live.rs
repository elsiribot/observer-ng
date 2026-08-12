mod common;

use std::sync::Arc;

use common::{dummy_config, dummy_session, insert_federation, reset_db, test_pool, DB_LOCK};
use fedimint_core::core::{Decoder, DynInput, DynModuleConsensusItem, DynOutput, ModuleKind};
use fedimint_core::encoding::Encodable;
use fedimint_core::session_outcome::SessionOutcome;
use fmo_core::live::{finalize_live_session, live_process};
use fmo_core::module::{CiMeta, ItemMeta, Migration, ObserverModule, ProcessCtx, ProcessedItem};
use fmo_core::registry::ModuleRegistry;
use fmo_core::services::CoreServices;
use serde_json::json;

/// Same shape as `incremental.rs`'s `TestModule`: records every item it sees
/// in its own `seen` table (this time also recording `item_index`, so the
/// test can assert exactly which items got dispatched by each `live_process`
/// slice) so the test can assert whether dispatch actually ran.
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
            sql: "CREATE TABLE seen (
                session_index INTEGER NOT NULL,
                item_index    INTEGER NOT NULL,
                what          TEXT NOT NULL,
                UNIQUE (session_index, item_index, what)
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
                "INSERT INTO seen VALUES ($1, $2, 'input') ON CONFLICT DO NOTHING",
                &[&(meta.session_index as i32), &(meta.item_index as i32)],
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
                "INSERT INTO seen VALUES ($1, $2, 'output') ON CONFLICT DO NOTHING",
                &[&(meta.session_index as i32), &(meta.item_index as i32)],
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
                "INSERT INTO seen VALUES ($1, $2, 'ci') ON CONFLICT DO NOTHING",
                &[&(meta.session_index as i32), &(meta.item_index as i32)],
            )
            .await?;
        Ok(Some(json!({"t": "ci"})))
    }
}

/// A second, otherwise-inert module (no items in the fixture belong to it --
/// there's no "laggard"-kind instance in `dummy_config`) used only to prove
/// the conditional `module_progress` advance in `finalize_live_session`:
/// registered alongside `TestModule` so `finalize_live_session` iterates
/// over both, its cursor is pre-seeded *behind* `session_index`, and the
/// test asserts it is left untouched (0 rows matched by the conditional
/// `UPDATE`) while `TestModule`'s caught-up cursor does advance.
struct LaggardModule;

#[async_trait::async_trait]
impl ObserverModule for LaggardModule {
    fn kind(&self) -> ModuleKind {
        ModuleKind::from_static_str("laggard")
    }

    fn decoder(&self) -> Decoder {
        Decoder::builder().build()
    }

    fn version(&self) -> u32 {
        1
    }

    fn migrations(&self) -> &'static [Migration] {
        &[]
    }

    async fn process_input(
        &self,
        _ctx: &mut ProcessCtx<'_>,
        _input: &DynInput,
        _meta: &ItemMeta,
    ) -> anyhow::Result<ProcessedItem> {
        Ok(ProcessedItem::default())
    }

    async fn process_output(
        &self,
        _ctx: &mut ProcessCtx<'_>,
        _output: &DynOutput,
        _meta: &ItemMeta,
    ) -> anyhow::Result<ProcessedItem> {
        Ok(ProcessedItem::default())
    }

    async fn process_ci(
        &self,
        _ctx: &mut ProcessCtx<'_>,
        _ci: &DynModuleConsensusItem,
        _meta: &CiMeta,
    ) -> anyhow::Result<Option<serde_json::Value>> {
        Ok(None)
    }
}

/// End-to-end exercise of the two functions the live loop (a later task)
/// will call per poll / on finalize: `live_process` incrementally as new
/// items arrive within a still-open session, then `finalize_live_session`
/// once the session signs.
#[tokio::test]
async fn live_process_then_finalize() {
    let _guard = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;

    let (config, federation_id) = dummy_config();
    insert_federation(&pool, &config, federation_id).await;
    let fed = federation_id.consensus_encode_to_vec();

    let module: Arc<dyn ObserverModule> = Arc::new(TestModule);
    let laggard: Arc<dyn ObserverModule> = Arc::new(LaggardModule);
    let registry = ModuleRegistry::new(vec![module, laggard]);
    fmo_core::db::migrations::setup_module_schema(
        &pool,
        "dummy",
        1,
        &[Migration {
            sql: "CREATE TABLE seen (
                session_index INTEGER NOT NULL,
                item_index    INTEGER NOT NULL,
                what          TEXT NOT NULL,
                UNIQUE (session_index, item_index, what)
            );",
        }],
    )
    .await
    .unwrap();
    let services = Arc::new(CoreServices::new("http://unused".to_owned(), pool.clone()));

    // A session's worth of items: one dummy transaction (1 input, 1 output)
    // at item_index 0, followed by one dummy module CI at item_index 1 --
    // built by the shared `dummy_session` fixture, which already matches
    // `dummy_config`'s single "dummy" module instance.
    let session_index = 1u64;
    let items = dummy_session(9_001).items;
    assert_eq!(items.len(), 2, "test setup: transaction + CI");

    // --- Poll 1: only the transaction (item 0) has landed so far. ---
    live_process(
        &pool,
        &registry,
        &services,
        federation_id,
        &config,
        session_index,
        &items[..1],
        0,
    )
    .await
    .unwrap();

    {
        let conn = pool.get().await.unwrap();

        let tx_count: i64 = conn
            .query_one(
                "SELECT COUNT(*) FROM transactions WHERE federation_id = $1",
                &[&fed],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(tx_count, 1, "structural transaction row for item 0");

        let amounts: Vec<Option<i64>> = conn
            .query(
                "SELECT amount_msat FROM transaction_inputs WHERE federation_id = $1",
                &[&fed],
            )
            .await
            .unwrap()
            .iter()
            .map(|r| r.get(0))
            .collect();
        assert_eq!(
            amounts,
            vec![Some(42)],
            "module dispatch wrote the amount back for the input"
        );

        let stats = conn
            .query_one(
                "SELECT tx_count, ci_count FROM session_stats
                 WHERE federation_id = $1 AND session_index = 1",
                &[&fed],
            )
            .await
            .unwrap();
        assert_eq!(stats.get::<_, i32>(0), 1, "running tx_count after poll 1");
        assert_eq!(stats.get::<_, i32>(1), 0, "running ci_count after poll 1");

        let seen: Vec<(i32, i32, String)> = conn
            .query(
                "SELECT session_index, item_index, what FROM fmo_dummy.seen ORDER BY item_index, what",
                &[],
            )
            .await
            .unwrap()
            .iter()
            .map(|r| (r.get(0), r.get(1), r.get(2)))
            .collect();
        assert_eq!(
            seen,
            vec![(1, 0, "input".to_owned()), (1, 0, "output".to_owned())],
            "module silver rows for item 0 only"
        );

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
            "live_process must never touch module_progress (no row yet == still at 0)"
        );
    }

    // --- Poll 2: the CI (item 1) has now landed too. ---
    live_process(
        &pool,
        &registry,
        &services,
        federation_id,
        &config,
        session_index,
        &items,
        1,
    )
    .await
    .unwrap();

    {
        let conn = pool.get().await.unwrap();

        let tx_count: i64 = conn
            .query_one(
                "SELECT COUNT(*) FROM transactions WHERE federation_id = $1",
                &[&fed],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(
            tx_count, 1,
            "structural transaction row unchanged (ON CONFLICT DO NOTHING)"
        );

        let ci_count: i64 = conn
            .query_one(
                "SELECT COUNT(*) FROM consensus_items WHERE federation_id = $1",
                &[&fed],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(ci_count, 1, "structural CI row for item 1");

        let stats = conn
            .query_one(
                "SELECT tx_count, ci_count FROM session_stats
                 WHERE federation_id = $1 AND session_index = 1",
                &[&fed],
            )
            .await
            .unwrap();
        assert_eq!(stats.get::<_, i32>(0), 1, "final tx_count");
        assert_eq!(stats.get::<_, i32>(1), 1, "final ci_count");

        let seen: Vec<(i32, i32, String)> = conn
            .query(
                "SELECT session_index, item_index, what FROM fmo_dummy.seen ORDER BY item_index, what",
                &[],
            )
            .await
            .unwrap()
            .iter()
            .map(|r| (r.get(0), r.get(1), r.get(2)))
            .collect();
        assert_eq!(
            seen,
            vec![
                (1, 0, "input".to_owned()),
                (1, 0, "output".to_owned()),
                (1, 1, "ci".to_owned()),
            ],
            "module silver rows for all items now"
        );

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
            "live_process still must not touch module_progress after processing the whole session"
        );

        let data_is_null: bool = conn
            .query_one(
                "SELECT data IS NULL FROM sessions WHERE federation_id = $1 AND session_index = 1",
                &[&fed],
            )
            .await
            .unwrap()
            .get(0);
        assert!(
            data_is_null,
            "session is still open (unsigned) before finalize"
        );
    }

    // --- Guard: a poll with nothing new (start == items.len()) is a no-op,
    // not a panic on `items[start..]`. ---
    live_process(
        &pool,
        &registry,
        &services,
        federation_id,
        &config,
        session_index,
        &items,
        items.len(),
    )
    .await
    .unwrap();

    // Pre-seed module_progress for both registered modules, at two
    // different cursors, to exercise both branches of the CRITICAL
    // conditional advance in `finalize_live_session`:
    // - "dummy" is exactly at `session_index`, the way a separate `run_processor`
    //   that has already caught up would leave it -- this is the condition under
    //   which the cursor MUST advance.
    // - "laggard" is one session behind, the way a `run_processor` that hasn't
    //   caught up yet would leave it -- its cursor MUST NOT move, or the
    //   un-dispatched session it's still behind on would get skipped.
    {
        let conn = pool.get().await.unwrap();
        conn.execute(
            "INSERT INTO module_progress (module_kind, federation_id, next_session_index)
             VALUES ('dummy', $1, $2)",
            &[&fed, &(session_index as i32)],
        )
        .await
        .unwrap();
        conn.execute(
            "INSERT INTO module_progress (module_kind, federation_id, next_session_index)
             VALUES ('laggard', $1, $2)",
            &[&fed, &(session_index as i32 - 1)],
        )
        .await
        .unwrap();
    }

    // --- The session signs. `finalize_live_session` is called with a
    // deliberately-short `processed_count` (as if the live poller had only
    // ever seen item 0), so it must backfill the CI tail itself. ---
    let final_session = SessionOutcome {
        items: items.clone(),
    };
    let data_bytes = final_session.consensus_encode_to_vec();
    let processed_count = items.len() - 1;
    assert!(
        processed_count < items.len(),
        "test setup: deliberately short"
    );

    finalize_live_session(
        &pool,
        &registry,
        &services,
        federation_id,
        &config,
        session_index,
        &items,
        processed_count,
        &data_bytes,
        None,
    )
    .await
    .unwrap();

    {
        let conn = pool.get().await.unwrap();

        let stored_data: Option<Vec<u8>> = conn
            .query_one(
                "SELECT data FROM sessions WHERE federation_id = $1 AND session_index = 1",
                &[&fed],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(
            stored_data,
            Some(data_bytes),
            "sessions.data is set to the finalized session"
        );

        let ci_count: i64 = conn
            .query_one(
                "SELECT COUNT(*) FROM consensus_items WHERE federation_id = $1",
                &[&fed],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(
            ci_count, 1,
            "tail backfill did not duplicate the already-ingested CI"
        );

        let seen_count: i64 = conn
            .query_one("SELECT COUNT(*) FROM fmo_dummy.seen", &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(
            seen_count, 3,
            "tail backfill did not duplicate module silver rows either"
        );

        let cursor: i32 = conn
            .query_one(
                "SELECT next_session_index FROM module_progress
                 WHERE module_kind = 'dummy' AND federation_id = $1",
                &[&fed],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(
            cursor,
            session_index as i32 + 1,
            "module_progress advanced past the now-finalized session"
        );

        let laggard_cursor: i32 = conn
            .query_one(
                "SELECT next_session_index FROM module_progress
                 WHERE module_kind = 'laggard' AND federation_id = $1",
                &[&fed],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(
            laggard_cursor,
            session_index as i32 - 1,
            "CRITICAL: a module that hasn't caught up to session_index yet must NOT be \
             advanced by finalize -- otherwise the un-dispatched session(s) it's still \
             behind on would be skipped forever"
        );
    }
}
