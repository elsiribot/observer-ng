mod common;

use common::{dummy_config, insert_federation, reset_db, test_pool, DB_LOCK};
use fedimint_core::encoding::Encodable;
use fmo_core::session_stats::backfill_session_stats;
use serde_json::json;

/// Backfill fills in `session_stats` for a session that predates the table,
/// counting whole transactions and per-kind consensus items, and is
/// idempotent when re-run.
#[tokio::test]
async fn backfill_computes_stats_and_is_idempotent() {
    let _guard = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;

    let (config, federation_id) = dummy_config();
    insert_federation(&pool, &config, federation_id).await;
    let fed = federation_id.consensus_encode_to_vec();

    let conn = pool.get().await.unwrap();

    conn.execute(
        "INSERT INTO sessions (federation_id, session_index, data) VALUES ($1, 0, ''::bytea)",
        &[&fed],
    )
    .await
    .unwrap();

    // Two transactions in the session.
    for (i, txid) in [b"tx_one".to_vec(), b"tx_two".to_vec()]
        .into_iter()
        .enumerate()
    {
        conn.execute(
            "INSERT INTO transactions (federation_id, txid, session_index, item_index, data)
             VALUES ($1, $2, 0, $3, ''::bytea)",
            &[&fed, &txid, &(i as i32)],
        )
        .await
        .unwrap();
    }

    // Consensus items of two different kinds.
    conn.execute(
        "INSERT INTO consensus_items (federation_id, session_index, item_index, peer_id, kind)
         VALUES ($1, 0, 2, 0, 'wallet'), ($1, 0, 3, 0, 'wallet'), ($1, 0, 4, 0, 'ln')",
        &[&fed],
    )
    .await
    .unwrap();

    backfill_session_stats(&pool, &fed).await.unwrap();

    let row = conn
        .query_one(
            "SELECT tx_count, ci_count, items_by_kind FROM session_stats
             WHERE federation_id = $1 AND session_index = 0",
            &[&fed],
        )
        .await
        .unwrap();
    let tx_count: i32 = row.get(0);
    let ci_count: i32 = row.get(1);
    let items_by_kind: serde_json::Value = row.get(2);

    assert_eq!(tx_count, 2);
    assert_eq!(ci_count, 3);
    assert_eq!(items_by_kind, json!({"ln": 1, "wallet": 2}));

    // Re-running is a no-op: still exactly one row, unchanged.
    backfill_session_stats(&pool, &fed).await.unwrap();

    let n: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM session_stats WHERE federation_id = $1",
            &[&fed],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(n, 1);

    let row = conn
        .query_one(
            "SELECT tx_count, ci_count, items_by_kind FROM session_stats
             WHERE federation_id = $1 AND session_index = 0",
            &[&fed],
        )
        .await
        .unwrap();
    let tx_count: i32 = row.get(0);
    let ci_count: i32 = row.get(1);
    let items_by_kind: serde_json::Value = row.get(2);
    assert_eq!(tx_count, 2);
    assert_eq!(ci_count, 3);
    assert_eq!(items_by_kind, json!({"ln": 1, "wallet": 2}));
}
