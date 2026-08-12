mod common;

use common::{dummy_config, insert_federation, reset_db, test_pool, DB_LOCK};
use fedimint_core::encoding::Encodable;
use fmo_core::observer::FederationObserver;
use fmo_core::registry::ModuleRegistry;
use serde_json::json;

/// Session list is keyset-paginated over `session_stats` (+ `session_times`
/// for the estimated timestamp), and session detail unions transactions and
/// consensus items for one session, ordered by `item_index`.
#[tokio::test]
async fn session_page_and_items() {
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

    // Two sessions, each with one transaction and one consensus item.
    conn.execute(
        "INSERT INTO sessions (federation_id, session_index, data)
         VALUES ($1, 0, ''::bytea), ($1, 1, ''::bytea)",
        &[&fed],
    )
    .await
    .unwrap();

    conn.execute(
        "INSERT INTO transactions (federation_id, txid, session_index, item_index, data)
         VALUES ($1, $2, 0, 0, ''::bytea), ($1, $3, 1, 0, ''::bytea)",
        &[&fed, &b"tx_zero".to_vec(), &b"tx_one".to_vec()],
    )
    .await
    .unwrap();

    conn.execute(
        "INSERT INTO consensus_items (federation_id, session_index, item_index, peer_id, kind, details)
         VALUES ($1, 0, 1, 2, 'wallet', $2), ($1, 1, 1, 3, 'ln', NULL)",
        &[&fed, &json!({"foo": "bar"})],
    )
    .await
    .unwrap();

    conn.execute(
        "INSERT INTO session_stats (federation_id, session_index, tx_count, ci_count, items_by_kind)
         VALUES ($1, 0, 1, 1, $2), ($1, 1, 1, 1, $3)",
        &[&fed, &json!({"wallet": 1}), &json!({"ln": 1})],
    )
    .await
    .unwrap();

    conn.execute(
        "INSERT INTO session_time_votes (federation_id, session_index, source_kind, peer_id, timestamp)
         VALUES ($1, 0, 'wallet', 0, '2024-01-15 12:00:00'),
                ($1, 1, 'wallet', 0, '2024-01-15 12:05:00')",
        &[&fed],
    )
    .await
    .unwrap();
    fmo_core::db::session_times::recompute_full(&conn)
        .await
        .unwrap();

    // Link the session-0 transaction to a user transaction so the item list
    // can resolve `user_tx_key` via `user_transaction_txs`.
    conn.execute(
        "INSERT INTO user_transactions
             (federation_id, user_tx_key, kind, direction, num_fedimint_txs, first_session_index)
         VALUES ($1, $2, 'dummy', 'internal', 1, 0)",
        &[&fed, &b"tx_zero".to_vec()],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO user_transaction_txs (federation_id, txid, user_tx_key, role, session_index)
         VALUES ($1, $2, $2, 'self', 0)",
        &[&fed, &b"tx_zero".to_vec()],
    )
    .await
    .unwrap();
    drop(conn);

    let registry = ModuleRegistry::new(vec![]);
    let observer = FederationObserver::new_without_tasks(
        &std::env::var("FMO_TEST_DATABASE").unwrap(),
        "admin",
        "http://unused.invalid",
        registry,
    )
    .await
    .unwrap();

    // --- session_stats page: newest first, keyset-paginated ---
    let page = observer
        .federation_session_page(federation_id, None, 50)
        .await
        .unwrap();
    assert_eq!(page.len(), 2);
    assert_eq!(page[0].session_index, 1);
    assert_eq!(page[0].tx_count, 1);
    assert_eq!(page[0].items_by_kind, json!({"ln": 1}));
    assert!(page[0].estimated_time.is_some());
    assert_eq!(page[1].session_index, 0);
    assert_eq!(page[1].tx_count, 1);
    assert_eq!(page[1].items_by_kind, json!({"wallet": 1}));
    assert!(page[1].estimated_time.is_some());

    // Keyset cursor: only sessions strictly before 1.
    let next_page = observer
        .federation_session_page(federation_id, Some(1), 50)
        .await
        .unwrap();
    assert_eq!(next_page.len(), 1);
    assert_eq!(next_page[0].session_index, 0);

    // --- session detail: tx union ci, ordered by item_index ---
    let items = observer
        .federation_session_items(federation_id, 0)
        .await
        .unwrap();
    assert_eq!(items.len(), 2);

    assert_eq!(items[0].session_index, 0);
    assert_eq!(items[0].item_index, 0);
    assert_eq!(items[0].item_type, "transaction");
    assert_eq!(
        items[0].txid.as_deref(),
        Some(hex::encode(b"tx_zero")).as_deref()
    );
    assert_eq!(
        items[0].user_tx_key.as_deref(),
        Some(hex::encode(b"tx_zero")).as_deref()
    );
    assert_eq!(items[0].user_tx_kind.as_deref(), Some("dummy"));
    assert_eq!(items[0].direction.as_deref(), Some("internal"));
    assert!(items[0].kind.is_none());
    assert!(items[0].details.is_none());

    assert_eq!(items[1].session_index, 0);
    assert_eq!(items[1].item_index, 1);
    assert_eq!(items[1].item_type, "ci");
    assert_eq!(items[1].kind.as_deref(), Some("wallet"));
    assert_eq!(items[1].peer_id, Some(2));
    assert_eq!(items[1].details, Some(json!({"foo": "bar"})));
    assert!(items[1].txid.is_none());
    assert!(items[1].user_tx_key.is_none());
    assert!(items[1].user_tx_kind.is_none());
    assert!(items[1].direction.is_none());
}
