mod common;

use common::{dummy_config, insert_federation, reset_db, test_pool, DB_LOCK};
use fedimint_core::encoding::Encodable;
use fmo_core::observer::FederationObserver;
use fmo_core::registry::ModuleRegistry;

/// `federation_live_items` is the ascending, bounded keyset delta the SSE
/// handler tails: `after < (session,item) <= up_to`. This mirrors the
/// `consensus_stream_filters_and_paging` fixture in `consensus_api.rs`
/// (same tx ⊔ ci union + `USER_TX_LATERAL` enrichment, reused verbatim) but
/// walks it ascending and bounded on both ends instead of descending +
/// keyset-paginated.
#[tokio::test]
async fn live_items_delta_has_no_overlap_or_gap() {
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

    // Three sessions:
    //   session 0: tx tx_a (item 0), ci wallet (item 1)
    //   session 1: tx tx_b (item 0), ci ln (item 1)
    //   session 2: tx tx_c (item 0)
    conn.execute(
        "INSERT INTO sessions (federation_id, session_index, data)
         VALUES ($1, 0, ''::bytea), ($1, 1, ''::bytea), ($1, 2, ''::bytea)",
        &[&fed],
    )
    .await
    .unwrap();

    conn.execute(
        "INSERT INTO transactions (federation_id, txid, session_index, item_index, data)
         VALUES ($1, $2, 0, 0, ''::bytea), ($1, $3, 1, 0, ''::bytea), ($1, $4, 2, 0, ''::bytea)",
        &[
            &fed,
            &b"tx_a".to_vec(),
            &b"tx_b".to_vec(),
            &b"tx_c".to_vec(),
        ],
    )
    .await
    .unwrap();

    conn.execute(
        "INSERT INTO consensus_items (federation_id, session_index, item_index, peer_id, kind, details)
         VALUES ($1, 0, 1, 2, 'wallet', NULL), ($1, 1, 1, 3, 'ln', NULL)",
        &[&fed],
    )
    .await
    .unwrap();

    // Link tx_c to a user transaction so the enrichment (user_tx_key /
    // user_tx_kind / direction) is exercised too.
    conn.execute(
        "INSERT INTO user_transactions
             (federation_id, user_tx_key, kind, direction, num_fedimint_txs, first_session_index)
         VALUES ($1, $2, 'dummy', 'internal', 1, 2)",
        &[&fed, &b"tx_c".to_vec()],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO user_transaction_txs (federation_id, txid, user_tx_key, role, session_index)
         VALUES ($1, $2, $2, 'self', 2)",
        &[&fed, &b"tx_c".to_vec()],
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

    // Full ascending order: (0,0) tx_a, (0,1) ci wallet, (1,0) tx_b, (1,1) ci
    // ln, (2,0) tx_c.

    // --- from the very start, up to the end: all 5 items, ascending, and
    // the enriched tx carries user_tx_kind/direction ---
    let all = observer
        .federation_live_items(federation_id, None, (2, 0))
        .await
        .unwrap();
    assert_eq!(
        all.iter()
            .map(|i| (i.session_index, i.item_index, i.item_type.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (0, 0, "transaction"),
            (0, 1, "ci"),
            (1, 0, "transaction"),
            (1, 1, "ci"),
            (2, 0, "transaction"),
        ]
    );
    let tx_c = all.last().unwrap();
    assert_eq!(tx_c.txid.as_deref(), Some(hex::encode(b"tx_c")).as_deref());
    assert_eq!(
        tx_c.user_tx_key.as_deref(),
        Some(hex::encode(b"tx_c")).as_deref()
    );
    assert_eq!(tx_c.user_tx_kind.as_deref(), Some("dummy"));
    assert_eq!(tx_c.direction.as_deref(), Some("internal"));
    // tx_a/tx_b are not linked to any user transaction: orphan txs.
    assert!(all[0].user_tx_kind.is_none());
    assert!(all[2].user_tx_kind.is_none());

    // --- bounded first read: only items <= (1,0) ---
    let first = observer
        .federation_live_items(federation_id, None, (1, 0))
        .await
        .unwrap();
    assert_eq!(
        first
            .iter()
            .map(|i| (i.session_index, i.item_index))
            .collect::<Vec<_>>(),
        vec![(0, 0), (0, 1), (1, 0)]
    );

    // --- delta strictly after the first read's cursor, up to the end ---
    let cursor = first
        .last()
        .map(|i| (i.session_index, i.item_index))
        .unwrap();
    assert_eq!(cursor, (1, 0));
    let delta = observer
        .federation_live_items(federation_id, Some(cursor), (2, 0))
        .await
        .unwrap();
    assert_eq!(
        delta
            .iter()
            .map(|i| (i.session_index, i.item_index))
            .collect::<Vec<_>>(),
        vec![(1, 1), (2, 0)]
    );
    // Delta carries the enrichment too.
    assert_eq!(delta.last().unwrap().user_tx_kind.as_deref(), Some("dummy"));

    // No overlap, no gap: first ++ delta == the full ascending 5-item set.
    let mut combined: Vec<(i64, i64)> = first
        .iter()
        .chain(delta.iter())
        .map(|i| (i.session_index, i.item_index))
        .collect();
    let mut all_keys: Vec<(i64, i64)> = all
        .iter()
        .map(|i| (i.session_index, i.item_index))
        .collect();
    combined.sort();
    all_keys.sort();
    assert_eq!(combined, all_keys);
    assert_eq!(
        combined.len(),
        5,
        "no overlap and no gap between the two reads"
    );

    // --- an empty delta when there's nothing new past `up_to` ---
    let empty = observer
        .federation_live_items(federation_id, Some((2, 0)), (2, 0))
        .await
        .unwrap();
    assert!(empty.is_empty());
}
