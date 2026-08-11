mod common;

use common::{dummy_config, insert_federation, reset_db, test_pool, DB_LOCK};
use fedimint_core::encoding::Encodable;
use fmo_core::observer::FederationObserver;
use fmo_core::registry::ModuleRegistry;

/// Federation-wide consensus item stream: keyset-paginated over
/// `(session_index, item_index)` desc, filterable by `"all"` (tx ⊔ ci),
/// `"transaction"` (tx only), or a specific module kind (ci only, that
/// kind).
#[tokio::test]
async fn consensus_stream_filters_and_paging() {
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

    // Link tx_c to a user transaction so the "transaction" filter can
    // exercise the user_tx_key join too.
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

    // --- filter=all, page 1: newest 3 items overall ---
    // Full order desc: (2,0) tx_c, (1,1) ci ln, (1,0) tx_b, (0,1) ci wallet, (0,0)
    // tx_a
    let page1 = observer
        .federation_consensus_page(federation_id, "all", None, 3)
        .await
        .unwrap();
    assert_eq!(page1.items.len(), 3);
    assert_eq!(
        page1
            .items
            .iter()
            .map(|i| (i.session_index, i.item_index, i.item_type.as_str()))
            .collect::<Vec<_>>(),
        vec![(2, 0, "transaction"), (1, 1, "ci"), (1, 0, "transaction")]
    );
    assert_eq!(
        page1.items[0].txid.as_deref(),
        Some(hex::encode(b"tx_c")).as_deref()
    );
    assert_eq!(
        page1.items[0].user_tx_key.as_deref(),
        Some(hex::encode(b"tx_c")).as_deref()
    );
    assert_eq!(page1.items[1].kind.as_deref(), Some("ln"));
    assert_eq!(page1.next, Some((1, 0)));

    // --- filter=all, page 2: remaining 2 items, using the returned cursor ---
    let page2 = observer
        .federation_consensus_page(federation_id, "all", page1.next, 3)
        .await
        .unwrap();
    assert_eq!(page2.items.len(), 2);
    assert_eq!(
        page2
            .items
            .iter()
            .map(|i| (i.session_index, i.item_index, i.item_type.as_str()))
            .collect::<Vec<_>>(),
        vec![(0, 1, "ci"), (0, 0, "transaction")]
    );
    // Fewer than `limit` returned -> no more pages.
    assert_eq!(page2.next, None);

    // No overlap/gap between page1 and page2: union is exactly all 5 items.
    let mut all_keys: Vec<(i64, i64)> = page1
        .items
        .iter()
        .chain(page2.items.iter())
        .map(|i| (i.session_index, i.item_index))
        .collect();
    all_keys.sort();
    all_keys.dedup();
    assert_eq!(all_keys.len(), 5);

    // --- filter=transaction: tx items only, newest first ---
    let tx_page = observer
        .federation_consensus_page(federation_id, "transaction", None, 50)
        .await
        .unwrap();
    assert_eq!(
        tx_page
            .items
            .iter()
            .map(|i| (i.session_index, i.item_index))
            .collect::<Vec<_>>(),
        vec![(2, 0), (1, 0), (0, 0)]
    );
    assert!(tx_page.items.iter().all(|i| i.item_type == "transaction"));
    assert_eq!(tx_page.next, None);

    // --- filter=<kind>: consensus items of that kind only ---
    let ln_page = observer
        .federation_consensus_page(federation_id, "ln", None, 50)
        .await
        .unwrap();
    assert_eq!(ln_page.items.len(), 1);
    assert_eq!(ln_page.items[0].session_index, 1);
    assert_eq!(ln_page.items[0].item_index, 1);
    assert_eq!(ln_page.items[0].kind.as_deref(), Some("ln"));

    let wallet_page = observer
        .federation_consensus_page(federation_id, "wallet", None, 50)
        .await
        .unwrap();
    assert_eq!(wallet_page.items.len(), 1);
    assert_eq!(wallet_page.items[0].session_index, 0);
    assert_eq!(wallet_page.items[0].item_index, 1);
    assert_eq!(wallet_page.items[0].kind.as_deref(), Some("wallet"));
}
