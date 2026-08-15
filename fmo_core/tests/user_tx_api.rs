mod common;

use common::{dummy_config, insert_federation, reset_db, test_pool, DB_LOCK};
use fedimint_core::encoding::Encodable;
use fmo_core::observer::FederationObserver;
use fmo_core::registry::ModuleRegistry;
use serde_json::json;

/// Structured tx detail reads `transaction_inputs`/`transaction_outputs`
/// directly (not the Debug-string decode) and resolves the tx's gold-layer
/// `user_tx_key` via `user_transaction_txs`; the user-transaction endpoint
/// reads the `user_transactions` summary row plus all its member legs with
/// their roles, ordered by `session_index, role`.
#[tokio::test]
async fn tx_detail_and_user_transaction_assembly() {
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

    // Three sessions, one leg tx each: offer/fund/claim of one LN contract.
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
            &b"tx_offer".to_vec(),
            &b"tx_fund".to_vec(),
            &b"tx_claim".to_vec(),
        ],
    )
    .await
    .unwrap();

    // Structured inputs/outputs for the "fund" leg, with kind+amount+details.
    conn.execute(
        "INSERT INTO transaction_inputs (federation_id, txid, in_index, kind, amount_msat, details)
         VALUES ($1, $2, 0, 'mint', 1000, $3)",
        &[&fed, &b"tx_fund".to_vec(), &json!({"note": "in0"})],
    )
    .await
    .unwrap();

    conn.execute(
        "INSERT INTO transaction_outputs (federation_id, txid, out_index, kind, amount_msat, details)
         VALUES ($1, $2, 0, 'ln', 990, $3)",
        &[&fed, &b"tx_fund".to_vec(), &json!({"contract": "out0"})],
    )
    .await
    .unwrap();

    // Gold layer: one user_transactions row (contract_1), 3 member legs.
    conn.execute(
        "INSERT INTO user_transactions
             (federation_id, user_tx_key, kind, direction, amount_msat, fedimint_fee_msat,
              num_fedimint_txs, first_session_index, first_timestamp, last_timestamp)
         VALUES ($1, $2, 'ln_receive', 'in', 990, 10, 3, 0, '2024-01-15 12:00:00+00', '2024-01-15 12:10:00+00')",
        &[&fed, &b"contract_1".to_vec()],
    )
    .await
    .unwrap();

    // A second user transaction (e.g. a non-ecash kind).
    conn.execute(
        "INSERT INTO transactions (federation_id, txid, session_index, item_index, data)
         VALUES ($1, $2, 0, 1, ''::bytea)",
        &[&fed, &b"tx_wallet".to_vec()],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO user_transactions
             (federation_id, user_tx_key, kind, direction, amount_msat, fedimint_fee_msat,
              num_fedimint_txs, first_session_index, first_timestamp, last_timestamp)
         VALUES ($1, $2, 'wallet_deposit', 'in', 5000, 0, 1, 0, '2024-01-15 12:00:00+00', '2024-01-15 12:00:00+00')",
        &[&fed, &b"tx_wallet".to_vec()],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO user_transaction_txs (federation_id, txid, user_tx_key, role, session_index)
         VALUES ($1, $2, $2, 'self', 0)",
        &[&fed, &b"tx_wallet".to_vec()],
    )
    .await
    .unwrap();

    conn.execute(
        "INSERT INTO user_transaction_txs (federation_id, txid, user_tx_key, role, session_index)
         VALUES ($1, $2, $5, 'offer', 0),
                ($1, $3, $5, 'fund', 1),
                ($1, $4, $5, 'claim', 2)",
        &[
            &fed,
            &b"tx_offer".to_vec(),
            &b"tx_fund".to_vec(),
            &b"tx_claim".to_vec(),
            &b"contract_1".to_vec(),
        ],
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

    // --- structured transaction detail ---
    let detail = observer
        .federation_transaction_detail(federation_id, b"tx_fund")
        .await
        .unwrap();

    assert_eq!(detail.txid, hex::encode(b"tx_fund"));
    assert_eq!(detail.session_index, 1);
    assert_eq!(detail.item_index, 0);

    assert_eq!(detail.inputs.len(), 1);
    assert_eq!(detail.inputs[0].index, 0);
    assert_eq!(detail.inputs[0].kind, "mint");
    assert_eq!(detail.inputs[0].amount_msat, Some(1000));
    assert_eq!(detail.inputs[0].details, Some(json!({"note": "in0"})));

    assert_eq!(detail.outputs.len(), 1);
    assert_eq!(detail.outputs[0].index, 0);
    assert_eq!(detail.outputs[0].kind, "ln");
    assert_eq!(detail.outputs[0].amount_msat, Some(990));
    assert_eq!(detail.outputs[0].details, Some(json!({"contract": "out0"})));

    assert_eq!(
        detail.user_tx_key.as_deref(),
        Some(hex::encode(b"contract_1")).as_deref()
    );

    // A tx not part of any user transaction has user_tx_key = None.
    conn_no_gold_check(&pool, &federation_id).await;

    // --- gold user-transaction assembly ---
    let user_tx = observer
        .federation_user_transaction(federation_id, b"contract_1")
        .await
        .unwrap();

    assert_eq!(user_tx.kind, "ln_receive");
    assert_eq!(user_tx.direction, "in");
    assert_eq!(user_tx.amount_msat, Some(990));
    assert_eq!(user_tx.fedimint_fee_msat, Some(10));
    assert_eq!(user_tx.gateway_fee_estimate_msat, None);
    assert_eq!(user_tx.num_fedimint_txs, 3);
    assert!(user_tx.first_timestamp.is_some());
    assert!(user_tx.last_timestamp.is_some());
    assert!(user_tx.first_timestamp.unwrap() <= user_tx.last_timestamp.unwrap());

    assert_eq!(user_tx.member_txs.len(), 3);
    assert_eq!(
        user_tx
            .member_txs
            .iter()
            .map(|m| (m.txid.clone(), m.role.clone(), m.session_index))
            .collect::<Vec<_>>(),
        vec![
            (hex::encode(b"tx_offer"), "offer".to_owned(), 0),
            (hex::encode(b"tx_fund"), "fund".to_owned(), 1),
            (hex::encode(b"tx_claim"), "claim".to_owned(), 2),
        ]
    );
}

/// A transaction not linked to any gold user transaction resolves
/// `user_tx_key: None` rather than erroring.
async fn conn_no_gold_check(
    pool: &deadpool_postgres::Pool,
    federation_id: &fedimint_core::config::FederationId,
) {
    let fed = federation_id.consensus_encode_to_vec();
    let conn = pool.get().await.unwrap();
    conn.execute(
        "INSERT INTO sessions (federation_id, session_index, data) VALUES ($1, 3, ''::bytea)",
        &[&fed],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO transactions (federation_id, txid, session_index, item_index, data)
         VALUES ($1, $2, 3, 0, ''::bytea)",
        &[&fed, &b"tx_orphan".to_vec()],
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

    let detail = observer
        .federation_transaction_detail(*federation_id, b"tx_orphan")
        .await
        .unwrap();
    assert!(detail.user_tx_key.is_none());
    assert!(detail.inputs.is_empty());
    assert!(detail.outputs.is_empty());
}
