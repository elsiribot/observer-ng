mod common;

use common::{dummy_config, insert_federation, reset_db, test_pool, DB_LOCK};
use fedimint_core::encoding::Encodable;

/// Creates minimal fmo_ln / fmo_lnv2 contract tables so fold_standalone's
/// `NOT EXISTS` guards against LN-leg txs can run (Task 3 will make these
/// guards schema-existence-aware; for now the schemas are assumed present).
async fn create_ln_schemas(pool: &deadpool_postgres::Pool) {
    pool.get()
        .await
        .unwrap()
        .batch_execute(
            "CREATE SCHEMA IF NOT EXISTS fmo_ln;
             CREATE TABLE IF NOT EXISTS fmo_ln.output_contracts (federation_id BYTEA NOT NULL, txid BYTEA NOT NULL);
             CREATE TABLE IF NOT EXISTS fmo_ln.input_contracts (federation_id BYTEA NOT NULL, txid BYTEA NOT NULL);
             CREATE SCHEMA IF NOT EXISTS fmo_lnv2;
             CREATE TABLE IF NOT EXISTS fmo_lnv2.contracts (federation_id BYTEA NOT NULL, txid BYTEA NOT NULL);
             CREATE TABLE IF NOT EXISTS fmo_lnv2.input_outpoints (federation_id BYTEA NOT NULL, txid BYTEA NOT NULL);",
        )
        .await
        .unwrap();
}

/// Inserts a bare session row (FK target for `transactions`); session
/// contents are irrelevant here since gold folding reads structural tables
/// directly, not raw session data.
async fn insert_session(pool: &deadpool_postgres::Pool, fed: &[u8], session_index: i32) {
    pool.get()
        .await
        .unwrap()
        .execute(
            "INSERT INTO sessions (federation_id, session_index, data) VALUES ($1, $2, $3)
             ON CONFLICT DO NOTHING",
            &[&fed, &session_index, &vec![0u8; 1]],
        )
        .await
        .unwrap();
}

#[allow(clippy::too_many_arguments)]
async fn insert_tx(
    pool: &deadpool_postgres::Pool,
    fed: &[u8],
    txid: &[u8],
    session_index: i32,
    in_kind: &str,
    in_amount_msat: i64,
    out_kind: &str,
    out_amount_msat: i64,
) {
    let conn = pool.get().await.unwrap();
    conn.execute(
        "INSERT INTO transactions (federation_id, txid, session_index, item_index, data)
         VALUES ($1, $2, $3, 0, $4)",
        &[&fed, &txid, &session_index, &vec![0u8; 1]],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO transaction_inputs (federation_id, txid, in_index, kind, amount_msat)
         VALUES ($1, $2, 0, $3, $4)",
        &[&fed, &txid, &in_kind, &in_amount_msat],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO transaction_outputs (federation_id, txid, out_index, kind, amount_msat)
         VALUES ($1, $2, 0, $3, $4)",
        &[&fed, &txid, &out_kind, &out_amount_msat],
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn fold_standalone_classifies_peg_in_peg_out_and_ecash() {
    let _guard = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;
    create_ln_schemas(&pool).await;

    let (config, federation_id) = dummy_config();
    insert_federation(&pool, &config, federation_id).await;
    let fed = federation_id.consensus_encode_to_vec();

    pool.get()
        .await
        .unwrap()
        .execute(
            "INSERT INTO gold_progress (federation_id, next_session_index) VALUES ($1, 0)",
            &[&fed],
        )
        .await
        .unwrap();

    insert_session(&pool, &fed, 1).await;

    let peg_in_txid = vec![1u8; 32];
    let peg_out_txid = vec![2u8; 32];
    let ecash_txid = vec![3u8; 32];

    // peg-in: wallet input funds a mint output, in=100k out=99k -> fee 1000
    insert_tx(
        &pool,
        &fed,
        &peg_in_txid,
        1,
        "wallet",
        100_000,
        "mint",
        99_000,
    )
    .await;
    // peg-out: mint input redeemed for a wallet output, in=50k out=49k -> fee 1000
    insert_tx(
        &pool,
        &fed,
        &peg_out_txid,
        1,
        "mint",
        50_000,
        "wallet",
        49_000,
    )
    .await;
    // ecash transfer: mint in, mint out, no fee
    insert_tx(&pool, &fed, &ecash_txid, 1, "mint", 30_000, "mint", 30_000).await;

    {
        let mut conn = pool.get().await.unwrap();
        let dbtx = conn.transaction().await.unwrap();
        fmo_core::gold::fold_sessions(&dbtx, &fed, 0, 2)
            .await
            .unwrap();
        dbtx.commit().await.unwrap();
    }

    let conn = pool.get().await.unwrap();

    let row = conn
        .query_one(
            "SELECT kind, direction, amount_msat, fedimint_fee_msat, num_fedimint_txs
             FROM user_transactions WHERE federation_id = $1 AND user_tx_key = $2",
            &[&fed, &peg_in_txid],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>("kind"), "peg_in");
    assert_eq!(row.get::<_, String>("direction"), "in");
    assert_eq!(row.get::<_, i64>("amount_msat"), 100_000);
    assert_eq!(row.get::<_, i64>("fedimint_fee_msat"), 1_000);
    assert_eq!(row.get::<_, i32>("num_fedimint_txs"), 1);

    let row = conn
        .query_one(
            "SELECT kind, direction, amount_msat, fedimint_fee_msat
             FROM user_transactions WHERE federation_id = $1 AND user_tx_key = $2",
            &[&fed, &peg_out_txid],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>("kind"), "peg_out");
    assert_eq!(row.get::<_, String>("direction"), "out");
    assert_eq!(row.get::<_, i64>("amount_msat"), 49_000);
    assert_eq!(row.get::<_, i64>("fedimint_fee_msat"), 1_000);

    let row = conn
        .query_one(
            "SELECT kind, direction, amount_msat, fedimint_fee_msat
             FROM user_transactions WHERE federation_id = $1 AND user_tx_key = $2",
            &[&fed, &ecash_txid],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>("kind"), "ecash_transfer");
    assert_eq!(row.get::<_, String>("direction"), "internal");
    assert_eq!(row.get::<_, i64>("amount_msat"), 30_000);
    assert_eq!(row.get::<_, i64>("fedimint_fee_msat"), 0);

    // self membership rows
    for txid in [&peg_in_txid, &peg_out_txid, &ecash_txid] {
        let role: String = conn
            .query_one(
                "SELECT role FROM user_transaction_txs
                 WHERE federation_id = $1 AND txid = $2 AND user_tx_key = $2",
                &[&fed, txid],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(role, "self");
    }

    let user_tx_count: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM user_transactions WHERE federation_id = $1",
            &[&fed],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(user_tx_count, 3);
    let membership_count: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM user_transaction_txs WHERE federation_id = $1",
            &[&fed],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(membership_count, 3);
    drop(conn);

    // idempotent: re-running changes nothing
    {
        let mut conn = pool.get().await.unwrap();
        let dbtx = conn.transaction().await.unwrap();
        fmo_core::gold::fold_sessions(&dbtx, &fed, 0, 2)
            .await
            .unwrap();
        dbtx.commit().await.unwrap();
    }
    let conn = pool.get().await.unwrap();
    let user_tx_count: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM user_transactions WHERE federation_id = $1",
            &[&fed],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(user_tx_count, 3, "idempotent: no duplicate rows");
    let membership_count: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM user_transaction_txs WHERE federation_id = $1",
            &[&fed],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        membership_count, 3,
        "idempotent: no duplicate membership rows"
    );
    let row = conn
        .query_one(
            "SELECT fedimint_fee_msat FROM user_transactions WHERE federation_id = $1 AND user_tx_key = $2",
            &[&fed, &peg_in_txid],
        )
        .await
        .unwrap();
    assert_eq!(
        row.get::<_, i64>("fedimint_fee_msat"),
        1_000,
        "idempotent: values unchanged"
    );
}
