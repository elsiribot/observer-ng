mod common;

use common::{dummy_config, insert_federation, reset_db, test_pool, DB_LOCK};
use fedimint_core::encoding::Encodable;

/// Creates the real `fmo_ln` / `fmo_lnv2` schemas (mirroring
/// `fmo_modules/fmo_module_ln/schema/v0.sql` and
/// `fmo_modules/fmo_module_lnv2/schema/v0.sql`, minus the parts gold folding
/// never reads: gateways, decryption shares). Raw SQL rather than
/// `setup_module_schema` — depending on `fmo_module_ln`/`fmo_module_lnv2`
/// from `fmo_core`'s tests would create a dependency cycle since those crates
/// depend on `fmo_core`.
///
/// fold_standalone's `NOT EXISTS` guards and fold_ln's `to_regclass` checks
/// both rely on these schemas existing (or not, in the lnv1/lnv2-absent
/// tests); this helper is shared by both.
async fn create_ln_schemas(pool: &deadpool_postgres::Pool) {
    pool.get()
        .await
        .unwrap()
        .batch_execute(
            "DROP SCHEMA IF EXISTS fmo_ln CASCADE;
             DROP SCHEMA IF EXISTS fmo_lnv2 CASCADE;
             CREATE SCHEMA IF NOT EXISTS fmo_ln;
             CREATE TABLE IF NOT EXISTS fmo_ln.contracts (
                 federation_id BYTEA NOT NULL REFERENCES public.federations (federation_id),
                 contract_id   BYTEA NOT NULL,
                 type          TEXT  NOT NULL CHECK (type IN ('incoming', 'outgoing')),
                 payment_hash  BYTEA NOT NULL,
                 PRIMARY KEY (federation_id, contract_id)
             );
             CREATE TABLE IF NOT EXISTS fmo_ln.input_contracts (
                 federation_id BYTEA   NOT NULL REFERENCES public.federations (federation_id),
                 txid          BYTEA   NOT NULL,
                 in_index      INTEGER NOT NULL,
                 contract_id   BYTEA   NOT NULL,
                 PRIMARY KEY (federation_id, txid, in_index),
                 FOREIGN KEY (federation_id, txid, in_index)
                     REFERENCES public.transaction_inputs (federation_id, txid, in_index)
             );
             CREATE TABLE IF NOT EXISTS fmo_ln.output_contracts (
                 federation_id    BYTEA   NOT NULL REFERENCES public.federations (federation_id),
                 txid             BYTEA   NOT NULL,
                 out_index        INTEGER NOT NULL,
                 interaction_kind TEXT    NOT NULL CHECK (interaction_kind IN ('fund', 'cancel', 'offer')),
                 contract_id      BYTEA   NOT NULL,
                 PRIMARY KEY (federation_id, txid, out_index),
                 FOREIGN KEY (federation_id, txid, out_index)
                     REFERENCES public.transaction_outputs (federation_id, txid, out_index)
             );
             CREATE SCHEMA IF NOT EXISTS fmo_lnv2;
             CREATE TABLE IF NOT EXISTS fmo_lnv2.contracts (
                 federation_id BYTEA   NOT NULL REFERENCES public.federations (federation_id),
                 contract_id   BYTEA   NOT NULL,
                 type          TEXT    NOT NULL CHECK (type IN ('incoming', 'outgoing')),
                 amount_msat   BIGINT  NOT NULL,
                 txid          BYTEA   NOT NULL,
                 out_index     INTEGER NOT NULL,
                 PRIMARY KEY (federation_id, contract_id)
             );
             CREATE TABLE IF NOT EXISTS fmo_lnv2.input_outpoints (
                 federation_id      BYTEA   NOT NULL REFERENCES public.federations (federation_id),
                 txid               BYTEA   NOT NULL,
                 in_index           INTEGER NOT NULL,
                 type               TEXT    NOT NULL CHECK (type IN ('incoming', 'outgoing')),
                 outpoint_txid      BYTEA   NOT NULL,
                 outpoint_out_index INTEGER NOT NULL,
                 PRIMARY KEY (federation_id, txid, in_index),
                 FOREIGN KEY (federation_id, txid, in_index)
                     REFERENCES public.transaction_inputs (federation_id, txid, in_index)
             );",
        )
        .await
        .unwrap();
}

/// Inserts a bare transaction row (no inputs/outputs) — used for offer legs,
/// which don't necessarily spend/create anything gold cares about beyond the
/// `fmo_ln.output_contracts` row.
async fn insert_bare_tx(pool: &deadpool_postgres::Pool, fed: &[u8], txid: &[u8], session_index: i32) {
    pool.get()
        .await
        .unwrap()
        .execute(
            "INSERT INTO transactions (federation_id, txid, session_index, item_index, data)
             VALUES ($1, $2, $3, 0, $4)",
            &[&fed, &txid, &session_index, &vec![0u8; 1]],
        )
        .await
        .unwrap();
}

async fn insert_tx_input(
    pool: &deadpool_postgres::Pool,
    fed: &[u8],
    txid: &[u8],
    in_index: i32,
    kind: &str,
    amount_msat: i64,
) {
    pool.get()
        .await
        .unwrap()
        .execute(
            "INSERT INTO transaction_inputs (federation_id, txid, in_index, kind, amount_msat)
             VALUES ($1, $2, $3, $4, $5)",
            &[&fed, &txid, &in_index, &kind, &amount_msat],
        )
        .await
        .unwrap();
}

async fn insert_tx_output(
    pool: &deadpool_postgres::Pool,
    fed: &[u8],
    txid: &[u8],
    out_index: i32,
    kind: &str,
    amount_msat: i64,
) {
    pool.get()
        .await
        .unwrap()
        .execute(
            "INSERT INTO transaction_outputs (federation_id, txid, out_index, kind, amount_msat)
             VALUES ($1, $2, $3, $4, $5)",
            &[&fed, &txid, &out_index, &kind, &amount_msat],
        )
        .await
        .unwrap();
}

async fn insert_ln_contract(pool: &deadpool_postgres::Pool, fed: &[u8], contract_id: &[u8], typ: &str) {
    pool.get()
        .await
        .unwrap()
        .execute(
            "INSERT INTO fmo_ln.contracts (federation_id, contract_id, type, payment_hash)
             VALUES ($1, $2, $3, $4)",
            &[&fed, &contract_id, &typ, &vec![0u8; 32]],
        )
        .await
        .unwrap();
}

async fn insert_ln_output_contract(
    pool: &deadpool_postgres::Pool,
    fed: &[u8],
    txid: &[u8],
    out_index: i32,
    interaction_kind: &str,
    contract_id: &[u8],
) {
    pool.get()
        .await
        .unwrap()
        .execute(
            "INSERT INTO fmo_ln.output_contracts (federation_id, txid, out_index, interaction_kind, contract_id)
             VALUES ($1, $2, $3, $4, $5)",
            &[&fed, &txid, &out_index, &interaction_kind, &contract_id],
        )
        .await
        .unwrap();
}

async fn insert_ln_input_contract(
    pool: &deadpool_postgres::Pool,
    fed: &[u8],
    txid: &[u8],
    in_index: i32,
    contract_id: &[u8],
) {
    pool.get()
        .await
        .unwrap()
        .execute(
            "INSERT INTO fmo_ln.input_contracts (federation_id, txid, in_index, contract_id)
             VALUES ($1, $2, $3, $4)",
            &[&fed, &txid, &in_index, &contract_id],
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

/// LN receive spanning three sessions: offer (session 1, no legs of
/// interest), fund (session 2, mint-in 10000 -> ln-out funding contract C),
/// claim (session 3, ln-in C -> mint-out). One `ln_receive` user_transaction
/// row keyed by the contract id, with membership rows for all three legs.
#[tokio::test]
async fn fold_ln_groups_offer_fund_claim_into_one_receive() {
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
    insert_session(&pool, &fed, 2).await;
    insert_session(&pool, &fed, 3).await;

    let contract_id = vec![0xC0u8; 32];
    let offer_txid = vec![0xA1u8; 32];
    let fund_txid = vec![0xA2u8; 32];
    let claim_txid = vec![0xA3u8; 32];

    insert_ln_contract(&pool, &fed, &contract_id, "incoming").await;

    // offer leg: session 1, no inputs/outputs of consequence beyond the
    // offer output itself
    insert_bare_tx(&pool, &fed, &offer_txid, 1).await;
    insert_tx_output(&pool, &fed, &offer_txid, 0, "ln", 0).await;
    insert_ln_output_contract(&pool, &fed, &offer_txid, 0, "offer", &contract_id).await;

    // fund leg: session 2, mint input funds the ln contract for 10000
    insert_bare_tx(&pool, &fed, &fund_txid, 2).await;
    insert_tx_input(&pool, &fed, &fund_txid, 0, "mint", 10_000).await;
    insert_tx_output(&pool, &fed, &fund_txid, 0, "ln", 10_000).await;
    insert_ln_output_contract(&pool, &fed, &fund_txid, 0, "fund", &contract_id).await;

    // claim leg: session 3, ln input spends the contract into a mint output
    insert_bare_tx(&pool, &fed, &claim_txid, 3).await;
    insert_tx_input(&pool, &fed, &claim_txid, 0, "ln", 10_000).await;
    insert_tx_output(&pool, &fed, &claim_txid, 0, "mint", 10_000).await;
    insert_ln_input_contract(&pool, &fed, &claim_txid, 0, &contract_id).await;

    {
        let mut conn = pool.get().await.unwrap();
        let dbtx = conn.transaction().await.unwrap();
        fmo_core::gold::fold_sessions(&dbtx, &fed, 0, 4)
            .await
            .unwrap();
        dbtx.commit().await.unwrap();
    }

    let conn = pool.get().await.unwrap();
    let row = conn
        .query_one(
            "SELECT kind, direction, amount_msat, status, num_fedimint_txs
             FROM user_transactions WHERE federation_id = $1 AND user_tx_key = $2",
            &[&fed, &contract_id],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>("kind"), "ln_receive");
    assert_eq!(row.get::<_, String>("direction"), "in");
    assert_eq!(row.get::<_, i64>("amount_msat"), 10_000);
    assert_eq!(row.get::<_, String>("status"), "completed");
    assert_eq!(row.get::<_, i32>("num_fedimint_txs"), 3);

    let user_tx_count: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM user_transactions WHERE federation_id = $1",
            &[&fed],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(user_tx_count, 1, "no standalone rows for LN-leg txs");

    let mut roles: Vec<String> = conn
        .query(
            "SELECT role FROM user_transaction_txs
             WHERE federation_id = $1 AND user_tx_key = $2 ORDER BY role",
            &[&fed, &contract_id],
        )
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.get(0))
        .collect();
    roles.sort();
    assert_eq!(roles, vec!["claim", "fund", "offer"]);
    drop(conn);

    // idempotent: re-running changes nothing
    {
        let mut conn = pool.get().await.unwrap();
        let dbtx = conn.transaction().await.unwrap();
        fmo_core::gold::fold_sessions(&dbtx, &fed, 0, 4)
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
    assert_eq!(user_tx_count, 1, "idempotent: no duplicate rows");
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
}

/// LN receive with only offer + fund legs (no claim yet): the contract is
/// still `in_flight`, and `num_fedimint_txs` counts just the two legs seen so
/// far.
#[tokio::test]
async fn fold_ln_without_claim_is_in_flight() {
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
    insert_session(&pool, &fed, 2).await;

    let contract_id = vec![0xD0u8; 32];
    let offer_txid = vec![0xB1u8; 32];
    let fund_txid = vec![0xB2u8; 32];

    insert_ln_contract(&pool, &fed, &contract_id, "incoming").await;

    insert_bare_tx(&pool, &fed, &offer_txid, 1).await;
    insert_tx_output(&pool, &fed, &offer_txid, 0, "ln", 0).await;
    insert_ln_output_contract(&pool, &fed, &offer_txid, 0, "offer", &contract_id).await;

    insert_bare_tx(&pool, &fed, &fund_txid, 2).await;
    insert_tx_input(&pool, &fed, &fund_txid, 0, "mint", 5_000).await;
    insert_tx_output(&pool, &fed, &fund_txid, 0, "ln", 5_000).await;
    insert_ln_output_contract(&pool, &fed, &fund_txid, 0, "fund", &contract_id).await;

    {
        let mut conn = pool.get().await.unwrap();
        let dbtx = conn.transaction().await.unwrap();
        fmo_core::gold::fold_sessions(&dbtx, &fed, 0, 3)
            .await
            .unwrap();
        dbtx.commit().await.unwrap();
    }

    let conn = pool.get().await.unwrap();
    let row = conn
        .query_one(
            "SELECT status, num_fedimint_txs, amount_msat
             FROM user_transactions WHERE federation_id = $1 AND user_tx_key = $2",
            &[&fed, &contract_id],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>("status"), "in_flight");
    assert_eq!(row.get::<_, i32>("num_fedimint_txs"), 2);
    assert_eq!(row.get::<_, i64>("amount_msat"), 5_000);
}

/// Graceful degradation: an observer instance with neither the LN nor the
/// LNv2 module installed has no `fmo_ln`/`fmo_lnv2` schema at all.
/// `fold_sessions` (both `fold_standalone`'s guards and `fold_ln`'s blocks)
/// must skip them via `to_regclass` rather than erroring on a missing
/// relation.
#[tokio::test]
async fn fold_sessions_without_ln_modules_installed_does_not_error() {
    let _guard = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;
    // deliberately do NOT create fmo_ln / fmo_lnv2 schemas

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
    let txid = vec![9u8; 32];
    insert_tx(&pool, &fed, &txid, 1, "mint", 30_000, "mint", 30_000).await;

    let mut conn = pool.get().await.unwrap();
    let dbtx = conn.transaction().await.unwrap();
    fmo_core::gold::fold_sessions(&dbtx, &fed, 0, 2)
        .await
        .expect("fold_sessions must not error when fmo_ln/fmo_lnv2 schemas are absent");
    dbtx.commit().await.unwrap();

    let conn = pool.get().await.unwrap();
    let row = conn
        .query_one(
            "SELECT kind FROM user_transactions WHERE federation_id = $1 AND user_tx_key = $2",
            &[&fed, &txid],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>("kind"), "ecash_transfer");
}
