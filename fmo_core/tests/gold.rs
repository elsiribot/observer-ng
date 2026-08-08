mod common;

use common::{dummy_config, insert_federation, reset_db, test_pool, DB_LOCK};
use fedimint_core::encoding::Encodable;

/// Creates the real `fmo_ln` / `fmo_lnv2` schemas (mirroring
/// `fmo_modules/fmo_module_ln/schema/v0.sql` and
/// `fmo_modules/fmo_module_lnv2/schema/v0.sql`, minus the parts gold folding
/// never reads: decryption shares). Includes `fmo_ln.gateways` since
/// `estimate_ln_gateway_fees` reads its `raw` fee schedule. Raw SQL rather
/// than `setup_module_schema` — depending on `fmo_module_ln`/`fmo_module_lnv2`
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
             CREATE TABLE IF NOT EXISTS fmo_ln.gateways (
                 federation_id   BYTEA       NOT NULL REFERENCES public.federations (federation_id),
                 gateway_id      TEXT        NOT NULL,
                 node_pub_key    TEXT        NOT NULL,
                 api_endpoint    TEXT        NOT NULL,
                 lightning_alias TEXT        NOT NULL,
                 vetted          BOOLEAN     NOT NULL DEFAULT FALSE,
                 raw             JSONB       NOT NULL,
                 first_seen      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                 last_seen       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                 PRIMARY KEY (federation_id, gateway_id)
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

/// Inserts a fund-leg output carrying a `details` JSON shaped like the real
/// `LightningOutput` serialization (confirmed against production 2026-08-07):
/// `{"V0":{"Contract":{"contract":{"Outgoing":{"gateway_key":"..."}}}}}`.
/// Used by the gateway-fee-estimate test to exercise the same JSON path
/// `estimate_ln_gateway_fees` reads.
async fn insert_ln_fund_output_with_gateway_key(
    pool: &deadpool_postgres::Pool,
    fed: &[u8],
    txid: &[u8],
    out_index: i32,
    amount_msat: i64,
    gateway_key: &str,
) {
    let details = serde_json::json!({
        "V0": {"Contract": {"contract": {"Outgoing": {"gateway_key": gateway_key}}}}
    });
    pool.get()
        .await
        .unwrap()
        .execute(
            "INSERT INTO transaction_outputs (federation_id, txid, out_index, kind, amount_msat, details)
             VALUES ($1, $2, $3, 'ln', $4, $5)",
            &[&fed, &txid, &out_index, &amount_msat, &details],
        )
        .await
        .unwrap();
}

/// Inserts an `fmo_ln.gateways` row with a `raw` blob shaped like the real
/// gateway poller's stored JSON (confirmed against production 2026-08-07):
/// `{"info":{"gateway_redeem_key":"...","fees":{"base_msat":...,"proportional_millionths":...}}}`.
/// `gateway_redeem_key` — NOT `gateway_id`/`node_pub_key` — is the column
/// `OutgoingContract.gateway_key` matches (verified live: 0/128 matched
/// `gateway_id`/`node_pub_key`, 117/128 matched `raw->info->gateway_redeem_key`).
async fn insert_ln_gateway(
    pool: &deadpool_postgres::Pool,
    fed: &[u8],
    gateway_id: &str,
    gateway_redeem_key: &str,
    base_msat: i64,
    proportional_millionths: i64,
) {
    let raw = serde_json::json!({
        "info": {
            "gateway_redeem_key": gateway_redeem_key,
            "fees": {"base_msat": base_msat, "proportional_millionths": proportional_millionths}
        }
    });
    pool.get()
        .await
        .unwrap()
        .execute(
            "INSERT INTO fmo_ln.gateways
                (federation_id, gateway_id, node_pub_key, api_endpoint, lightning_alias, raw)
             VALUES ($1, $2, $3, 'https://example.com', 'test gateway', $4)",
            &[&fed, &gateway_id, &gateway_id, &raw],
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

/// walletv2 pegs mirror v1 wallet: a walletv2 INPUT is a deposit/peg-in (money
/// entering the federation, classified `peg_in_v2`/`in` with the amount from
/// the walletv2 input side), a walletv2 OUTPUT is a withdrawal/peg-out
/// (`peg_out_v2`/`out`, amount from the walletv2 output side). Regression guard
/// for the previously-inverted v2 arms.
#[tokio::test]
async fn fold_standalone_classifies_walletv2_pegs() {
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

    // peg-in v2: walletv2 INPUT funds a mintv2 output, in=100k out=99k -> fee 1000
    let peg_in_txid = vec![0x21u8; 32];
    insert_tx(&pool, &fed, &peg_in_txid, 1, "walletv2", 100_000, "mintv2", 99_000).await;
    // peg-out v2: mintv2 input redeemed for a walletv2 OUTPUT, in=50k out=49k -> fee 1000
    let peg_out_txid = vec![0x22u8; 32];
    insert_tx(&pool, &fed, &peg_out_txid, 1, "mintv2", 50_000, "walletv2", 49_000).await;

    {
        let mut conn = pool.get().await.unwrap();
        let dbtx = conn.transaction().await.unwrap();
        fmo_core::gold::fold_sessions(&dbtx, &fed, 0, 2).await.unwrap();
        dbtx.commit().await.unwrap();
    }

    let conn = pool.get().await.unwrap();

    let row = conn
        .query_one(
            "SELECT kind, direction, amount_msat, fedimint_fee_msat
             FROM user_transactions WHERE federation_id = $1 AND user_tx_key = $2",
            &[&fed, &peg_in_txid],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>("kind"), "peg_in_v2");
    assert_eq!(row.get::<_, String>("direction"), "in");
    assert_eq!(row.get::<_, i64>("amount_msat"), 100_000);
    assert_eq!(row.get::<_, i64>("fedimint_fee_msat"), 1_000);

    let row = conn
        .query_one(
            "SELECT kind, direction, amount_msat, fedimint_fee_msat
             FROM user_transactions WHERE federation_id = $1 AND user_tx_key = $2",
            &[&fed, &peg_out_txid],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>("kind"), "peg_out_v2");
    assert_eq!(row.get::<_, String>("direction"), "out");
    assert_eq!(row.get::<_, i64>("amount_msat"), 49_000);
    assert_eq!(row.get::<_, i64>("fedimint_fee_msat"), 1_000);
}

/// I2 regression: gold routinely folds a session BEFORE `session_times` covers
/// it (session_times is an async matview refreshed in `refresh_views_inner`),
/// so `first_timestamp` is NULL and the row is dropped from `user_tx_daily`
/// (`WHERE first_timestamp IS NOT NULL`). The refresh-cycle self-heal
/// (`heal_gold`, run after session_times refresh, before user_tx_daily refresh)
/// must backfill the timestamp so the row appears in the rollup.
#[tokio::test]
async fn heal_gold_backfills_timestamp_and_populates_daily() {
    let _guard = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;

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
    let peg_in_txid = vec![0x31u8; 32];
    insert_tx(&pool, &fed, &peg_in_txid, 1, "wallet", 100_000, "mint", 99_000).await;

    // Fold BEFORE any session_time vote exists.
    {
        let mut conn = pool.get().await.unwrap();
        let dbtx = conn.transaction().await.unwrap();
        fmo_core::gold::fold_sessions(&dbtx, &fed, 0, 2).await.unwrap();
        dbtx.commit().await.unwrap();
    }

    let conn = pool.get().await.unwrap();
    let ts_before: Option<std::time::SystemTime> = conn
        .query_one(
            "SELECT first_timestamp FROM user_transactions WHERE federation_id = $1 AND user_tx_key = $2",
            &[&fed, &peg_in_txid],
        )
        .await
        .unwrap()
        .get(0);
    assert!(ts_before.is_none(), "first_timestamp must be NULL before the vote exists");

    conn.batch_execute("REFRESH MATERIALIZED VIEW user_tx_daily")
        .await
        .unwrap();
    let daily_before: i64 = conn
        .query_one("SELECT COUNT(*) FROM user_tx_daily WHERE federation_id = $1", &[&fed])
        .await
        .unwrap()
        .get(0);
    assert_eq!(daily_before, 0, "timestamp-less row must be absent from user_tx_daily");
    drop(conn);

    // Simulate a refresh cycle: vote arrives, session_times refreshes, heal runs.
    let conn = pool.get().await.unwrap();
    conn.execute(
        "INSERT INTO session_time_votes (federation_id, session_index, source_kind, peer_id, timestamp)
         VALUES ($1, 1, 'wallet', 0, '2024-01-15 12:00:00')",
        &[&fed],
    )
    .await
    .unwrap();
    conn.batch_execute("REFRESH MATERIALIZED VIEW session_times")
        .await
        .unwrap();
    fmo_core::gold::heal_gold(&conn).await.unwrap();

    let ts_after: Option<std::time::SystemTime> = conn
        .query_one(
            "SELECT first_timestamp FROM user_transactions WHERE federation_id = $1 AND user_tx_key = $2",
            &[&fed, &peg_in_txid],
        )
        .await
        .unwrap()
        .get(0);
    assert!(ts_after.is_some(), "heal must backfill first_timestamp from session_times");

    conn.batch_execute("REFRESH MATERIALIZED VIEW user_tx_daily")
        .await
        .unwrap();
    let daily_after: i64 = conn
        .query_one("SELECT COUNT(*) FROM user_tx_daily WHERE federation_id = $1", &[&fed])
        .await
        .unwrap()
        .get(0);
    assert_eq!(daily_after, 1, "healed row must now appear in user_tx_daily");
}

/// I3 regression: a walletv2 peg-in's amount comes from balance inference
/// (`fmo_module_walletv2` returns `amount: None`), which runs asynchronously in
/// `refresh_views_inner`. Gold folds the row before that, so `amount_msat` is
/// NULL. After inference fills the underlying input amount, the refresh-cycle
/// self-heal (`heal_gold`) must backfill the user_transaction amount/fee.
#[tokio::test]
async fn heal_gold_backfills_walletv2_pegin_amount_after_inference() {
    let _guard = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;

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

    // walletv2 peg-in: input amount is NULL (module can't decode it), output
    // mintv2 amount is known. Balance inference will later fill the input.
    let peg_in_txid = vec![0x41u8; 32];
    let conn = pool.get().await.unwrap();
    conn.execute(
        "INSERT INTO transactions (federation_id, txid, session_index, item_index, data)
         VALUES ($1, $2, 1, 0, $3)",
        &[&fed, &peg_in_txid, &vec![0u8; 1]],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO transaction_inputs (federation_id, txid, in_index, kind, amount_msat)
         VALUES ($1, $2, 0, 'walletv2', NULL)",
        &[&fed, &peg_in_txid],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO transaction_outputs (federation_id, txid, out_index, kind, amount_msat)
         VALUES ($1, $2, 0, 'mintv2', 100000)",
        &[&fed, &peg_in_txid],
    )
    .await
    .unwrap();
    // module_progress so inference considers session 1 fully processed
    conn.execute(
        "INSERT INTO module_progress (module_kind, federation_id, next_session_index)
         VALUES ('mintv2', $1, 2), ('walletv2', $1, 2)",
        &[&fed],
    )
    .await
    .unwrap();
    drop(conn);

    // Fold BEFORE inference: amount is NULL.
    {
        let mut conn = pool.get().await.unwrap();
        let dbtx = conn.transaction().await.unwrap();
        fmo_core::gold::fold_sessions(&dbtx, &fed, 0, 2).await.unwrap();
        dbtx.commit().await.unwrap();
    }

    let conn = pool.get().await.unwrap();
    let row = conn
        .query_one(
            "SELECT kind, direction, amount_msat FROM user_transactions
             WHERE federation_id = $1 AND user_tx_key = $2",
            &[&fed, &peg_in_txid],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>("kind"), "peg_in_v2");
    assert_eq!(row.get::<_, String>("direction"), "in");
    let amt_before: Option<i64> = row.get("amount_msat");
    assert!(amt_before.is_none(), "amount must be NULL before inference fills the input");

    // Refresh cycle: inference fills the input amount, then heal backfills gold.
    let (inputs, _outputs) = fmo_core::amounts::infer_missing_amounts(&conn).await.unwrap();
    assert_eq!(inputs, 1, "inference must fill the one NULL walletv2 input");
    fmo_core::gold::heal_gold(&conn).await.unwrap();

    let row = conn
        .query_one(
            "SELECT amount_msat, fedimint_fee_msat FROM user_transactions
             WHERE federation_id = $1 AND user_tx_key = $2",
            &[&fed, &peg_in_txid],
        )
        .await
        .unwrap();
    assert_eq!(
        row.get::<_, Option<i64>>("amount_msat"),
        Some(100_000),
        "heal must backfill the peg_in_v2 amount from the inferred input"
    );
    assert_eq!(
        row.get::<_, Option<i64>>("fedimint_fee_msat"),
        Some(0),
        "fee = inputs - outputs = 100000 - 100000 = 0 after inference"
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

/// Perf-fix regression: `fold_ln_v1`'s `funds`/`legs` subqueries must be
/// scoped to `LN_TOUCHED_CONTRACTS` (this batch's touched contracts) rather
/// than aggregating over every contract in `fmo_ln` — an unscoped aggregation
/// turned each 500-session batch into a full-table scan (30-60 minutes in
/// production, with 100k-266k contracts per federation). This proves the
/// scoping is correct on the two axes that matter:
///
/// - Contract A is touched by the SECOND batch (`[3,4)`) only via its claim
///   leg; its fund leg was ingested and folded in an EARLIER batch
///   (`[1,3)`), so it lives in `fmo_ln.output_contracts` at session 1, well
///   outside `[3,4)`. `LN_TOUCHED_CONTRACTS` matches contracts by
///   `(federation_id, contract_id)`, not by session, so the scoped
///   `funds`/`legs` subqueries must still pick up A's session-1 fund leg to
///   compute the right amount/status/`num_fedimint_txs` — proving the fix
///   preserves "full lifecycle of touched contracts", not "full lifecycle of
///   ALL contracts" (the bug) and not "only this batch's session range" (a
///   different, equally wrong, fix).
/// - Contract B is a fully independent, already-completed contract whose
///   legs are entirely within the FIRST batch's range (`[1,3)`) — it has no
///   leg in `[3,4)`, so it must be absent from `LN_TOUCHED_CONTRACTS` for the
///   second batch and must come out of that batch byte-for-byte unchanged
///   (not recreated, not updated). A and B are funded for different amounts,
///   so any cross-contract contamination in the scoped subqueries would show
///   up as a wrong amount on one of them.
#[tokio::test]
async fn fold_ln_scopes_aggregation_to_touched_contracts_only() {
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

    // Contract A: fund leg only, session 1. Its claim leg doesn't exist yet
    // (simulating that session 3 hasn't been ingested by the LN module yet).
    let contract_a = vec![0xAAu8; 32];
    let a_fund_txid = vec![0xA1u8; 32];
    insert_ln_contract(&pool, &fed, &contract_a, "incoming").await;
    insert_bare_tx(&pool, &fed, &a_fund_txid, 1).await;
    insert_tx_input(&pool, &fed, &a_fund_txid, 0, "mint", 10_000).await;
    insert_tx_output(&pool, &fed, &a_fund_txid, 0, "ln", 10_000).await;
    insert_ln_output_contract(&pool, &fed, &a_fund_txid, 0, "fund", &contract_a).await;

    // Contract B: unrelated, fully funded+claimed within sessions 1-2, for a
    // different amount from A so cross-contamination would be visible.
    let contract_b = vec![0xBBu8; 32];
    let b_fund_txid = vec![0xB1u8; 32];
    let b_claim_txid = vec![0xB2u8; 32];
    insert_ln_contract(&pool, &fed, &contract_b, "incoming").await;
    insert_bare_tx(&pool, &fed, &b_fund_txid, 1).await;
    insert_tx_input(&pool, &fed, &b_fund_txid, 0, "mint", 7_777).await;
    insert_tx_output(&pool, &fed, &b_fund_txid, 0, "ln", 7_777).await;
    insert_ln_output_contract(&pool, &fed, &b_fund_txid, 0, "fund", &contract_b).await;
    insert_bare_tx(&pool, &fed, &b_claim_txid, 2).await;
    insert_tx_input(&pool, &fed, &b_claim_txid, 0, "ln", 7_777).await;
    insert_tx_output(&pool, &fed, &b_claim_txid, 0, "mint", 7_777).await;
    insert_ln_input_contract(&pool, &fed, &b_claim_txid, 0, &contract_b).await;

    // First batch [1,3): folds B fully (fund+claim both in range) and A's
    // fund leg only (A has no claim leg in the DB yet) -> A stays in_flight.
    {
        let mut conn = pool.get().await.unwrap();
        let dbtx = conn.transaction().await.unwrap();
        fmo_core::gold::fold_sessions(&dbtx, &fed, 1, 3)
            .await
            .unwrap();
        dbtx.commit().await.unwrap();
    }

    let conn = pool.get().await.unwrap();
    let b_row_before = conn
        .query_one(
            "SELECT kind, direction, amount_msat, status, num_fedimint_txs
             FROM user_transactions WHERE federation_id = $1 AND user_tx_key = $2",
            &[&fed, &contract_b],
        )
        .await
        .unwrap();
    assert_eq!(b_row_before.get::<_, String>("status"), "completed");
    assert_eq!(b_row_before.get::<_, i64>("amount_msat"), 7_777);
    assert_eq!(b_row_before.get::<_, i32>("num_fedimint_txs"), 2);

    let a_row_before = conn
        .query_one(
            "SELECT status, num_fedimint_txs FROM user_transactions
             WHERE federation_id = $1 AND user_tx_key = $2",
            &[&fed, &contract_a],
        )
        .await
        .unwrap();
    assert_eq!(a_row_before.get::<_, String>("status"), "in_flight");
    assert_eq!(a_row_before.get::<_, i32>("num_fedimint_txs"), 1);
    drop(conn);

    // Now A's claim leg lands (simulating session 3 finally being ingested by
    // the LN module).
    let a_claim_txid = vec![0xA3u8; 32];
    insert_bare_tx(&pool, &fed, &a_claim_txid, 3).await;
    insert_tx_input(&pool, &fed, &a_claim_txid, 0, "ln", 10_000).await;
    insert_tx_output(&pool, &fed, &a_claim_txid, 0, "mint", 10_000).await;
    insert_ln_input_contract(&pool, &fed, &a_claim_txid, 0, &contract_a).await;

    // Second batch [3,4): touches A only, via its claim leg. B has no leg in
    // this range, so it must be absent from LN_TOUCHED_CONTRACTS entirely.
    {
        let mut conn = pool.get().await.unwrap();
        let dbtx = conn.transaction().await.unwrap();
        fmo_core::gold::fold_sessions(&dbtx, &fed, 3, 4)
            .await
            .unwrap();
        dbtx.commit().await.unwrap();
    }

    let conn = pool.get().await.unwrap();

    // A: now completed, with the FULL lifecycle amount (session-1 fund leg +
    // session-3 claim leg) even though only the claim leg was in this batch's
    // session range -- proving the scoped funds/legs subqueries still
    // aggregate the touched contract's full history, not just the batch's
    // session range, and not some blend with contract B's data.
    let a_row_after = conn
        .query_one(
            "SELECT kind, direction, amount_msat, status, num_fedimint_txs
             FROM user_transactions WHERE federation_id = $1 AND user_tx_key = $2",
            &[&fed, &contract_a],
        )
        .await
        .unwrap();
    assert_eq!(a_row_after.get::<_, String>("kind"), "ln_receive");
    assert_eq!(a_row_after.get::<_, String>("status"), "completed");
    assert_eq!(
        a_row_after.get::<_, i64>("amount_msat"),
        10_000,
        "A's amount must be its own fund leg, not contaminated by B's"
    );
    assert_eq!(a_row_after.get::<_, i32>("num_fedimint_txs"), 2);

    let mut a_roles: Vec<String> = conn
        .query(
            "SELECT role FROM user_transaction_txs
             WHERE federation_id = $1 AND user_tx_key = $2 ORDER BY role",
            &[&fed, &contract_a],
        )
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    a_roles.sort();
    assert_eq!(a_roles, vec!["claim", "fund"]);

    // B: byte-for-byte unchanged and not duplicated by a batch that doesn't
    // touch any of its legs.
    let b_row_after = conn
        .query_one(
            "SELECT kind, direction, amount_msat, status, num_fedimint_txs
             FROM user_transactions WHERE federation_id = $1 AND user_tx_key = $2",
            &[&fed, &contract_b],
        )
        .await
        .unwrap();
    assert_eq!(
        b_row_after.get::<_, String>("kind"),
        b_row_before.get::<_, String>("kind")
    );
    assert_eq!(
        b_row_after.get::<_, String>("direction"),
        b_row_before.get::<_, String>("direction")
    );
    assert_eq!(
        b_row_after.get::<_, i64>("amount_msat"),
        b_row_before.get::<_, i64>("amount_msat")
    );
    assert_eq!(
        b_row_after.get::<_, String>("status"),
        b_row_before.get::<_, String>("status")
    );
    assert_eq!(
        b_row_after.get::<_, i32>("num_fedimint_txs"),
        b_row_before.get::<_, i32>("num_fedimint_txs")
    );

    let b_tx_count: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM user_transactions WHERE federation_id = $1 AND user_tx_key = $2",
            &[&fed, &contract_b],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        b_tx_count, 1,
        "B must not be duplicated/recreated by a batch that doesn't touch it"
    );
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

/// An unpaid Lightning invoice: an lnv1 `offer` output with NO fund and NO
/// claim. It moved no value, so per spec it is not a user transaction and
/// must produce neither a `user_transactions` row nor a membership row.
/// Regression guard: the membership insert must not emit an orphan
/// `user_transaction_txs` row keyed by a contract with no parent
/// `user_transactions` row — that would violate the FK and stall the gold
/// processor for the whole federation (production has hundreds of thousands
/// of unpaid invoices).
#[tokio::test]
async fn fold_ln_offer_only_unfunded_invoice_produces_nothing() {
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

    let contract_id = vec![0xE0u8; 32];
    let offer_txid = vec![0xC1u8; 32];

    insert_ln_contract(&pool, &fed, &contract_id, "incoming").await;

    // offer leg only: no fund output, no claim input
    insert_bare_tx(&pool, &fed, &offer_txid, 1).await;
    insert_tx_output(&pool, &fed, &offer_txid, 0, "ln", 0).await;
    insert_ln_output_contract(&pool, &fed, &offer_txid, 0, "offer", &contract_id).await;

    {
        let mut conn = pool.get().await.unwrap();
        let dbtx = conn.transaction().await.unwrap();
        fmo_core::gold::fold_sessions(&dbtx, &fed, 0, 2)
            .await
            .expect("fold_sessions must not error on an unfunded offer-only contract");
        dbtx.commit().await.unwrap();
    }

    let conn = pool.get().await.unwrap();
    let user_tx_count: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM user_transactions WHERE federation_id = $1 AND user_tx_key = $2",
            &[&fed, &contract_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(user_tx_count, 0, "unfunded offer is not a user transaction");
    let membership_count: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM user_transaction_txs WHERE federation_id = $1 AND user_tx_key = $2",
            &[&fed, &contract_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(membership_count, 0, "no orphan membership rows for an unfunded offer");
}

/// `gateway_fee_estimate_msat` for outgoing LN sends: the gateway fee isn't
/// on-ledger, so it's estimated by inverting the gateway's advertised fee
/// schedule against the gross contract amount. Contract funded for 10520
/// msat, gateway advertises base_msat=2000, proportional_millionths=5000
/// (0.5%): invoice = (10520-2000)/(1+5000/1e6) ≈ 8477.6, so
/// gateway_fee_estimate_msat = 10520 - round(invoice) ≈ 2043 (±1 for
/// rounding). An `ln_receive` row and a non-LN (`peg_in`) row touched in the
/// same batch must stay NULL — the gateway fee only applies to the outgoing
/// leg of a payment.
#[tokio::test]
async fn fold_ln_estimates_gateway_fee_for_outgoing_send() {
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

    let gateway_key = "02aabbccddeeff00112233445566778899aabbccddeeff00112233445566778";
    insert_ln_gateway(&pool, &fed, "gw1", gateway_key, 2000, 5000).await;

    // outgoing (ln_send) contract funded 10520 msat via a gateway with a
    // known fee schedule
    let send_contract_id = vec![0xF0u8; 32];
    let send_fund_txid = vec![0xF1u8; 32];
    insert_ln_contract(&pool, &fed, &send_contract_id, "outgoing").await;
    insert_bare_tx(&pool, &fed, &send_fund_txid, 1).await;
    insert_tx_input(&pool, &fed, &send_fund_txid, 0, "mint", 10_520).await;
    insert_ln_fund_output_with_gateway_key(
        &pool,
        &fed,
        &send_fund_txid,
        0,
        10_520,
        gateway_key,
    )
    .await;
    insert_ln_output_contract(&pool, &fed, &send_fund_txid, 0, "fund", &send_contract_id).await;

    // incoming (ln_receive) contract — no gateway fee applies
    let recv_contract_id = vec![0xF2u8; 32];
    let recv_fund_txid = vec![0xF3u8; 32];
    insert_ln_contract(&pool, &fed, &recv_contract_id, "incoming").await;
    insert_bare_tx(&pool, &fed, &recv_fund_txid, 1).await;
    insert_tx_input(&pool, &fed, &recv_fund_txid, 0, "mint", 5_000).await;
    insert_tx_output(&pool, &fed, &recv_fund_txid, 0, "ln", 5_000).await;
    insert_ln_output_contract(&pool, &fed, &recv_fund_txid, 0, "fund", &recv_contract_id).await;

    // non-LN (peg_in) standalone tx — kind != ln_send, must stay untouched
    let peg_in_txid = vec![0xF4u8; 32];
    insert_tx(&pool, &fed, &peg_in_txid, 1, "wallet", 1_000, "mint", 990).await;

    {
        let mut conn = pool.get().await.unwrap();
        let dbtx = conn.transaction().await.unwrap();
        fmo_core::gold::fold_sessions(&dbtx, &fed, 0, 2)
            .await
            .unwrap();
        dbtx.commit().await.unwrap();
    }

    let conn = pool.get().await.unwrap();

    let fee: Option<i64> = conn
        .query_one(
            "SELECT gateway_fee_estimate_msat FROM user_transactions
             WHERE federation_id = $1 AND user_tx_key = $2",
            &[&fed, &send_contract_id],
        )
        .await
        .unwrap()
        .get(0);
    let fee = fee.expect("ln_send row must have a gateway fee estimate");
    assert!(
        (2042..=2044).contains(&fee),
        "expected gateway_fee_estimate_msat ~= 2043 (+/-1), got {fee}"
    );

    let recv_fee: Option<i64> = conn
        .query_one(
            "SELECT gateway_fee_estimate_msat FROM user_transactions
             WHERE federation_id = $1 AND user_tx_key = $2",
            &[&fed, &recv_contract_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(recv_fee, None, "ln_receive must not get a gateway fee estimate");

    let peg_in_fee: Option<i64> = conn
        .query_one(
            "SELECT gateway_fee_estimate_msat FROM user_transactions
             WHERE federation_id = $1 AND user_tx_key = $2",
            &[&fed, &peg_in_txid],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(peg_in_fee, None, "non-LN rows must not get a gateway fee estimate");
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
    let fee_again: Option<i64> = conn
        .query_one(
            "SELECT gateway_fee_estimate_msat FROM user_transactions
             WHERE federation_id = $1 AND user_tx_key = $2",
            &[&fed, &send_contract_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(fee_again, Some(fee), "idempotent: value unchanged");
}

/// Fee-schedule drift: a gateway's CURRENT advertised `base_msat` (5000) is
/// larger than an old contract's gross amount (3000). Naively inverting the
/// fee would give `invoice = (3000-5000)/(1+ppm) < 0` and an estimated "fee"
/// of ~4990 msat — larger than the whole contract, a nonsense
/// fee-transparency number. The estimator must leave `gateway_fee_estimate_msat`
/// NULL (unknown), never a negative or >contract value, and must not error.
#[tokio::test]
async fn fold_ln_leaves_gateway_fee_null_on_schedule_drift() {
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

    let gateway_key = "03ffeeddccbbaa00998877665544332211ffeeddccbbaa009988776655443322";
    // base_msat (5000) > the contract amount (3000): schedule drift
    insert_ln_gateway(&pool, &fed, "gw1", gateway_key, 5000, 5000).await;

    let send_contract_id = vec![0xC5u8; 32];
    let send_fund_txid = vec![0xC6u8; 32];
    insert_ln_contract(&pool, &fed, &send_contract_id, "outgoing").await;
    insert_bare_tx(&pool, &fed, &send_fund_txid, 1).await;
    insert_tx_input(&pool, &fed, &send_fund_txid, 0, "mint", 3_000).await;
    insert_ln_fund_output_with_gateway_key(&pool, &fed, &send_fund_txid, 0, 3_000, gateway_key)
        .await;
    insert_ln_output_contract(&pool, &fed, &send_fund_txid, 0, "fund", &send_contract_id).await;

    {
        let mut conn = pool.get().await.unwrap();
        let dbtx = conn.transaction().await.unwrap();
        fmo_core::gold::fold_sessions(&dbtx, &fed, 0, 2)
            .await
            .expect("fold_sessions must not error on schedule drift");
        dbtx.commit().await.unwrap();
    }

    let conn = pool.get().await.unwrap();
    let fee: Option<i64> = conn
        .query_one(
            "SELECT gateway_fee_estimate_msat FROM user_transactions
             WHERE federation_id = $1 AND user_tx_key = $2",
            &[&fed, &send_contract_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        fee, None,
        "schedule drift (base > contract) must leave the fee NULL, not a nonsense value"
    );
}

/// `user_tx_daily` is a materialized view, not auto-refreshed on write: after
/// folding a couple of standalone transactions it must still be empty until
/// explicitly refreshed. This guards the periodic refresh loop
/// (`refresh_views_inner` in `fmo_core::api`) which must include
/// `"user_tx_daily"` in its matview list alongside `"session_times"` for the
/// gold rollup to ever become visible in production.
#[tokio::test]
async fn user_tx_daily_rolls_up_after_refresh() {
    let _guard = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;

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
    // Give session 1 a real timestamp via the same path production uses
    // (module-contributed votes aggregated into `session_times`), so the
    // resulting `user_transactions` rows have a non-null `first_timestamp`
    // -- `user_tx_daily` only rolls up rows where that's set.
    pool.get()
        .await
        .unwrap()
        .execute(
            "INSERT INTO session_time_votes (federation_id, session_index, source_kind, peer_id, timestamp)
             VALUES ($1, 1, 'wallet', 0, '2024-01-15 12:00:00')",
            &[&fed],
        )
        .await
        .unwrap();
    pool.get()
        .await
        .unwrap()
        .batch_execute("REFRESH MATERIALIZED VIEW session_times")
        .await
        .unwrap();

    let peg_in_1 = vec![10u8; 32];
    let peg_in_2 = vec![11u8; 32];
    let peg_out = vec![12u8; 32];
    insert_tx(&pool, &fed, &peg_in_1, 1, "wallet", 100_000, "mint", 99_000).await;
    insert_tx(&pool, &fed, &peg_in_2, 1, "wallet", 200_000, "mint", 198_000).await;
    insert_tx(&pool, &fed, &peg_out, 1, "mint", 50_000, "wallet", 49_000).await;

    {
        let mut conn = pool.get().await.unwrap();
        let dbtx = conn.transaction().await.unwrap();
        fmo_core::gold::fold_sessions(&dbtx, &fed, 0, 2)
            .await
            .unwrap();
        dbtx.commit().await.unwrap();
    }

    let conn = pool.get().await.unwrap();

    // Sanity: the fold produced the rows we expect, with a real timestamp.
    let user_tx_count: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM user_transactions WHERE federation_id = $1 AND first_timestamp IS NOT NULL",
            &[&fed],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(user_tx_count, 3);

    // Red: the matview is not auto-refreshed, so it must still be empty.
    let daily_count_before: i64 = conn
        .query_one("SELECT COUNT(*) FROM user_tx_daily WHERE federation_id = $1", &[&fed])
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        daily_count_before, 0,
        "user_tx_daily must not auto-populate without an explicit refresh"
    );

    conn.batch_execute("REFRESH MATERIALIZED VIEW user_tx_daily")
        .await
        .unwrap();

    // Green: after refresh, peg_in rows (same kind/direction/status) roll up
    // together and peg_out stays a separate group.
    let peg_in_row = conn
        .query_one(
            "SELECT tx_count, volume_msat::bigint, fedimint_fee_msat::bigint
             FROM user_tx_daily
             WHERE federation_id = $1 AND day = '2024-01-15' AND kind = 'peg_in'
               AND direction = 'in' AND status = 'completed'",
            &[&fed],
        )
        .await
        .unwrap();
    assert_eq!(peg_in_row.get::<_, i64>("tx_count"), 2);
    assert_eq!(peg_in_row.get::<_, i64>("volume_msat"), 300_000);
    assert_eq!(peg_in_row.get::<_, i64>("fedimint_fee_msat"), 3_000);

    let peg_out_row = conn
        .query_one(
            "SELECT tx_count, volume_msat::bigint, fedimint_fee_msat::bigint
             FROM user_tx_daily
             WHERE federation_id = $1 AND day = '2024-01-15' AND kind = 'peg_out'
               AND direction = 'out' AND status = 'completed'",
            &[&fed],
        )
        .await
        .unwrap();
    assert_eq!(peg_out_row.get::<_, i64>("tx_count"), 1);
    assert_eq!(peg_out_row.get::<_, i64>("volume_msat"), 49_000);
    assert_eq!(peg_out_row.get::<_, i64>("fedimint_fee_msat"), 1_000);
}
