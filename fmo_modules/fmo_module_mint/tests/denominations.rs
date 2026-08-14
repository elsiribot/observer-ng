//! Tests for the mint module's `note_denominations` counts: the one-time
//! history backfill baked into the migration, the read query behind the
//! `/denominations` endpoint, and the incremental upsert semantics that
//! `process_output`/`process_input` use at steady state.

use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::TransactionId;
use fmo_core::module::ObserverModule;
use fmo_core::test_util::{insert_federation, minimal_config, reset_db, test_pool};
use fmo_module_mint::MintObserver;

/// Tests share one database (`reset_db` drops/recreates the public schema), so
/// serialize the DB-touching tests within this binary.
static DB_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Resets the public (core) schema and drops any leftover `fmo_mint` schema so
/// `setup_module_schema` re-runs the migration (incl. the backfill) cleanly.
async fn reset_all(pool: &deadpool_postgres::Pool) {
    reset_db(pool).await;
    pool.get()
        .await
        .unwrap()
        .batch_execute("DROP SCHEMA IF EXISTS fmo_mint CASCADE")
        .await
        .unwrap();
}

async fn setup_mint_schema(pool: &deadpool_postgres::Pool) {
    let module = MintObserver;
    fmo_core::db::migrations::setup_module_schema(
        pool,
        "mint",
        module.version(),
        module.migrations(),
    )
    .await
    .unwrap();
}

/// The exact read query behind the `/denominations` endpoint.
async fn read_denominations(
    pool: &deadpool_postgres::Pool,
    fed: &[u8],
) -> Vec<(i64, i64, i64)> {
    pool.get()
        .await
        .unwrap()
        .query(
            "SELECT denomination_msat, issued, GREATEST(issued - spent, 0) AS in_circulation
             FROM fmo_mint.note_denominations
             WHERE federation_id = $1
             ORDER BY denomination_msat",
            &[&fed],
        )
        .await
        .unwrap()
        .into_iter()
        .map(|row| {
            (
                row.get::<_, i64>("denomination_msat"),
                row.get::<_, i64>("issued"),
                row.get::<_, i64>("in_circulation"),
            )
        })
        .collect()
}

/// Seeds one session + one transaction so the input/output foreign keys hold.
async fn seed_transaction(pool: &deadpool_postgres::Pool, fed: &[u8], txid: &[u8]) {
    let conn = pool.get().await.unwrap();
    conn.execute("INSERT INTO sessions VALUES ($1, 0, ''::bytea)", &[&fed])
        .await
        .unwrap();
    conn.execute(
        "INSERT INTO transactions VALUES ($1, $2, 0, 0, ''::bytea)",
        &[&fed, &txid],
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn backfill_counts_history_and_read_clamps_circulation() {
    let _guard = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_all(&pool).await;
    let (config, federation_id) = minimal_config();
    insert_federation(&pool, &config, federation_id).await;

    let fed = federation_id.consensus_encode_to_vec();
    let txid = TransactionId::consensus_decode_whole(&[7; 32], &Default::default()).unwrap();
    let txid_bytes = txid.consensus_encode_to_vec();
    seed_transaction(&pool, &fed, &txid_bytes).await;

    // Seed core mint rows BEFORE the schema exists, so the migration backfill
    // has to count them (this is the production ordering: core rows already
    // present up to the cursor when the migration first runs).
    // issued:  1000 x3, 2000 x2, 4000 x1   spent: 1000 x1, 2000 x2
    // => in circulation: 1000 -> 2, 2000 -> 0 (clamped from 2-2), 4000 -> 1
    {
        let conn = pool.get().await.unwrap();
        let outputs: [(i32, Option<i64>); 7] = [
            (0, Some(1000)),
            (1, Some(1000)),
            (2, Some(1000)),
            (3, Some(2000)),
            (4, Some(2000)),
            (5, Some(4000)),
            // A NULL-amount (undecoded) mint output must be ignored.
            (6, None),
        ];
        for (out_index, amount) in outputs {
            conn.execute(
                "INSERT INTO transaction_outputs VALUES ($1, $2, $3, 'mint', $4, NULL)",
                &[&fed, &txid_bytes, &out_index, &amount],
            )
            .await
            .unwrap();
        }
        // A non-mint output at a mint denomination must NOT be counted.
        conn.execute(
            "INSERT INTO transaction_outputs VALUES ($1, $2, 7, 'wallet', 1000, NULL)",
            &[&fed, &txid_bytes],
        )
        .await
        .unwrap();

        let inputs: [(i32, Option<i64>); 4] = [
            (0, Some(1000)),
            (1, Some(2000)),
            (2, Some(2000)),
            // A NULL-amount (undecoded) mint input must be ignored.
            (3, None),
        ];
        for (in_index, amount) in inputs {
            conn.execute(
                "INSERT INTO transaction_inputs VALUES ($1, $2, $3, 'mint', $4, NULL)",
                &[&fed, &txid_bytes, &in_index, &amount],
            )
            .await
            .unwrap();
        }
    }

    setup_mint_schema(&pool).await;

    assert_eq!(
        read_denominations(&pool, &fed).await,
        vec![(1000, 3, 2), (2000, 2, 0), (4000, 1, 1)]
    );
}

#[tokio::test]
async fn incremental_upserts_accumulate_and_clamp() {
    let _guard = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_all(&pool).await;
    let (config, federation_id) = minimal_config();
    insert_federation(&pool, &config, federation_id).await;

    let fed = federation_id.consensus_encode_to_vec();

    // Empty core tables -> backfill inserts nothing; the counts are then built
    // purely from the incremental upserts below.
    setup_mint_schema(&pool).await;

    let conn = pool.get().await.unwrap();
    conn.batch_execute("SET search_path TO fmo_mint, public")
        .await
        .unwrap();

    // Mirrors the two statements `count_note` issues (kept in sync manually,
    // as they are private to the module).
    let issue = "INSERT INTO note_denominations (federation_id, denomination_msat, issued, spent)
                 VALUES ($1, $2, 1, 0)
                 ON CONFLICT (federation_id, denomination_msat)
                 DO UPDATE SET issued = note_denominations.issued + 1";
    let spend = "INSERT INTO note_denominations (federation_id, denomination_msat, issued, spent)
                 VALUES ($1, $2, 0, 1)
                 ON CONFLICT (federation_id, denomination_msat)
                 DO UPDATE SET spent = note_denominations.spent + 1";

    // 1000: issued x2, spent x1 -> circ 1. 2000: issued x1, spent x2 -> circ 0.
    for _ in 0..2 {
        conn.execute(issue, &[&fed, &1000i64]).await.unwrap();
    }
    conn.execute(spend, &[&fed, &1000i64]).await.unwrap();
    conn.execute(issue, &[&fed, &2000i64]).await.unwrap();
    for _ in 0..2 {
        conn.execute(spend, &[&fed, &2000i64]).await.unwrap();
    }

    assert_eq!(
        read_denominations(&pool, &fed).await,
        vec![(1000, 2, 1), (2000, 1, 0)]
    );
}
