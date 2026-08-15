//! Tests for the mintv2 module's `note_denominations` counts: the one-time
//! history backfill baked into the migration, the read query behind the
//! `/denominations` endpoint, and the incremental upsert semantics that
//! `process_output`/`process_input` use at steady state.

use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::TransactionId;
use fmo_core::module::ObserverModule;
use fmo_core::test_util::{insert_federation, minimal_config, reset_db, test_pool};
use fmo_module_mintv2::MintV2Observer;

/// Tests share one database (`reset_db` drops/recreates the public schema), so
/// serialize the DB-touching tests within this binary.
static DB_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Resets the public (core) schema and drops any leftover `fmo_mintv2` schema
/// so `setup_module_schema` re-runs the migration (incl. the backfill) cleanly.
async fn reset_all(pool: &deadpool_postgres::Pool) {
    reset_db(pool).await;
    pool.get()
        .await
        .unwrap()
        .batch_execute("DROP SCHEMA IF EXISTS fmo_mintv2 CASCADE")
        .await
        .unwrap();
}

async fn setup_mint_schema(pool: &deadpool_postgres::Pool) {
    let module = MintV2Observer;
    fmo_core::db::migrations::setup_module_schema(
        pool,
        "mintv2",
        module.version(),
        module.migrations(),
    )
    .await
    .unwrap();
}

/// The exact read query behind the `/denominations` endpoint. Kept in sync
/// manually with the `denominations(...)` fn in `src/lib.rs` (the module keeps
/// its endpoint SQL private, so tests mirror it verbatim, as done elsewhere in
/// this repo -- e.g. walletv2's LATEST_RESOLVED test). Pads each federation's
/// response to the GLOBAL denomination set, zero-filling denominations this
/// federation never used, with an `EXISTS` guard so a federation with no mint
/// notes of its own returns an empty list.
async fn read_denominations(pool: &deadpool_postgres::Pool, fed: &[u8]) -> Vec<(i64, i64, i64)> {
    pool.get()
        .await
        .unwrap()
        .query(
            "SELECT d.denomination_msat,
                    COALESCE(n.issued, 0) AS issued,
                    GREATEST(COALESCE(n.issued, 0) - COALESCE(n.spent, 0), 0) AS in_circulation
             FROM (SELECT DISTINCT denomination_msat FROM fmo_mintv2.note_denominations) d
             LEFT JOIN fmo_mintv2.note_denominations n
                    ON n.denomination_msat = d.denomination_msat
                   AND n.federation_id = $1
             WHERE EXISTS (SELECT 1 FROM fmo_mintv2.note_denominations f
                           WHERE f.federation_id = $1)
             ORDER BY d.denomination_msat",
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

    // Seed core mintv2 rows BEFORE the schema exists, so the migration backfill
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
                "INSERT INTO transaction_outputs VALUES ($1, $2, $3, 'mintv2', $4, NULL)",
                &[&fed, &txid_bytes, &out_index, &amount],
            )
            .await
            .unwrap();
        }
        // A non-mintv2 output at a mint denomination must NOT be counted.
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
                "INSERT INTO transaction_inputs VALUES ($1, $2, $3, 'mintv2', $4, NULL)",
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
    conn.batch_execute("SET search_path TO fmo_mintv2, public")
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

#[tokio::test]
async fn padding_shares_denomination_axis_across_federations() {
    let _guard = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_all(&pool).await;

    // Three federations with distinct ids. Raw inserts (rather than
    // `insert_federation`/`minimal_config`) since we need multiple distinct ids.
    let fed_a: &[u8] = &[0xaa; 32];
    let fed_b: &[u8] = &[0xbb; 32];
    let fed_c: &[u8] = &[0xcc; 32];
    {
        let conn = pool.get().await.unwrap();
        for fed in [fed_a, fed_b, fed_c] {
            conn.execute(
                "INSERT INTO federations (federation_id, config) VALUES ($1, ''::bytea)",
                &[&fed],
            )
            .await
            .unwrap();
        }
    }

    // Empty core mint tables -> migration backfill inserts nothing; the counts
    // below are seeded directly.
    setup_mint_schema(&pool).await;

    {
        let conn = pool.get().await.unwrap();
        // fed A = {1000: issued 3 spent 0, 2000: issued 2 spent 0}
        // fed B = {1000: issued 1 spent 1, 4000: issued 5 spent 0}
        // fed C = no rows
        let rows: [(&[u8], i64, i64, i64); 4] = [
            (fed_a, 1000, 3, 0),
            (fed_a, 2000, 2, 0),
            (fed_b, 1000, 1, 1),
            (fed_b, 4000, 5, 0),
        ];
        for (fed, denom, issued, spent) in rows {
            conn.execute(
                "INSERT INTO fmo_mintv2.note_denominations
                     (federation_id, denomination_msat, issued, spent)
                 VALUES ($1, $2, $3, $4)",
                &[&fed, &denom, &issued, &spent],
            )
            .await
            .unwrap();
        }
    }

    // Global denomination set is {1000, 2000, 4000}. Each federation is padded
    // to it, zero-filling the denominations it never used.
    assert_eq!(
        read_denominations(&pool, fed_a).await,
        vec![(1000, 3, 3), (2000, 2, 2), (4000, 0, 0)]
    );
    assert_eq!(
        read_denominations(&pool, fed_b).await,
        vec![(1000, 1, 0), (2000, 0, 0), (4000, 5, 5)]
    );
    // Federation with no mint notes of its own returns empty (EXISTS guard).
    assert_eq!(read_denominations(&pool, fed_c).await, Vec::new());
}
