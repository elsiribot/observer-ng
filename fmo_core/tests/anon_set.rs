use fedimint_core::encoding::Encodable;
use fmo_core::test_util::{insert_federation, minimal_config, reset_db, test_pool};

// Serialize DB-touching tests (reset_db drops/recreates public).
static DB_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

// Seed one mint/mintv2 input or output at (session, denom).
#[allow(clippy::too_many_arguments)]
async fn seed_note(
    conn: &deadpool_postgres::Object,
    fed: &[u8],
    session: i32,
    txid: &[u8],
    io: &str, // "in" | "out"
    idx: i32,
    kind: &str, // "mint" | "mintv2"
    denom: i64,
) {
    conn.execute(
        "INSERT INTO sessions VALUES ($1,$2,''::bytea) ON CONFLICT DO NOTHING",
        &[&fed, &session],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO transactions VALUES ($1,$2,$3,0,''::bytea) ON CONFLICT DO NOTHING",
        &[&fed, &txid, &session],
    )
    .await
    .unwrap();
    let table = if io == "in" {
        "transaction_inputs"
    } else {
        "transaction_outputs"
    };
    conn.execute(
        &format!("INSERT INTO {table} VALUES ($1,$2,$3,$4,$5,NULL)"),
        &[&fed, &txid, &idx, &kind, &denom],
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn rebuild_note_circulation_builds_running_curve() {
    let _g = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;
    let (config, fid) = minimal_config();
    insert_federation(&pool, &config, fid).await;
    let fed = fid.consensus_encode_to_vec();
    let conn = pool.get().await.unwrap();

    // session 0: issue 3x1000 (mint). session 2: spend 1x1000 (mint), issue 1x1000
    // (mintv2).
    seed_note(&conn, &fed, 0, &[1; 32], "out", 0, "mint", 1000).await;
    seed_note(&conn, &fed, 0, &[1; 32], "out", 1, "mint", 1000).await;
    seed_note(&conn, &fed, 0, &[1; 32], "out", 2, "mint", 1000).await;
    seed_note(&conn, &fed, 2, &[2; 32], "in", 0, "mint", 1000).await;
    seed_note(&conn, &fed, 2, &[2; 32], "out", 0, "mintv2", 1000).await;

    fmo_core::gold::rebuild_note_circulation(&conn)
        .await
        .unwrap();

    let rows = conn
        .query(
            "SELECT kind, session_index, in_circulation FROM note_circulation
         WHERE federation_id=$1 AND denomination_msat=1000 ORDER BY kind, session_index",
            &[&fed],
        )
        .await
        .unwrap();
    let got: Vec<(String, i32, i64)> = rows
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    // mint: +3 at s0, -1 at s2 -> running 3 then 2. mintv2: +1 at s2.
    assert_eq!(
        got,
        vec![
            ("mint".into(), 0, 3),
            ("mint".into(), 2, 2),
            ("mintv2".into(), 2, 1),
        ]
    );

    // Idempotent: second run yields identical rows.
    fmo_core::gold::rebuild_note_circulation(&conn)
        .await
        .unwrap();
    let n: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM note_circulation WHERE federation_id=$1",
            &[&fed],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(n, 3);
}

#[tokio::test]
async fn backfill_scores_min_over_denominations() {
    let _g = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;
    let (config, fid) = minimal_config();
    insert_federation(&pool, &config, fid).await;
    let fed = fid.consensus_encode_to_vec();
    let conn = pool.get().await.unwrap();

    // Build pools BEFORE session 5: denom 1000 has 16 in circulation, denom
    // 2 has 1024 in circulation (both mint). Seed as direct pool rows.
    conn.execute("DELETE FROM note_circulation", &[])
        .await
        .unwrap();
    conn.execute(
        "INSERT INTO note_circulation VALUES ($1,'mint',1000,0,16)",
        &[&fed],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO note_circulation VALUES ($1,'mint',2,0,1024)",
        &[&fed],
    )
    .await
    .unwrap();

    // A user tx at session 5 spending one 1000-note and one 2-note (mint).
    conn.execute(
        "INSERT INTO sessions VALUES ($1,5,''::bytea) ON CONFLICT DO NOTHING",
        &[&fed],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO transactions VALUES ($1,$2,5,0,''::bytea)",
        &[&fed, &[9u8; 32].as_slice()],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO transaction_inputs VALUES ($1,$2,0,'mint',1000,NULL)",
        &[&fed, &[9u8; 32].as_slice()],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO transaction_inputs VALUES ($1,$2,1,'mint',2,NULL)",
        &[&fed, &[9u8; 32].as_slice()],
    )
    .await
    .unwrap();

    fmo_core::gold::backfill_ecash_anon_bits(&conn)
        .await
        .unwrap();

    // min(log2(16), log2(1024)) = min(4, 10) = 4 bits (the rarer 1000-pool).
    let bits: Option<f64> = conn
        .query_opt(
            "SELECT ecash_anon_bits FROM transaction_privacy WHERE federation_id=$1 AND txid=$2",
            &[&fed, &[9u8; 32].as_slice()],
        )
        .await
        .unwrap()
        .map(|row| row.get(0));
    assert!((bits.unwrap() - 4.0).abs() < 1e-9, "got {bits:?}");
}

#[tokio::test]
async fn maintain_extends_curve_from_prior_tail() {
    let _g = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;
    let (config, fid) = minimal_config();
    insert_federation(&pool, &config, fid).await;
    let fed = fid.consensus_encode_to_vec();
    let mut conn = pool.get().await.unwrap();

    // Prior tail: denom 1000 mint at 5 in circulation as of session 1.
    conn.execute(
        "INSERT INTO note_circulation VALUES ($1,'mint',1000,1,5)",
        &[&fed],
    )
    .await
    .unwrap();
    // New range [2,4): session 2 issues 2x1000, session 3 spends 1x1000.
    seed_note(&conn, &fed, 2, &[3; 32], "out", 0, "mint", 1000).await;
    seed_note(&conn, &fed, 2, &[3; 32], "out", 1, "mint", 1000).await;
    seed_note(&conn, &fed, 3, &[4; 32], "in", 0, "mint", 1000).await;

    let dbtx = conn.transaction().await.unwrap();
    fmo_core::gold::maintain_note_circulation(&dbtx, &fed, 2, 4)
        .await
        .unwrap();
    dbtx.commit().await.unwrap();

    let rows = conn
        .query(
            "SELECT session_index, in_circulation FROM note_circulation
         WHERE federation_id=$1 AND kind='mint' AND denomination_msat=1000 ORDER BY session_index",
            &[&fed],
        )
        .await
        .unwrap();
    let got: Vec<(i32, i64)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();
    // seed 5 (@1) + 2 (@2) = 7, then -1 (@3) = 6.
    assert_eq!(got, vec![(1, 5), (2, 7), (3, 6)]);
}

#[tokio::test]
async fn fold_sessions_scores_new_ecash_tx() {
    let _g = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;
    let (config, fid) = minimal_config();
    insert_federation(&pool, &config, fid).await;
    let fed = fid.consensus_encode_to_vec();
    let mut conn = pool.get().await.unwrap();

    // Pool as of session 0: denom 1000 mint at 8 in circulation.
    conn.execute(
        "INSERT INTO note_circulation VALUES ($1,'mint',1000,0,8)",
        &[&fed],
    )
    .await
    .unwrap();
    // A mint->mint transfer at session 3 spending one 1000-note.
    conn.execute("INSERT INTO sessions VALUES ($1,3,''::bytea)", &[&fed])
        .await
        .unwrap();
    conn.execute(
        "INSERT INTO transactions VALUES ($1,$2,3,0,''::bytea)",
        &[&fed, &[7u8; 32].as_slice()],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO transaction_inputs VALUES ($1,$2,0,'mint',1000,NULL)",
        &[&fed, &[7u8; 32].as_slice()],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO transaction_outputs VALUES ($1,$2,0,'mint',1000,NULL)",
        &[&fed, &[7u8; 32].as_slice()],
    )
    .await
    .unwrap();

    let dbtx = conn.transaction().await.unwrap();
    fmo_core::gold::fold_sessions(&dbtx, &fed, 3, 4)
        .await
        .unwrap();
    dbtx.commit().await.unwrap();

    let bits: Option<f64> = conn
        .query_opt(
            "SELECT ecash_anon_bits FROM transaction_privacy WHERE federation_id=$1 AND txid=$2",
            &[&fed, &[7u8; 32].as_slice()],
        )
        .await
        .unwrap()
        .map(|row| row.get(0));
    // log2(8) = 3 bits (pool strictly before session 3).
    assert!((bits.unwrap() - 3.0).abs() < 1e-9, "got {bits:?}");
}

/// Guards the range-scoping of `ECASH_ANON_BITS_SQL`'s `tx_denoms` CTE
/// (pushed into the forward path by `compute_ecash_anon_bits` to avoid
/// rescanning the federation's entire `transaction_inputs` history every
/// fold batch): folding a narrow range must not touch an already-scored tx
/// whose `first_session_index` falls outside that range.
#[tokio::test]
async fn fold_sessions_does_not_touch_out_of_range_scored_tx() {
    let _g = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;
    let (config, fid) = minimal_config();
    insert_federation(&pool, &config, fid).await;
    let fed = fid.consensus_encode_to_vec();
    let mut conn = pool.get().await.unwrap();

    // tx A at session 3: already scored (e.g. by a prior fold or backfill).
    // Its sentinel value must survive a fold of an unrelated later range.
    conn.execute("INSERT INTO sessions VALUES ($1,3,''::bytea)", &[&fed])
        .await
        .unwrap();
    conn.execute(
        "INSERT INTO transactions VALUES ($1,$2,3,0,''::bytea)",
        &[&fed, &[7u8; 32].as_slice()],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO transaction_inputs VALUES ($1,$2,0,'mint',1000,NULL)",
        &[&fed, &[7u8; 32].as_slice()],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO transaction_privacy (federation_id, txid, ecash_anon_bits)
         VALUES ($1,$2,999.0)",
        &[&fed, &[7u8; 32].as_slice()],
    )
    .await
    .unwrap();

    // tx B at session 5, in a separate later range, to be scored by this fold.
    conn.execute("INSERT INTO sessions VALUES ($1,5,''::bytea)", &[&fed])
        .await
        .unwrap();
    conn.execute(
        "INSERT INTO transactions VALUES ($1,$2,5,0,''::bytea)",
        &[&fed, &[8u8; 32].as_slice()],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO transaction_inputs VALUES ($1,$2,0,'mint',1000,NULL)",
        &[&fed, &[8u8; 32].as_slice()],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO transaction_outputs VALUES ($1,$2,0,'mint',1000,NULL)",
        &[&fed, &[8u8; 32].as_slice()],
    )
    .await
    .unwrap();
    // Pool strictly before session 5: denom 1000 mint at 8 in circulation.
    conn.execute(
        "INSERT INTO note_circulation VALUES ($1,'mint',1000,4,8)",
        &[&fed],
    )
    .await
    .unwrap();

    let dbtx = conn.transaction().await.unwrap();
    fmo_core::gold::fold_sessions(&dbtx, &fed, 5, 6)
        .await
        .unwrap();
    dbtx.commit().await.unwrap();

    let rows = conn
        .query(
            "SELECT txid, ecash_anon_bits FROM transaction_privacy
             WHERE federation_id=$1 ORDER BY txid",
            &[&fed],
        )
        .await
        .unwrap();
    let got: Vec<(Vec<u8>, f64)> = rows.iter().map(|r| (r.get(0), r.get(1))).collect();

    // tx A (out of range) keeps its untouched sentinel score.
    let a = got
        .iter()
        .find(|(txid, _)| txid.as_slice() == [7u8; 32].as_slice())
        .unwrap();
    assert!((a.1 - 999.0).abs() < 1e-9, "got {a:?}");
    // tx B (in range) is freshly scored from the pool: log2(8) = 3 bits.
    let b = got
        .iter()
        .find(|(txid, _)| txid.as_slice() == [8u8; 32].as_slice())
        .unwrap();
    assert!((b.1 - 3.0).abs() < 1e-9, "got {b:?}");
}

/// The anonymity-set estimate is the crowd of possible spenders — `log2(pool)`
/// of the rarest spent denomination — and must NOT grow with the number of
/// notes of that denomination the transaction spends. A consolidation spending
/// many notes of one denomination scores `log2(N)`, not `q * log2(N)` (the old
/// falling-factorial blew up to nonsensical values, e.g. 1545 bits, for such
/// spends).
#[tokio::test]
async fn scoring_ignores_note_count_per_denomination() {
    let _g = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;
    let (config, fid) = minimal_config();
    insert_federation(&pool, &config, fid).await;
    let fed = fid.consensus_encode_to_vec();
    let conn = pool.get().await.unwrap();

    // Pool before session 5: denom 1000 mint at 16 in circulation.
    conn.execute(
        "INSERT INTO note_circulation VALUES ($1,'mint',1000,0,16)",
        &[&fed],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO sessions VALUES ($1,5,''::bytea) ON CONFLICT DO NOTHING",
        &[&fed],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO transactions VALUES ($1,$2,5,0,''::bytea)",
        &[&fed, &[5u8; 32].as_slice()],
    )
    .await
    .unwrap();
    // A consolidation spending FIVE 1000-notes (five input rows, same denom).
    for in_index in 0..5i32 {
        conn.execute(
            "INSERT INTO transaction_inputs VALUES ($1,$2,$3,'mint',1000,NULL)",
            &[&fed, &[5u8; 32].as_slice(), &in_index],
        )
        .await
        .unwrap();
    }

    fmo_core::gold::backfill_ecash_anon_bits(&conn)
        .await
        .unwrap();

    // log2(16) = 4 bits, regardless of the 5 notes spent (NOT 5*4 = 20).
    let bits: Option<f64> = conn
        .query_opt(
            "SELECT ecash_anon_bits FROM transaction_privacy WHERE federation_id=$1 AND txid=$2",
            &[&fed, &[5u8; 32].as_slice()],
        )
        .await
        .unwrap()
        .map(|row| row.get(0));
    assert!((bits.unwrap() - 4.0).abs() < 1e-9, "got {bits:?}");
}

/// `transaction_privacy` is scored by `txid`, independent of the gold-layer
/// `user_transactions` dedup. This is the whole point of moving the score off
/// `user_transactions`: Lightning `fund` legs (and any other tx never
/// materialized as its own `user_transactions` row, e.g. because gold hasn't
/// folded it yet) still get scored as long as the tx + its ecash inputs +
/// the pool are present.
#[tokio::test]
async fn scores_by_txid_without_a_user_transaction() {
    let _g = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;
    let (config, fid) = minimal_config();
    insert_federation(&pool, &config, fid).await;
    let fed = fid.consensus_encode_to_vec();
    let conn = pool.get().await.unwrap();

    // Pool before session 5: denom 1000 mint at 16 in circulation.
    conn.execute(
        "INSERT INTO note_circulation VALUES ($1,'mint',1000,0,16)",
        &[&fed],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO sessions VALUES ($1,5,''::bytea) ON CONFLICT DO NOTHING",
        &[&fed],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO transactions VALUES ($1,$2,5,0,''::bytea)",
        &[&fed, &[6u8; 32].as_slice()],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO transaction_inputs VALUES ($1,$2,0,'mint',1000,NULL)",
        &[&fed, &[6u8; 32].as_slice()],
    )
    .await
    .unwrap();
    // Deliberately NO user_transactions row for this txid.

    fmo_core::gold::backfill_ecash_anon_bits(&conn)
        .await
        .unwrap();

    // log2(16) = 4 bits, scored purely from transactions/transaction_inputs +
    // the pool, with no user_transactions row involved.
    let bits: Option<f64> = conn
        .query_opt(
            "SELECT ecash_anon_bits FROM transaction_privacy WHERE federation_id=$1 AND txid=$2",
            &[&fed, &[6u8; 32].as_slice()],
        )
        .await
        .unwrap()
        .map(|row| row.get(0));
    assert!((bits.unwrap() - 4.0).abs() < 1e-9, "got {bits:?}");
}
