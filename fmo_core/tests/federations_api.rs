mod common;

use common::{dummy_config, insert_federation, reset_db, test_pool, DB_LOCK};
use fedimint_core::config::FederationId;
use fedimint_core::encoding::Encodable;
use fmo_core::observer::FederationObserver;
use fmo_core::registry::ModuleRegistry;

/// Regression test for per-federation histogram isolation. The histogram is
/// now served from the `federation_tx_daily` matview, which groups by
/// `federation_id`. Two federations share the same session/date and txid bytes
/// so a matview that failed to key on `federation_id` would double-count or
/// drop amounts; verify each federation's histogram only reflects its own
/// inputs.
#[tokio::test]
async fn histogram_is_isolated_per_federation() {
    let _guard = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;

    let (config_a, fed_a) = dummy_config();
    insert_federation(&pool, &config_a, fed_a).await;
    let fed_a_bytes = fed_a.consensus_encode_to_vec();

    // Second federation: same config shape but a distinct id is required, so
    // reuse the dummy config but insert directly under a different id via a
    // second `insert_federation` call is not possible (config determines the
    // id), so build a second federation by inserting the row manually with a
    // different id derived from a tweaked config peer name.
    let (config_b, fed_b) = {
        use std::collections::BTreeMap;

        use fedimint_core::config::{
            ClientConfig, ClientModuleConfig, GlobalClientConfig, PeerUrl,
        };
        use fedimint_core::core::ModuleKind;
        use fedimint_core::module::CoreConsensusVersion;
        use fedimint_core::PeerId;
        use fedimint_dummy_common::config::DummyClientConfig;

        let config = ClientConfig {
            global: GlobalClientConfig {
                api_endpoints: BTreeMap::from([(
                    PeerId::from(0),
                    PeerUrl {
                        url: "wss://example-b.com/".parse().expect("valid url"),
                        name: "peer0b".to_owned(),
                    },
                )]),
                broadcast_public_keys: None,
                consensus_version: CoreConsensusVersion::new(2, 0),
                meta: BTreeMap::new(),
            },
            modules: BTreeMap::from([(
                0,
                ClientModuleConfig::from_typed(
                    0,
                    ModuleKind::from_static_str("dummy"),
                    fedimint_core::module::ModuleConsensusVersion::new(2, 0),
                    DummyClientConfig,
                )
                .expect("valid module config"),
            )]),
        };
        let federation_id = config.global.calculate_federation_id();
        (config, federation_id)
    };
    insert_federation(&pool, &config_b, fed_b).await;
    let fed_b_bytes = fed_b.consensus_encode_to_vec();

    let conn = pool.get().await.unwrap();

    for fed in [&fed_a_bytes, &fed_b_bytes] {
        conn.execute(
            "INSERT INTO sessions (federation_id, session_index, data) VALUES ($1, 0, ''::bytea)",
            &[fed],
        )
        .await
        .unwrap();
        conn.execute(
            "INSERT INTO session_time_votes (federation_id, session_index, source_kind, peer_id, timestamp)
             VALUES ($1, 0, 'wallet', 0, '2024-01-15 12:00:00')",
            &[fed],
        )
        .await
        .unwrap();
    }
    fmo_core::db::session_times::recompute_full(&conn)
        .await
        .unwrap();

    conn.execute(
        "INSERT INTO transactions (federation_id, txid, session_index, item_index, data)
         VALUES ($1, $2, 0, 0, ''::bytea), ($3, $2, 0, 0, ''::bytea)",
        &[&fed_a_bytes, &b"shared_txid".to_vec(), &fed_b_bytes],
    )
    .await
    .unwrap();

    // Same txid bytes in both federations (allowed: PK is
    // (federation_id, txid)), but different input amounts, so a query that
    // fails to filter by federation would sum them together.
    conn.execute(
        "INSERT INTO transaction_inputs (federation_id, txid, in_index, kind, amount_msat)
         VALUES ($1, $2, 0, 'dummy', 1000), ($3, $2, 0, 'dummy', 500)",
        &[&fed_a_bytes, &b"shared_txid".to_vec(), &fed_b_bytes],
    )
    .await
    .unwrap();

    // The histogram reads the `federation_tx_daily` matview, which depends on
    // both `session_times` (refreshed above) and the transactions/inputs just
    // inserted, so it must be refreshed before querying.
    conn.batch_execute("REFRESH MATERIALIZED VIEW federation_tx_daily")
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

    let histogram_a = observer.transaction_histogram(fed_a).await.unwrap();
    assert_eq!(histogram_a.len(), 1);
    assert_eq!(histogram_a[0].count, 1);
    assert_eq!(histogram_a[0].amount, 1000);

    let histogram_b = observer.transaction_histogram(fed_b).await.unwrap();
    assert_eq!(histogram_b.len(), 1);
    assert_eq!(histogram_b[0].count, 1);
    assert_eq!(histogram_b[0].amount, 500);
}

/// `federation_summary` (used by the new single-federation summary endpoint)
/// must produce the same result as the corresponding entry from
/// `list_federation_summaries` (used by the fleet overview) -- they share
/// the extracted per-federation logic.
#[tokio::test]
async fn federation_summary_matches_list_entry() {
    let _guard = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;

    let (config, federation_id) = dummy_config();
    insert_federation(&pool, &config, federation_id).await;

    let registry = ModuleRegistry::new(vec![]);
    let observer = FederationObserver::new_without_tasks(
        &std::env::var("FMO_TEST_DATABASE").unwrap(),
        "admin",
        "http://unused.invalid",
        registry,
    )
    .await
    .unwrap();

    let single = observer.federation_summary(federation_id).await.unwrap();
    let list = observer.list_federation_summaries().await.unwrap();
    assert_eq!(list.len(), 1);

    assert_eq!(
        serde_json::to_value(&single).unwrap(),
        serde_json::to_value(&list[0]).unwrap()
    );

    // With no transactions, the all-time totals fall back to zero.
    assert_eq!(single.total_tx_count, 0);
    assert_eq!(single.total_volume, fedimint_core::Amount::ZERO);
}

/// `federation_summary.total_tx_count`/`total_volume` are summed from the
/// `federation_tx_daily` matview. Insert two transactions with known input
/// amounts and assert the summary reports the all-time count and volume.
#[tokio::test]
async fn federation_summary_reports_all_time_totals() {
    let _guard = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;

    let (config, federation_id) = dummy_config();
    insert_federation(&pool, &config, federation_id).await;
    let fed_bytes = federation_id.consensus_encode_to_vec();

    let conn = pool.get().await.unwrap();
    conn.execute(
        "INSERT INTO sessions (federation_id, session_index, data) VALUES ($1, 0, ''::bytea)",
        &[&fed_bytes],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO session_time_votes (federation_id, session_index, source_kind, peer_id, timestamp)
         VALUES ($1, 0, 'wallet', 0, '2024-01-15 12:00:00')",
        &[&fed_bytes],
    )
    .await
    .unwrap();
    fmo_core::db::session_times::recompute_full(&conn)
        .await
        .unwrap();

    // Two transactions in the same session, each with one input.
    conn.execute(
        "INSERT INTO transactions (federation_id, txid, session_index, item_index, data)
         VALUES ($1, $2, 0, 0, ''::bytea), ($1, $3, 0, 1, ''::bytea)",
        &[&fed_bytes, &b"txid_a".to_vec(), &b"txid_b".to_vec()],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO transaction_inputs (federation_id, txid, in_index, kind, amount_msat)
         VALUES ($1, $2, 0, 'dummy', 1000), ($1, $3, 0, 'dummy', 500)",
        &[&fed_bytes, &b"txid_a".to_vec(), &b"txid_b".to_vec()],
    )
    .await
    .unwrap();

    conn.batch_execute("REFRESH MATERIALIZED VIEW federation_tx_daily")
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

    let summary = observer.federation_summary(federation_id).await.unwrap();
    assert_eq!(summary.total_tx_count, 2);
    assert_eq!(
        summary.total_volume,
        fedimint_core::Amount::from_msats(1500)
    );
}

/// DDL mirroring `fmo_module_walletv2/schema/v1.sql` (the columns
/// `get_federation_assets` reads). fmo_core can't depend on the module crate,
/// so the test builds the table it queries directly; the module's own tests
/// cover the real migration.
async fn create_walletv2_utxos_table(conn: &deadpool_postgres::Object) {
    conn.batch_execute(
        "DROP SCHEMA IF EXISTS fmo_walletv2 CASCADE;
         CREATE SCHEMA fmo_walletv2;
         CREATE TABLE fmo_walletv2.wallet_utxos (
             federation_id   BYTEA   NOT NULL,
             session_index   INTEGER NOT NULL,
             item_index      INTEGER NOT NULL,
             txid            BYTEA   NOT NULL,
             utxo_value_msat BIGINT,
             address         TEXT,
             resolved_at     TIMESTAMP,
             PRIMARY KEY (federation_id, session_index, item_index)
         );",
    )
    .await
    .unwrap();
}

/// Inserts a v1 `wallet` deposit and a `walletv2` deposit whose exact
/// input/output netting is deliberately WRONG, so the test proves that the
/// walletv2 portion comes from the latest RESOLVED consolidated-UTXO value,
/// not the netting.
async fn seed_wallet_assets(
    conn: &deadpool_postgres::Object,
    fed: &[u8],
    wallet_net_msat: i64,
    walletv2_wrong_net_msat: i64,
) {
    conn.execute(
        "INSERT INTO sessions (federation_id, session_index, data) VALUES ($1, 0, ''::bytea)",
        &[&fed],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO transactions (federation_id, txid, session_index, item_index, data)
         VALUES ($1, $2, 0, 0, ''::bytea), ($1, $3, 0, 1, ''::bytea)",
        &[&fed, &b"wtx_v1".to_vec(), &b"wtx_v2".to_vec()],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO transaction_inputs (federation_id, txid, in_index, kind, amount_msat)
         VALUES ($1, $2, 0, 'wallet', $4), ($1, $3, 0, 'walletv2', $5)",
        &[
            &fed,
            &b"wtx_v1".to_vec(),
            &b"wtx_v2".to_vec(),
            &wallet_net_msat,
            &walletv2_wrong_net_msat,
        ],
    )
    .await
    .unwrap();
}

/// `get_federation_assets` uses the EXACT latest resolved walletv2 UTXO value
/// (plus the v1 wallet netting), ignoring the fee-approximate walletv2
/// input/output netting.
#[tokio::test]
async fn walletv2_assets_use_exact_resolved_utxo() {
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
    // wallet net = 10_000_000; walletv2 netting = 99_000_000 (wrong on purpose)
    seed_wallet_assets(&conn, &fed, 10_000_000, 99_000_000).await;
    create_walletv2_utxos_table(&conn).await;

    // Two resolved transitions; the latest (session 3, item 6) is the current
    // consolidated UTXO = 80_000_000 msat.
    conn.execute(
        "INSERT INTO fmo_walletv2.wallet_utxos
             (federation_id, session_index, item_index, txid, utxo_value_msat, resolved_at)
         VALUES
             ($1, 2, 0, '\\xaa', 50000000, NOW()::timestamp),
             ($1, 3, 6, '\\xbb', 80000000, NOW()::timestamp)",
        &[&fed],
    )
    .await
    .unwrap();
    // walletv2 has replayed past the tip (seed inserts only session 0), so the
    // exact value is trusted.
    conn.execute(
        "INSERT INTO module_progress (module_kind, federation_id, next_session_index)
         VALUES ('walletv2', $1, 100)",
        &[&fed],
    )
    .await
    .unwrap();
    drop(conn);

    let observer = FederationObserver::new_without_tasks(
        &std::env::var("FMO_TEST_DATABASE").unwrap(),
        "admin",
        "http://unused.invalid",
        ModuleRegistry::new(vec![]),
    )
    .await
    .unwrap();

    let assets = observer.get_federation_assets(federation_id).await.unwrap();
    // wallet net (10M) + exact latest walletv2 UTXO (80M) = 90M; NOT the
    // walletv2 netting path (10M + 99M = 109M).
    assert_eq!(assets, fedimint_core::Amount::from_msats(90_000_000));
}

/// During replay (walletv2 NOT yet caught up to the federation's tip), the
/// "latest resolved" row is a stale intermediate UTXO from partway through the
/// backfill. Assets must fall back to netting until walletv2 has replayed to
/// the tip, NOT show that stale historical value.
#[tokio::test]
async fn walletv2_assets_use_netting_during_replay() {
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
    seed_wallet_assets(&conn, &fed, 10_000_000, 99_000_000).await;
    // Federation has sessions up to index 10 (tip), but walletv2 has only
    // replayed to session 3 -> NOT caught up.
    for s in 1..=10i32 {
        conn.execute(
            "INSERT INTO sessions (federation_id, session_index, data) VALUES ($1, $2, ''::bytea)",
            &[&fed, &s],
        )
        .await
        .unwrap();
    }
    conn.execute(
        "INSERT INTO module_progress (module_kind, federation_id, next_session_index)
         VALUES ('walletv2', $1, 3)",
        &[&fed],
    )
    .await
    .unwrap();
    create_walletv2_utxos_table(&conn).await;
    // A resolved (but stale, mid-replay) intermediate UTXO.
    conn.execute(
        "INSERT INTO fmo_walletv2.wallet_utxos
             (federation_id, session_index, item_index, txid, utxo_value_msat, resolved_at)
         VALUES ($1, 2, 0, '\\xaa', 5000000, NOW()::timestamp)",
        &[&fed],
    )
    .await
    .unwrap();
    drop(conn);

    let observer = FederationObserver::new_without_tasks(
        &std::env::var("FMO_TEST_DATABASE").unwrap(),
        "admin",
        "http://unused.invalid",
        ModuleRegistry::new(vec![]),
    )
    .await
    .unwrap();

    let assets = observer.get_federation_assets(federation_id).await.unwrap();
    // NOT caught up -> netting (10M + 99M = 109M), not the stale 5M UTXO.
    assert_eq!(assets, fedimint_core::Amount::from_msats(109_000_000));
}

/// With no resolved walletv2 UTXO yet (backfill not done), assets fall back to
/// the input/output netting so the value is never worse than before.
#[tokio::test]
async fn walletv2_assets_fall_back_to_netting_when_unresolved() {
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
    seed_wallet_assets(&conn, &fed, 10_000_000, 99_000_000).await;
    create_walletv2_utxos_table(&conn).await;
    // Only UNRESOLVED rows (NULL value) -> no exact value available even though
    // walletv2 is caught up (tests the backfill-lag path, not the replay path).
    conn.execute(
        "INSERT INTO fmo_walletv2.wallet_utxos
             (federation_id, session_index, item_index, txid)
         VALUES ($1, 3, 6, '\\xbb')",
        &[&fed],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO module_progress (module_kind, federation_id, next_session_index)
         VALUES ('walletv2', $1, 100)",
        &[&fed],
    )
    .await
    .unwrap();
    drop(conn);

    let observer = FederationObserver::new_without_tasks(
        &std::env::var("FMO_TEST_DATABASE").unwrap(),
        "admin",
        "http://unused.invalid",
        ModuleRegistry::new(vec![]),
    )
    .await
    .unwrap();

    let assets = observer.get_federation_assets(federation_id).await.unwrap();
    // Falls back to netting: wallet (10M) + walletv2 net (99M) = 109M.
    assert_eq!(assets, fedimint_core::Amount::from_msats(109_000_000));
}

/// The "latest" walletv2 UTXO must be ranked by each txid's FIRST appearance,
/// not any appearance. The server re-emits a `Signatures` CI for every
/// still-unfinalized tx each session, so an older pending tx can be
/// re-announced at a LATER (session, item) than a genuinely newer tx — a naive
/// `ORDER BY session DESC, item DESC` would then return the older tx's stale
/// value. Assert the newer tx (by first appearance) wins.
#[tokio::test]
async fn walletv2_assets_rank_latest_utxo_by_first_appearance() {
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
    // wallet net = 10_000_000; walletv2 netting = 99_000_000 (wrong on purpose)
    seed_wallet_assets(&conn, &fed, 10_000_000, 99_000_000).await;
    create_walletv2_utxos_table(&conn).await;

    // OLD txid `\xaa` first appears at session 2 (value 50M) and is then
    // RE-ANNOUNCED at session 4 (still unfinalized). NEW txid `\xbb` first
    // appears in between at session 3 (value 80M). Naive latest-by-(session,
    // item) would wrongly pick the session-4 re-announcement of the OLD txid.
    conn.execute(
        "INSERT INTO fmo_walletv2.wallet_utxos
             (federation_id, session_index, item_index, txid, utxo_value_msat, resolved_at)
         VALUES
             ($1, 2, 0, '\\xaa', 50000000, NOW()::timestamp),
             ($1, 3, 0, '\\xbb', 80000000, NOW()::timestamp),
             ($1, 4, 0, '\\xaa', 50000000, NOW()::timestamp)",
        &[&fed],
    )
    .await
    .unwrap();
    conn.execute(
        "INSERT INTO module_progress (module_kind, federation_id, next_session_index)
         VALUES ('walletv2', $1, 100)",
        &[&fed],
    )
    .await
    .unwrap();
    drop(conn);

    let observer = FederationObserver::new_without_tasks(
        &std::env::var("FMO_TEST_DATABASE").unwrap(),
        "admin",
        "http://unused.invalid",
        ModuleRegistry::new(vec![]),
    )
    .await
    .unwrap();

    let assets = observer.get_federation_assets(federation_id).await.unwrap();
    // wallet net (10M) + newer-by-first-appearance walletv2 UTXO (80M) = 90M;
    // NOT the re-announced OLD txid (would give 10M + 50M = 60M).
    assert_eq!(assets, fedimint_core::Amount::from_msats(90_000_000));
}

/// Builds a `ClientConfig` with `n` guardians (peers `0..n`, each named
/// `peer<i>`) and a single dummy module, deriving its federation id.
fn multi_guardian_config(n: u16) -> (fedimint_core::config::ClientConfig, FederationId) {
    use std::collections::BTreeMap;

    use fedimint_core::config::{ClientConfig, ClientModuleConfig, GlobalClientConfig, PeerUrl};
    use fedimint_core::core::ModuleKind;
    use fedimint_core::module::{CoreConsensusVersion, ModuleConsensusVersion};
    use fedimint_core::PeerId;
    use fedimint_dummy_common::config::DummyClientConfig;

    let api_endpoints = (0..n)
        .map(|i| {
            (
                PeerId::from(i),
                PeerUrl {
                    url: format!("wss://guardian-{i}.example.com/")
                        .parse()
                        .expect("valid url"),
                    name: format!("peer{i}"),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();

    let config = ClientConfig {
        global: GlobalClientConfig {
            api_endpoints,
            broadcast_public_keys: None,
            consensus_version: CoreConsensusVersion::new(2, 0),
            meta: BTreeMap::new(),
        },
        modules: BTreeMap::from([(
            0,
            ClientModuleConfig::from_typed(
                0,
                ModuleKind::from_static_str("dummy"),
                ModuleConsensusVersion::new(2, 0),
                DummyClientConfig,
            )
            .expect("valid module config"),
        )]),
    };
    let federation_id = config.global.calculate_federation_id();
    (config, federation_id)
}

/// Inserts a `guardian_health` sample. `online = true` writes a non-NULL
/// `status` (guardian responded); `online = false` writes NULL (offline).
async fn insert_health_sample(
    conn: &deadpool_postgres::Object,
    fed: &[u8],
    time: chrono::NaiveDateTime,
    guardian_id: i32,
    online: bool,
) {
    let status: Option<serde_json::Value> = online.then(|| serde_json::json!({}));
    conn.execute(
        "INSERT INTO guardian_health (federation_id, time, guardian_id, status, block_height, latency_ms)
         VALUES ($1, $2, $3, $4, NULL, 10)",
        &[&fed, &time, &guardian_id, &status],
    )
    .await
    .unwrap();
}

/// `get_guardian_timeline` derives, from mixed NULL/non-NULL `guardian_health`
/// samples: (a) per-guardian maximal offline runs, and (b) federation-wide
/// inoperable runs where the online guardian count drops below the consensus
/// threshold. A 4-guardian federation has threshold 3, so two simultaneously
/// offline guardians make it inoperable.
#[tokio::test]
async fn guardian_timeline_computes_offline_and_inoperable_intervals() {
    let _guard = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;

    let (config, federation_id) = multi_guardian_config(4);
    insert_federation(&pool, &config, federation_id).await;
    let fed = federation_id.consensus_encode_to_vec();

    // Five polls, 60s apart, ending 6 minutes ago so every row is comfortably
    // inside the query window and before `window_end` (= now).
    let base = (chrono::Utc::now() - chrono::Duration::minutes(10)).naive_utc();
    let t: Vec<chrono::NaiveDateTime> = (0..5)
        .map(|i| base + chrono::Duration::minutes(i))
        .collect();

    let conn = pool.get().await.unwrap();
    for (i, &time) in t.iter().enumerate() {
        // Guardians 0 and 1: always online.
        insert_health_sample(&conn, &fed, time, 0, true).await;
        insert_health_sample(&conn, &fed, time, 1, true).await;
        // Guardians 2 and 3: offline at t2 and t3, online otherwise.
        let offline = i == 2 || i == 3;
        insert_health_sample(&conn, &fed, time, 2, !offline).await;
        insert_health_sample(&conn, &fed, time, 3, !offline).await;
    }
    drop(conn);

    let observer = FederationObserver::new_without_tasks(
        &std::env::var("FMO_TEST_DATABASE").unwrap(),
        "admin",
        "http://unused.invalid",
        ModuleRegistry::new(vec![]),
    )
    .await
    .unwrap();

    let timeline = observer
        .get_guardian_timeline(federation_id, chrono::Duration::days(1))
        .await
        .unwrap();

    assert_eq!(timeline.num_guardians, 4);
    assert_eq!(
        timeline.threshold,
        fedimint_core::NumPeers::from(4usize).threshold()
    );
    assert_eq!(timeline.threshold, 3);
    assert_eq!(timeline.guardians.len(), 4);

    let epoch = |ndt: chrono::NaiveDateTime| ndt.and_utc().timestamp();

    // Guardians 0 and 1 were never offline.
    assert!(timeline.guardians[0].offline_intervals.is_empty());
    assert!(timeline.guardians[1].offline_intervals.is_empty());
    assert_eq!(timeline.guardians[0].name, "peer0");

    // Guardians 2 and 3 were offline across [t2, t4): from the first NULL
    // sample (t2) until the next sample that shows them back online (t4).
    for gid in [2usize, 3usize] {
        assert_eq!(
            timeline.guardians[gid].offline_intervals.len(),
            1,
            "guardian {gid} should have one offline interval"
        );
        let interval = timeline.guardians[gid].offline_intervals[0];
        assert_eq!(interval.start, epoch(t[2]));
        assert_eq!(interval.end, epoch(t[4]));
    }

    // Two of four guardians offline at t2 and t3 => online count 2 < threshold
    // 3 => inoperable across [t2, t4).
    assert_eq!(timeline.inoperable_intervals.len(), 1);
    assert_eq!(timeline.inoperable_intervals[0].start, epoch(t[2]));
    assert_eq!(timeline.inoperable_intervals[0].end, epoch(t[4]));
}

/// An outage still ongoing at the end of the window (the guardian's latest
/// sample is NULL) extends to `window_end` (= now), for both the per-guardian
/// lane and the federation-wide inoperable run.
#[tokio::test]
async fn guardian_timeline_ongoing_outage_extends_to_window_end() {
    let _guard = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;

    // n=3 => threshold 3, so a single offline guardian makes the federation
    // inoperable. All three guardians report every poll (no absent guardians,
    // which would otherwise count as offline in the inoperable tally).
    let (config, federation_id) = multi_guardian_config(3);
    insert_federation(&pool, &config, federation_id).await;
    let fed = federation_id.consensus_encode_to_vec();

    let base = (chrono::Utc::now() - chrono::Duration::minutes(10)).naive_utc();
    let t: Vec<chrono::NaiveDateTime> = (0..4)
        .map(|i| base + chrono::Duration::minutes(i))
        .collect();

    let conn = pool.get().await.unwrap();
    for (i, &time) in t.iter().enumerate() {
        // Guardian 0: online for two polls, then offline and never recovers.
        insert_health_sample(&conn, &fed, time, 0, i < 2).await;
        // Guardians 1 and 2: online throughout.
        insert_health_sample(&conn, &fed, time, 1, true).await;
        insert_health_sample(&conn, &fed, time, 2, true).await;
    }
    drop(conn);

    let observer = FederationObserver::new_without_tasks(
        &std::env::var("FMO_TEST_DATABASE").unwrap(),
        "admin",
        "http://unused.invalid",
        ModuleRegistry::new(vec![]),
    )
    .await
    .unwrap();

    let before_end = chrono::Utc::now().timestamp();
    let timeline = observer
        .get_guardian_timeline(federation_id, chrono::Duration::days(1))
        .await
        .unwrap();
    let after_end = chrono::Utc::now().timestamp();

    assert_eq!(timeline.threshold, 3);

    // Guardian 0: one ongoing outage starting at t2, ending at window_end (now).
    assert_eq!(timeline.guardians[0].offline_intervals.len(), 1);
    let ongoing = timeline.guardians[0].offline_intervals[0];
    assert_eq!(ongoing.start, t[2].and_utc().timestamp());
    assert!(ongoing.end >= before_end && ongoing.end <= after_end);
    assert_eq!(ongoing.end, timeline.window_end);

    // Guardians 1 and 2 never went offline.
    assert!(timeline.guardians[1].offline_intervals.is_empty());
    assert!(timeline.guardians[2].offline_intervals.is_empty());

    // One guardian offline (of three) => online 2 < threshold 3 => inoperable
    // from t2, ongoing to window_end.
    assert_eq!(timeline.inoperable_intervals.len(), 1);
    assert_eq!(
        timeline.inoperable_intervals[0].start,
        t[2].and_utc().timestamp()
    );
    assert_eq!(timeline.inoperable_intervals[0].end, timeline.window_end);
}

/// A guardian that never produced any `guardian_health` sample in the window
/// gets an empty lane (no fabricated full-window outage), and every configured
/// guardian still appears as a lane.
#[tokio::test]
async fn guardian_timeline_guardian_without_samples_has_empty_lane() {
    let _guard = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;

    let (config, federation_id) = multi_guardian_config(2);
    insert_federation(&pool, &config, federation_id).await;
    let fed = federation_id.consensus_encode_to_vec();

    let base = (chrono::Utc::now() - chrono::Duration::minutes(10)).naive_utc();
    let conn = pool.get().await.unwrap();
    // Only guardian 0 reports (always online); guardian 1 never appears.
    for i in 0..4 {
        insert_health_sample(&conn, &fed, base + chrono::Duration::minutes(i), 0, true).await;
    }
    drop(conn);

    let observer = FederationObserver::new_without_tasks(
        &std::env::var("FMO_TEST_DATABASE").unwrap(),
        "admin",
        "http://unused.invalid",
        ModuleRegistry::new(vec![]),
    )
    .await
    .unwrap();

    let timeline = observer
        .get_guardian_timeline(federation_id, chrono::Duration::days(1))
        .await
        .unwrap();

    // Both guardians present as lanes; neither has an offline interval (the
    // reporting one was online, the silent one has no data to infer from).
    assert_eq!(timeline.guardians.len(), 2);
    assert_eq!(timeline.guardians[1].guardian_id, 1);
    assert!(timeline.guardians[0].offline_intervals.is_empty());
    assert!(timeline.guardians[1].offline_intervals.is_empty());
}

/// An isolated single-poll failure (one missed poll bracketed by online polls
/// on both sides) is a transient timeout, not a real outage, and is despiked:
/// it produces neither a per-guardian offline interval nor an inoperable
/// window, even when enough guardians blip in the same poll to momentarily drop
/// the online count below threshold. A run of >=2 consecutive misses is a real
/// outage and is still reported.
#[tokio::test]
async fn guardian_timeline_despikes_single_poll_blips() {
    let _guard = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;

    // n=4 => threshold 3. Two guardians blipping in one poll would drop online
    // to 2 (< 3) at that instant, but despiking must prevent an inoperable run.
    let (config, federation_id) = multi_guardian_config(4);
    insert_federation(&pool, &config, federation_id).await;
    let fed = federation_id.consensus_encode_to_vec();

    let base = (chrono::Utc::now() - chrono::Duration::minutes(10)).naive_utc();
    let t: Vec<chrono::NaiveDateTime> = (0..5)
        .map(|i| base + chrono::Duration::minutes(i))
        .collect();

    let conn = pool.get().await.unwrap();
    for (i, &time) in t.iter().enumerate() {
        // Guardians 0 and 1: always online.
        insert_health_sample(&conn, &fed, time, 0, true).await;
        insert_health_sample(&conn, &fed, time, 1, true).await;
        // Guardian 2: single isolated blip at t2 (online before and after).
        insert_health_sample(&conn, &fed, time, 2, i != 2).await;
        // Guardian 3: two-poll outage at t2 AND t3 (a real outage), recovers t4.
        insert_health_sample(&conn, &fed, time, 3, !(i == 2 || i == 3)).await;
    }
    drop(conn);

    let observer = FederationObserver::new_without_tasks(
        &std::env::var("FMO_TEST_DATABASE").unwrap(),
        "admin",
        "http://unused.invalid",
        ModuleRegistry::new(vec![]),
    )
    .await
    .unwrap();

    let timeline = observer
        .get_guardian_timeline(federation_id, chrono::Duration::days(1))
        .await
        .unwrap();
    let epoch = |ndt: chrono::NaiveDateTime| ndt.and_utc().timestamp();

    // Guardian 2's single-poll blip is despiked away entirely.
    assert!(
        timeline.guardians[2].offline_intervals.is_empty(),
        "single-poll blip should be despiked"
    );
    // Guardian 3's two-poll outage survives, spanning [t2, t4).
    assert_eq!(timeline.guardians[3].offline_intervals.len(), 1);
    assert_eq!(
        timeline.guardians[3].offline_intervals[0].start,
        epoch(t[2])
    );
    assert_eq!(timeline.guardians[3].offline_intervals[0].end, epoch(t[4]));

    // At t2 the raw online count is 2 (< threshold 3), but guardian 2's blip is
    // despiked to online, so the despiked online count is 3 == threshold and the
    // federation is never inoperable.
    assert!(
        timeline.inoperable_intervals.is_empty(),
        "a despiked blip must not fabricate an inoperable window"
    );
}
