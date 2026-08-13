mod common;

use common::{dummy_config, insert_federation, reset_db, test_pool, DB_LOCK};
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
    // Only UNRESOLVED rows (NULL value) -> no exact value available.
    conn.execute(
        "INSERT INTO fmo_walletv2.wallet_utxos
             (federation_id, session_index, item_index, txid)
         VALUES ($1, 3, 6, '\\xbb')",
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
