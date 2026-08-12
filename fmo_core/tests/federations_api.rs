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
    conn.batch_execute("REFRESH MATERIALIZED VIEW session_times")
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
}
