mod common;

use common::{dummy_config, dummy_session, test_pool, DB_LOCK};
use fedimint_core::encoding::Encodable;

/// Import from a v8-schema database: raw sessions round-trip through decode +
/// structural ingest, block times are copied, session counts are verified.
#[tokio::test]
async fn import_from_old_schema_db() {
    let _guard = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    let new_url = std::env::var("FMO_TEST_DATABASE").unwrap();
    let Some(old_url) = build_old_db_url(&new_url) else {
        eprintln!("skipping: cannot derive old-DB URL from FMO_TEST_DATABASE");
        return;
    };

    // fresh "old" database with the minimal v8 tables (DDL copied from the
    // legacy fmo_server/schema/v0.sql)
    {
        let conn = pool.get().await.unwrap();
        let _ = conn
            .execute("DROP DATABASE IF EXISTS fmo_test_old", &[])
            .await;
        conn.execute("CREATE DATABASE fmo_test_old", &[])
            .await
            .unwrap();
    }
    let (old, old_conn) = tokio_postgres::connect(&old_url, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move { old_conn.await.unwrap() });
    old.batch_execute(
        "CREATE TABLE federations (
             federation_id BYTEA PRIMARY KEY NOT NULL,
             config        BYTEA             NOT NULL
         );
         CREATE TABLE sessions (
             federation_id BYTEA   NOT NULL REFERENCES federations (federation_id),
             session_index INTEGER NOT NULL,
             session       BYTEA   NOT NULL,
             PRIMARY KEY (federation_id, session_index)
         );
         CREATE TABLE block_times (
             block_height INTEGER PRIMARY KEY,
             timestamp    TIMESTAMP NOT NULL
         );",
    )
    .await
    .unwrap();

    // seed old DB: one federation, 3 sessions, 2 block times
    let (config, federation_id) = dummy_config();
    let fed = federation_id.consensus_encode_to_vec();
    old.execute(
        "INSERT INTO federations VALUES ($1, $2)",
        &[&fed, &config.consensus_encode_to_vec()],
    )
    .await
    .unwrap();
    for session_index in 0..3i32 {
        let session = dummy_session(2_000 + session_index as u64);
        old.execute(
            "INSERT INTO sessions VALUES ($1, $2, $3)",
            &[&fed, &session_index, &session.consensus_encode_to_vec()],
        )
        .await
        .unwrap();
    }
    old.execute(
        "INSERT INTO block_times VALUES (820001, NOW()::timestamp), (820002, NOW()::timestamp)",
        &[],
    )
    .await
    .unwrap();

    // reset the new DB and import
    {
        let conn = pool.get().await.unwrap();
        conn.batch_execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
            .await
            .unwrap();
    }
    let registry = fmo_core::registry::ModuleRegistry::new(vec![]);
    fmo_core::import::import(&old_url, &new_url, &registry)
        .await
        .unwrap();

    let conn = pool.get().await.unwrap();
    let sessions: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM sessions WHERE federation_id = $1",
            &[&fed],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(sessions, 3);
    // structural ingest ran during import
    let txs: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM transactions WHERE federation_id = $1",
            &[&fed],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(txs, 3);
    let inputs: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM transaction_inputs WHERE federation_id = $1 AND kind = 'dummy'",
            &[&fed],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(inputs, 3);
    // block times copied
    let block_times: i64 = conn
        .query_one("SELECT COUNT(*) FROM block_times", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(block_times, 2);
    // no module progress: modules replay from 0 on next serve
    let cursors: i64 = conn
        .query_one("SELECT COUNT(*) FROM module_progress", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(cursors, 0);
}

fn build_old_db_url(new_url: &str) -> Option<String> {
    // e.g. postgres://user@/fmo_test?host=...&port=... -> .../fmo_test_old?...
    let replaced = new_url.replacen("/fmo_test?", "/fmo_test_old?", 1);
    (replaced != new_url).then_some(replaced)
}
