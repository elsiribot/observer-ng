mod common;

use fedimint_core::encoding::Encodable;

use common::{dummy_config, dummy_session, insert_federation, reset_db, test_pool, DB_LOCK};

#[tokio::test]
async fn ingest_fills_structural_tables() {
    let _guard = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;

    let (config, federation_id) = dummy_config();
    insert_federation(&pool, &config, federation_id).await;

    let session = dummy_session(1_000);
    let mut conn = pool.get().await.unwrap();
    let dbtx = conn.transaction().await.unwrap();
    fmo_core::ingest::ingest_session(&dbtx, &config, federation_id, 0, &session)
        .await
        .unwrap();
    // idempotent within the same session
    fmo_core::ingest::ingest_session(&dbtx, &config, federation_id, 0, &session)
        .await
        .unwrap();
    dbtx.commit().await.unwrap();

    let conn = pool.get().await.unwrap();
    let fed = federation_id.consensus_encode_to_vec();
    let sessions: i64 = conn
        .query_one("SELECT COUNT(*) FROM sessions WHERE federation_id = $1", &[&fed])
        .await
        .unwrap()
        .get(0);
    assert_eq!(sessions, 1);
    let txs: i64 = conn
        .query_one("SELECT COUNT(*) FROM transactions WHERE federation_id = $1", &[&fed])
        .await
        .unwrap()
        .get(0);
    assert_eq!(txs, 1);
    for table in ["transaction_inputs", "transaction_outputs"] {
        let row = conn
            .query_one(
                &format!(
                    "SELECT COUNT(*)::bigint AS n,
                            COUNT(amount_msat)::bigint AS amounts,
                            MIN(kind) AS kind
                     FROM {table} WHERE federation_id = $1"
                ),
                &[&fed],
            )
            .await
            .unwrap();
        assert_eq!(row.get::<_, i64>("n"), 1, "{table} row count");
        assert_eq!(row.get::<_, i64>("amounts"), 0, "{table} amounts stay NULL");
        assert_eq!(row.get::<_, String>("kind"), "dummy");
    }
    let cis = conn
        .query_one(
            "SELECT COUNT(*)::bigint AS n, MIN(kind) AS kind FROM consensus_items WHERE federation_id = $1",
            &[&fed],
        )
        .await
        .unwrap();
    assert_eq!(cis.get::<_, i64>("n"), 1);
    assert_eq!(cis.get::<_, String>("kind"), "dummy");
}
