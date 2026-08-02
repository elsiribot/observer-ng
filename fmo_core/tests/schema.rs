use deadpool_postgres::{Config, Runtime};
use tokio_postgres::NoTls;

/// Tests share one database; serialize them.
pub static DB_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub fn test_pool() -> Option<deadpool_postgres::Pool> {
    let url = std::env::var("FMO_TEST_DATABASE").ok()?;
    let cfg = Config {
        url: Some(url),
        ..Default::default()
    };
    Some(cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap())
}

#[tokio::test]
async fn core_schema_applies_and_is_idempotent() {
    let _guard = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    let conn = pool.get().await.unwrap();
    conn.batch_execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        .await
        .unwrap();
    fmo_core::db::migrations::setup_core_schema(&pool).await.unwrap();
    // idempotent
    fmo_core::db::migrations::setup_core_schema(&pool).await.unwrap();
    let v: i32 = conn
        .query_one("SELECT MAX(version) FROM core_schema_version", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(v, 0);
}

#[tokio::test]
async fn module_schema_version_bump_drops_and_recreates() {
    let _guard = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    let conn = pool.get().await.unwrap();
    conn.batch_execute("DROP SCHEMA IF EXISTS fmo_testmod CASCADE;")
        .await
        .unwrap();
    fmo_core::db::migrations::setup_core_schema(&pool).await.unwrap();
    let migs = [fmo_core::db::migrations::Migration {
        sql: "CREATE TABLE things (id INTEGER PRIMARY KEY);",
    }];
    fmo_core::db::migrations::setup_module_schema(&pool, "testmod", 1, &migs)
        .await
        .unwrap();
    conn.execute("INSERT INTO fmo_testmod.things VALUES (1)", &[])
        .await
        .unwrap();
    // re-running with the same version keeps data
    fmo_core::db::migrations::setup_module_schema(&pool, "testmod", 1, &migs)
        .await
        .unwrap();
    let n: i64 = conn
        .query_one("SELECT COUNT(*) FROM fmo_testmod.things", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(n, 1);
    // version bump wipes the schema
    fmo_core::db::migrations::setup_module_schema(&pool, "testmod", 2, &migs)
        .await
        .unwrap();
    let n: i64 = conn
        .query_one("SELECT COUNT(*) FROM fmo_testmod.things", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(n, 0);
}
