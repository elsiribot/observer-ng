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

const FED: [u8; 32] = [7u8; 32];

async fn insert_tx(
    conn: &deadpool_postgres::Object,
    txid: &[u8],
    session_index: i32,
    inputs: &[(&str, Option<i64>)],
    outputs: &[(&str, Option<i64>)],
) {
    conn.execute(
        "INSERT INTO transactions VALUES ($1, $2, $3, 0, $4)",
        &[&&FED[..], &txid, &session_index, &&b"raw"[..]],
    )
    .await
    .unwrap();
    for (index, (kind, amount)) in inputs.iter().enumerate() {
        conn.execute(
            "INSERT INTO transaction_inputs (federation_id, txid, in_index, kind, amount_msat)
             VALUES ($1, $2, $3, $4, $5)",
            &[&&FED[..], &txid, &(index as i32), kind, amount],
        )
        .await
        .unwrap();
    }
    for (index, (kind, amount)) in outputs.iter().enumerate() {
        conn.execute(
            "INSERT INTO transaction_outputs (federation_id, txid, out_index, kind, amount_msat)
             VALUES ($1, $2, $3, $4, $5)",
            &[&&FED[..], &txid, &(index as i32), kind, amount],
        )
        .await
        .unwrap();
    }
}

async fn input_amount(conn: &deadpool_postgres::Object, txid: &[u8]) -> Option<i64> {
    conn.query_one(
        "SELECT amount_msat FROM transaction_inputs
         WHERE federation_id = $1 AND txid = $2 AND in_index = 0",
        &[&&FED[..], &txid],
    )
    .await
    .unwrap()
    .get(0)
}

#[tokio::test]
async fn balance_inference_fills_single_unknown_items() {
    let _guard = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    let conn = pool.get().await.unwrap();
    conn.batch_execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        .await
        .unwrap();
    fmo_core::db::migrations::setup_core_schema(&pool)
        .await
        .unwrap();

    conn.execute(
        "INSERT INTO federations VALUES ($1, $2)",
        &[&&FED[..], &&b"cfg"[..]],
    )
    .await
    .unwrap();
    // One installed module fully processed sessions < 10.
    conn.execute(
        "INSERT INTO module_progress VALUES ('dummy', $1, 10)",
        &[&&FED[..]],
    )
    .await
    .unwrap();
    for session_index in [1i32, 20] {
        conn.execute(
            "INSERT INTO sessions VALUES ($1, $2, ''::bytea)",
            &[&&FED[..], &session_index],
        )
        .await
        .unwrap();
    }

    // Claim-style tx: single unknown input, known outputs -> inferred.
    insert_tx(
        &conn,
        b"tx_claim",
        1,
        &[("walletv2", None)],
        &[("mintv2", Some(1000)), ("mintv2", Some(500))],
    )
    .await;
    // Two unknown inputs -> untouched.
    insert_tx(
        &conn,
        b"tx_two_unknowns",
        1,
        &[("walletv2", None), ("stability_pool", None)],
        &[("mintv2", Some(700))],
    )
    .await;
    // Session not yet processed by every module -> untouched.
    insert_tx(
        &conn,
        b"tx_unprocessed",
        20,
        &[("walletv2", None)],
        &[("mintv2", Some(700))],
    )
    .await;
    // Single unknown output -> inferred from the input side.
    insert_tx(
        &conn,
        b"tx_out",
        1,
        &[("mintv2", Some(2000))],
        &[("walletv2", None), ("mintv2", Some(300))],
    )
    .await;
    // Balance would be negative -> untouched (never invent nonsense).
    insert_tx(
        &conn,
        b"tx_negative",
        1,
        &[("walletv2", None), ("mintv2", Some(500))],
        &[("mintv2", Some(100))],
    )
    .await;

    let (inputs, outputs) = fmo_core::amounts::infer_missing_amounts(&conn)
        .await
        .unwrap();
    assert_eq!((inputs, outputs), (1, 1));

    assert_eq!(input_amount(&conn, b"tx_claim").await, Some(1500));
    assert_eq!(input_amount(&conn, b"tx_two_unknowns").await, None);
    assert_eq!(input_amount(&conn, b"tx_unprocessed").await, None);
    assert_eq!(input_amount(&conn, b"tx_negative").await, None);

    let (out_amount, out_details): (Option<i64>, Option<serde_json::Value>) = {
        let row = conn
            .query_one(
                "SELECT amount_msat, details FROM transaction_outputs
                 WHERE federation_id = $1 AND txid = $2 AND out_index = 0",
                &[&&FED[..], &&b"tx_out"[..]],
            )
            .await
            .unwrap();
        (row.get(0), row.get(1))
    };
    assert_eq!(out_amount, Some(1700));
    assert_eq!(out_details.unwrap()["inferred"], serde_json::json!(true));

    // Idempotent: a second run finds nothing new and changes nothing.
    let (inputs, outputs) = fmo_core::amounts::infer_missing_amounts(&conn)
        .await
        .unwrap();
    assert_eq!((inputs, outputs), (0, 0));
    assert_eq!(input_amount(&conn, b"tx_claim").await, Some(1500));
}
