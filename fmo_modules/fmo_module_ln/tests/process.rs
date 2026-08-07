use std::str::FromStr;

use fedimint_core::core::{IntoDynInstance, ModuleKind};
use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::{Amount, PeerId, TransactionId};
use fedimint_ln_common::contracts::{ContractId, PreimageDecryptionShare};
use fedimint_ln_common::{LightningConsensusItem, LightningInput, LightningInputV0};
use fmo_core::module::{CiMeta, ItemMeta, ObserverModule, ProcessCtx};
use fmo_core::test_util::{insert_federation, minimal_config, reset_db, test_pool, test_services};
use fmo_module_ln::LnObserver;

/// These tests share one database; serialize them (the single-test module
/// crates don't need this, but this file has two).
static DB_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// `reset_db` only resets `public`; drop the module schema too so each test
/// applies the current `fmo_ln` schema from scratch (guards against a stale
/// leftover version in a shared dev database).
async fn reset_ln(pool: &deadpool_postgres::Pool) {
    reset_db(pool).await;
    pool.get()
        .await
        .unwrap()
        .batch_execute("DROP SCHEMA IF EXISTS fmo_ln CASCADE")
        .await
        .unwrap();
}

#[tokio::test]
async fn ln_input_records_contract_and_amount() {
    let _guard = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_ln(&pool).await;
    let (config, federation_id) = minimal_config();
    insert_federation(&pool, &config, federation_id).await;
    let services = test_services(&pool);

    let module = LnObserver;
    assert_eq!(module.kind(), ModuleKind::from_static_str("ln"));
    fmo_core::db::migrations::setup_module_schema(
        &pool,
        "ln",
        module.version(),
        module.migrations(),
    )
    .await
    .unwrap();

    let contract_id =
        ContractId::from_str("1111111111111111111111111111111111111111111111111111111111111111")
            .unwrap();
    let input = LightningInput::V0(LightningInputV0 {
        contract_id,
        amount: Amount::from_msats(1234),
        witness: None,
    })
    .into_dyn(0);

    let txid = TransactionId::consensus_decode_whole(&[7; 32], &Default::default()).unwrap();
    let fed = federation_id.consensus_encode_to_vec();

    // FK targets: transactions + transaction_inputs rows must exist
    {
        let conn = pool.get().await.unwrap();
        conn.execute("INSERT INTO sessions VALUES ($1, 0, ''::bytea)", &[&fed])
            .await
            .unwrap();
        conn.execute(
            "INSERT INTO transactions VALUES ($1, $2, 0, 0, ''::bytea)",
            &[&fed, &txid.consensus_encode_to_vec()],
        )
        .await
        .unwrap();
        conn.execute(
            "INSERT INTO transaction_inputs (federation_id, txid, in_index, kind) VALUES ($1, $2, 0, 'ln')",
            &[&fed, &txid.consensus_encode_to_vec()],
        )
        .await
        .unwrap();
    }

    let mut conn = pool.get().await.unwrap();
    let dbtx = conn.transaction().await.unwrap();
    dbtx.batch_execute("SET LOCAL search_path TO fmo_ln, public")
        .await
        .unwrap();
    let mut ctx = ProcessCtx {
        dbtx: &dbtx,
        federation_id,
        config,
        services,
    };
    let meta = ItemMeta {
        federation_id,
        txid,
        session_index: 0,
        item_index: 0,
        index: 0,
        peer_count: 1,
    };

    let processed = module.process_input(&mut ctx, &input, &meta).await.unwrap();
    assert_eq!(processed.amount, Some(Amount::from_msats(1234)));
    assert!(processed.details.is_some());
    dbtx.commit().await.unwrap();

    let conn = pool.get().await.unwrap();
    let contracts: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM fmo_ln.input_contracts WHERE federation_id = $1 AND contract_id = $2",
            &[&fed, &contract_id.consensus_encode_to_vec()],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(contracts, 1);
}

#[tokio::test]
async fn ln_records_decryption_shares_and_matview_flags_decrypted() {
    let _guard = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_ln(&pool).await;
    let (config, federation_id) = minimal_config();
    insert_federation(&pool, &config, federation_id).await;
    let services = test_services(&pool);

    let module = LnObserver;
    fmo_core::db::migrations::setup_module_schema(
        &pool,
        "ln",
        module.version(),
        module.migrations(),
    )
    .await
    .unwrap();

    let fed = federation_id.consensus_encode_to_vec();
    let contract_id = ContractId::consensus_decode_whole(&[7u8; 32], &Default::default()).unwrap();

    // A real 48-byte G1 decryption share captured from consensus; validity is
    // irrelevant to the observer, which only records that the share was cast.
    const SHARE: [u8; 48] = [
        141, 16, 160, 95, 153, 206, 24, 103, 214, 196, 171, 204, 118, 234, 120, 82, 93, 7, 153, 63,
        28, 184, 36, 212, 47, 145, 28, 39, 156, 86, 54, 17, 34, 94, 250, 168, 137, 82, 37, 55, 69,
        1, 223, 90, 53, 58, 223, 169,
    ];
    let share = threshold_crypto::DecryptionShare::from_bytes(&SHARE).expect("valid share");
    let ci = LightningConsensusItem::DecryptPreimage(contract_id, PreimageDecryptionShare(share))
        .into_dyn(0);

    let mut conn = pool.get().await.unwrap();
    let dbtx = conn.transaction().await.unwrap();
    dbtx.batch_execute("SET LOCAL search_path TO fmo_ln, public")
        .await
        .unwrap();
    let mut ctx = ProcessCtx {
        dbtx: &dbtx,
        federation_id,
        config: config.clone(),
        services: services.clone(),
    };

    // Two guardians cast a share for the same contract.
    for peer in [0u16, 1] {
        let meta = CiMeta {
            federation_id,
            session_index: 5,
            item_index: peer as u64,
            peer: PeerId::from(peer),
            peer_count: 2,
        };
        module.process_ci(&mut ctx, &ci, &meta).await.unwrap();
    }
    // A BlockCount CI must not create a decryption share.
    let block_count = LightningConsensusItem::BlockCount(800_000).into_dyn(0);
    let bc_meta = CiMeta {
        federation_id,
        session_index: 5,
        item_index: 9,
        peer: PeerId::from(0),
        peer_count: 2,
    };
    module
        .process_ci(&mut ctx, &block_count, &bc_meta)
        .await
        .unwrap();

    dbtx.commit().await.unwrap();

    let conn = pool.get().await.unwrap();
    let shares: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM fmo_ln.decryption_shares WHERE federation_id = $1 AND contract_id = $2",
            &[&fed, &contract_id.consensus_encode_to_vec()],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(shares, 2);

    // 2 guardians -> threshold 2 - (2-1)/3 = 2; 2 shares -> decrypted.
    conn.batch_execute("REFRESH MATERIALIZED VIEW fmo_ln.contract_decryption")
        .await
        .unwrap();
    let row = conn
        .query_one(
            "SELECT num_shares, num_guardians, threshold, decrypted
             FROM fmo_ln.contract_decryption WHERE federation_id = $1 AND contract_id = $2",
            &[&fed, &contract_id.consensus_encode_to_vec()],
        )
        .await
        .unwrap();
    let (num_shares, num_guardians, threshold, decrypted): (i64, i64, i64, bool) =
        (row.get(0), row.get(1), row.get(2), row.get(3));
    assert_eq!((num_shares, num_guardians, threshold), (2, 2, 2));
    assert!(decrypted);
}
