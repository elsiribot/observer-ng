use std::str::FromStr;

use fedimint_core::core::{IntoDynInstance, ModuleKind};
use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::{Amount, PeerId, TransactionId};
use fedimint_walletv2_common::{
    StandardScript, WalletConsensusItem, WalletInput, WalletInputV0, WalletOutput, WalletOutputV0,
};
use fmo_core::module::{CiMeta, ItemMeta, ObserverModule, ProcessCtx};
use fmo_core::test_util::{insert_federation, minimal_config, reset_db, test_pool, test_services};
use fmo_module_walletv2::WalletV2Observer;

const RECEIVE_ADDRESS: &str = "bc1qvzvkjn4q3nszqxrv3nraga2r822xjty3ykvkuw";

/// Tests share one database (`reset_db` drops/recreates the public schema), so
/// serialize the DB-touching tests within this binary.
static DB_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn test_pk() -> fedimint_core::secp256k1::PublicKey {
    fedimint_core::secp256k1::PublicKey::from_str(
        "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798",
    )
    .unwrap()
}

#[tokio::test]
async fn walletv2_processes_receives_sends_and_block_count_votes() {
    let _guard = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;
    let (config, federation_id) = minimal_config();
    insert_federation(&pool, &config, federation_id).await;
    let services = test_services(&pool);

    let module = WalletV2Observer;
    assert_eq!(module.kind(), ModuleKind::from_static_str("walletv2"));
    fmo_core::db::migrations::setup_module_schema(
        &pool,
        "walletv2",
        module.version(),
        module.migrations(),
    )
    .await
    .unwrap();

    let fed = federation_id.consensus_encode_to_vec();
    let txid = TransactionId::consensus_decode_whole(&[9; 32], &Default::default()).unwrap();

    // Structural rows required by the receives/sends foreign keys.
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
            "INSERT INTO transaction_inputs VALUES ($1, $2, 0, 'walletv2', NULL, NULL)",
            &[&fed, &txid.consensus_encode_to_vec()],
        )
        .await
        .unwrap();
        conn.execute(
            "INSERT INTO transaction_outputs VALUES ($1, $2, 0, 'walletv2', NULL, NULL)",
            &[&fed, &txid.consensus_encode_to_vec()],
        )
        .await
        .unwrap();
        // block time known for the voted height
        conn.execute(
            "INSERT INTO block_times VALUES ($1, NOW()::timestamp)",
            &[&850_000i32],
        )
        .await
        .unwrap();
    }

    let mut conn = pool.get().await.unwrap();
    let dbtx = conn.transaction().await.unwrap();
    dbtx.batch_execute("SET LOCAL search_path TO fmo_walletv2, public")
        .await
        .unwrap();
    let mut ctx = ProcessCtx {
        dbtx: &dbtx,
        federation_id,
        config,
        services,
    };

    // Peg-in claim: no amount attributable from the input alone
    let input = WalletInput::V0(WalletInputV0 {
        output_index: 42,
        tweak: test_pk(),
        fee: bitcoin::Amount::from_sat(1_000),
    })
    .into_dyn(0);
    let meta = ItemMeta {
        federation_id,
        txid,
        session_index: 0,
        item_index: 0,
        index: 0,
        peer_count: 4,
    };
    let processed = module.process_input(&mut ctx, &input, &meta).await.unwrap();
    assert_eq!(processed.amount, None);
    assert!(processed.details.is_some());

    // Peg-out: fedimint transaction is debited value + fee
    let destination_address = RECEIVE_ADDRESS
        .parse::<bitcoin::Address<bitcoin::address::NetworkUnchecked>>()
        .unwrap()
        .require_network(bitcoin::Network::Bitcoin)
        .unwrap();
    let output = WalletOutput::V0(WalletOutputV0 {
        destination: StandardScript::from_address(&destination_address).unwrap(),
        value: bitcoin::Amount::from_sat(50_000),
        fee: bitcoin::Amount::from_sat(2_000),
    })
    .into_dyn(0);
    let processed = module
        .process_output(&mut ctx, &output, &meta)
        .await
        .unwrap();
    assert_eq!(processed.amount, Some(Amount::from_msats(52_000_000)));
    assert!(processed.details.is_some());

    // Block count vote feeds core session time votes
    let ci = WalletConsensusItem::BlockCount(850_000).into_dyn(0);
    let ci_meta = CiMeta {
        federation_id,
        session_index: 0,
        item_index: 1,
        peer: PeerId::from(2),
        peer_count: 4,
    };
    let details = module.process_ci(&mut ctx, &ci, &ci_meta).await.unwrap();
    assert!(details.is_some());

    dbtx.commit().await.unwrap();

    let conn = pool.get().await.unwrap();
    let receives: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM fmo_walletv2.receives
             WHERE federation_id = $1 AND output_index = 42 AND fee_msat = 1000000",
            &[&fed],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(receives, 1);

    let sends: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM fmo_walletv2.sends
             WHERE federation_id = $1 AND address = $2 AND value_msat = 50000000 AND fee_msat = 2000000",
            &[&fed, &RECEIVE_ADDRESS],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(sends, 1);

    let height_votes: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM fmo_walletv2.block_height_votes
             WHERE federation_id = $1 AND height_vote = 850000 AND proposer = 2",
            &[&fed],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(height_votes, 1);

    let time_votes: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM session_time_votes
             WHERE federation_id = $1 AND session_index = 0 AND source_kind = 'walletv2' AND peer_id = 2",
            &[&fed],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(time_votes, 1);
}

/// A `Signatures` consensus item records the on-chain wallet-tx txid into
/// `wallet_utxos` with a NULL (not-yet-resolved) value. The txid is stored in
/// internal byte order so it round-trips via `Txid::from_slice`.
#[tokio::test]
async fn walletv2_records_signatures_txid_for_utxo_resolution() {
    use bitcoin::hashes::Hash;

    let _guard = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;
    let (config, federation_id) = minimal_config();
    insert_federation(&pool, &config, federation_id).await;
    let services = test_services(&pool);

    let module = WalletV2Observer;
    fmo_core::db::migrations::setup_module_schema(
        &pool,
        "walletv2",
        module.version(),
        module.migrations(),
    )
    .await
    .unwrap();

    let fed = federation_id.consensus_encode_to_vec();

    // The on-chain txid the federation's signatures commit to.
    let onchain_txid = bitcoin::Txid::from_byte_array([0xab; 32]);

    let mut conn = pool.get().await.unwrap();
    let dbtx = conn.transaction().await.unwrap();
    dbtx.batch_execute("SET LOCAL search_path TO fmo_walletv2, public")
        .await
        .unwrap();
    let mut ctx = ProcessCtx {
        dbtx: &dbtx,
        federation_id,
        config,
        services,
    };

    // Two peers announce the same transition (same txid) at different item
    // indexes; both are recorded, both unresolved.
    for (item_index, peer) in [(5u64, 1u16), (6, 2)] {
        let ci = WalletConsensusItem::Signatures(onchain_txid, vec![]).into_dyn(0);
        let ci_meta = CiMeta {
            federation_id,
            session_index: 3,
            item_index,
            peer: PeerId::from(peer),
            peer_count: 4,
        };
        let details = module.process_ci(&mut ctx, &ci, &ci_meta).await.unwrap();
        assert!(details.is_some());
    }

    dbtx.commit().await.unwrap();

    let conn = pool.get().await.unwrap();
    let rows = conn
        .query(
            "SELECT session_index, item_index, txid, utxo_value_msat, resolved_at
             FROM fmo_walletv2.wallet_utxos WHERE federation_id = $1
             ORDER BY item_index",
            &[&fed],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    for row in &rows {
        let txid_bytes: Vec<u8> = row.get("txid");
        assert_eq!(
            bitcoin::Txid::from_slice(&txid_bytes).unwrap(),
            onchain_txid
        );
        assert_eq!(row.get::<_, i32>("session_index"), 3);
        assert!(row.get::<_, Option<i64>>("utxo_value_msat").is_none());
        assert!(row
            .get::<_, Option<chrono::NaiveDateTime>>("resolved_at")
            .is_none());
    }
    assert_eq!(rows[0].get::<_, i32>("item_index"), 5);
    assert_eq!(rows[1].get::<_, i32>("item_index"), 6);
}
