use fedimint_core::core::{IntoDynInstance, ModuleKind};
use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::{PeerId, TransactionId};
use fedimint_wallet_common::WalletConsensusItem;
use fmo_core::module::{CiMeta, ObserverModule, ProcessCtx};
use fmo_core::test_util::{insert_federation, minimal_config, reset_db, test_pool, test_services};
use fmo_module_wallet::WalletObserver;

#[tokio::test]
async fn block_count_vote_feeds_session_time_votes() {
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;
    let (config, federation_id) = minimal_config();
    insert_federation(&pool, &config, federation_id).await;
    let services = test_services(&pool);

    let module = WalletObserver;
    assert_eq!(module.kind(), ModuleKind::from_static_str("wallet"));
    fmo_core::db::migrations::setup_module_schema(
        &pool,
        "wallet",
        module.version(),
        module.migrations(),
    )
    .await
    .unwrap();

    // block time known for the voted height
    let voted_height: u32 = 850_000;
    pool.get()
        .await
        .unwrap()
        .execute(
            "INSERT INTO block_times VALUES ($1, NOW()::timestamp)",
            &[&(voted_height as i32)],
        )
        .await
        .unwrap();

    let ci = WalletConsensusItem::BlockCount(voted_height).into_dyn(0);

    let mut conn = pool.get().await.unwrap();
    let dbtx = conn.transaction().await.unwrap();
    dbtx.batch_execute("SET LOCAL search_path TO fmo_wallet, public")
        .await
        .unwrap();
    let mut ctx = ProcessCtx {
        dbtx: &dbtx,
        federation_id,
        config,
        services,
    };
    let meta = CiMeta {
        federation_id,
        session_index: 7,
        item_index: 0,
        peer: PeerId::from(2),
        peer_count: 4,
    };

    // sessions row required by nothing here (no FK on votes tables to sessions)
    let details = module.process_ci(&mut ctx, &ci, &meta).await.unwrap();
    assert!(details.is_some(), "CI details JSON returned");
    dbtx.commit().await.unwrap();

    let conn = pool.get().await.unwrap();
    let fed = federation_id.consensus_encode_to_vec();
    let votes: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM fmo_wallet.block_height_votes WHERE federation_id = $1 AND height_vote = $2",
            &[&fed, &(voted_height as i32)],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(votes, 1);

    let time_votes: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM session_time_votes
             WHERE federation_id = $1 AND session_index = 7 AND source_kind = 'wallet' AND peer_id = 2",
            &[&fed],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(time_votes, 1);

    // silence unused import warnings for helper types used in other tests
    let _ = TransactionId::consensus_decode_whole(&[0; 32], &Default::default()).unwrap();
}
