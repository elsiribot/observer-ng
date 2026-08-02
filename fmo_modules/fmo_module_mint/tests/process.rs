use fedimint_core::core::{IntoDynInstance, ModuleKind};
use fedimint_core::encoding::Decodable;
use fedimint_core::TransactionId;
use fedimint_mint_common::MintInput;
use fmo_core::module::{ItemMeta, ObserverModule, ProcessCtx};
use fmo_core::test_util::{insert_federation, minimal_config, reset_db, test_pool, test_services};
use fmo_module_mint::MintObserver;

/// Unknown input versions must not panic (unlike the pre-modularization code
/// that used `.expect("Not v0")`): the module stores the JSON representation
/// and reports no amount.
#[tokio::test]
async fn unknown_mint_input_version_is_graceful() {
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;
    let (config, federation_id) = minimal_config();
    insert_federation(&pool, &config, federation_id).await;
    let services = test_services(&pool);

    let input = MintInput::Default {
        variant: 999,
        bytes: vec![1, 2, 3],
    }
    .into_dyn(0);

    let mut conn = pool.get().await.unwrap();
    let dbtx = conn.transaction().await.unwrap();
    let mut ctx = ProcessCtx {
        dbtx: &dbtx,
        federation_id,
        config,
        services,
    };
    let meta = ItemMeta {
        federation_id,
        txid: TransactionId::consensus_decode_whole(&[0; 32], &Default::default()).unwrap(),
        session_index: 0,
        item_index: 0,
        index: 0,
        peer_count: 1,
    };

    let module = MintObserver;
    assert_eq!(module.kind(), ModuleKind::from_static_str("mint"));
    let processed = module.process_input(&mut ctx, &input, &meta).await.unwrap();
    assert!(processed.amount.is_none());
    let details = processed.details.expect("details JSON present");
    assert!(details.to_string().contains("Default"));
}
