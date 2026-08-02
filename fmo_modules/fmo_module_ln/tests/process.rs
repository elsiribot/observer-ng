use std::str::FromStr;

use fedimint_core::core::{IntoDynInstance, ModuleKind};
use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::{Amount, TransactionId};
use fedimint_ln_common::contracts::ContractId;
use fedimint_ln_common::{LightningInput, LightningInputV0};
use fmo_core::module::{ItemMeta, ObserverModule, ProcessCtx};
use fmo_core::test_util::{insert_federation, minimal_config, reset_db, test_pool, test_services};
use fmo_module_ln::LnObserver;

#[tokio::test]
async fn ln_input_records_contract_and_amount() {
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;
    let (config, federation_id) = minimal_config();
    insert_federation(&pool, &config, federation_id).await;
    let services = test_services(&pool);

    let module = LnObserver;
    assert_eq!(module.kind(), ModuleKind::from_static_str("ln"));
    fmo_core::db::migrations::setup_module_schema(&pool, "ln", module.version(), module.migrations())
        .await
        .unwrap();

    let contract_id = ContractId::from_str(
        "1111111111111111111111111111111111111111111111111111111111111111",
    )
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
        conn.execute(
            "INSERT INTO sessions VALUES ($1, 0, ''::bytea)",
            &[&fed],
        )
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
