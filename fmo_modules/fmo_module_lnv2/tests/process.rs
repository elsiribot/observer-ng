use std::str::FromStr;

use fedimint_core::core::{IntoDynInstance, ModuleKind};
use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::secp256k1::PublicKey;
use fedimint_core::{Amount, OutPoint, PeerId, TransactionId};
use fedimint_lnv2_common::contracts::{OutgoingContract, PaymentImage};
use fedimint_lnv2_common::{
    LightningConsensusItem, LightningInput, LightningInputV0, LightningOutput, LightningOutputV0,
    OutgoingWitness,
};
use fmo_core::module::{CiMeta, ItemMeta, ObserverModule, ProcessCtx};
use fmo_core::test_util::{insert_federation, minimal_config, reset_db, test_pool, test_services};
use fmo_module_lnv2::LnV2Observer;

fn test_pk() -> PublicKey {
    PublicKey::from_str("0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798")
        .unwrap()
}

#[tokio::test]
async fn lnv2_output_records_contract_and_time_vote_feeds_session_times() {
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;
    let (config, federation_id) = minimal_config();
    insert_federation(&pool, &config, federation_id).await;
    let services = test_services(&pool);

    let module = LnV2Observer;
    assert_eq!(module.kind(), ModuleKind::from_static_str("lnv2"));
    fmo_core::db::migrations::setup_module_schema(
        &pool,
        "lnv2",
        module.version(),
        module.migrations(),
    )
    .await
    .unwrap();

    let fed = federation_id.consensus_encode_to_vec();
    let txid = TransactionId::consensus_decode_whole(&[9; 32], &Default::default()).unwrap();

    // FK targets for the contract row's txid/out_index are only on
    // public.transaction_outputs via input tables? contracts has no FK to
    // outputs, but sessions/transactions rows keep the fixture realistic.
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
    }

    let contract = OutgoingContract {
        payment_image: PaymentImage::Point(test_pk()),
        amount: Amount::from_msats(50_000),
        expiration: 123_456,
        claim_pk: test_pk(),
        refund_pk: test_pk(),
        ephemeral_pk: test_pk(),
    };
    let contract_id = contract.contract_id();
    let output = LightningOutput::V0(LightningOutputV0::Outgoing(contract)).into_dyn(0);

    let mut conn = pool.get().await.unwrap();
    let dbtx = conn.transaction().await.unwrap();
    dbtx.batch_execute("SET LOCAL search_path TO fmo_lnv2, public")
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
        peer_count: 4,
    };
    let processed = module
        .process_output(&mut ctx, &output, &meta)
        .await
        .unwrap();
    assert_eq!(processed.amount, Some(Amount::from_msats(50_000)));
    assert!(processed.details.is_some());

    // Unix time vote feeds core session time votes
    let unix_time: u64 = 1_700_000_000;
    let ci = LightningConsensusItem::UnixTimeVote(unix_time).into_dyn(0);
    let ci_meta = CiMeta {
        federation_id,
        session_index: 0,
        item_index: 1,
        peer: PeerId::from(1),
        peer_count: 4,
    };
    let details = module.process_ci(&mut ctx, &ci, &ci_meta).await.unwrap();
    assert!(details.is_some());

    // A later claim input references the contract's funding outpoint; its
    // amount is resolved from the contracts table and marked as such.
    let claim_txid = TransactionId::consensus_decode_whole(&[8; 32], &Default::default()).unwrap();
    dbtx.execute(
        "INSERT INTO public.transactions VALUES ($1, $2, 0, 2, ''::bytea)",
        &[&fed, &claim_txid.consensus_encode_to_vec()],
    )
    .await
    .unwrap();
    dbtx.execute(
        "INSERT INTO public.transaction_inputs (federation_id, txid, in_index, kind)
         VALUES ($1, $2, 0, 'lnv2')",
        &[&fed, &claim_txid.consensus_encode_to_vec()],
    )
    .await
    .unwrap();

    let claim_input = LightningInput::V0(LightningInputV0::Outgoing(
        OutPoint { txid, out_idx: 0 },
        OutgoingWitness::Refund,
    ))
    .into_dyn(0);
    let claim_meta = ItemMeta {
        federation_id,
        txid: claim_txid,
        session_index: 0,
        item_index: 2,
        index: 0,
        peer_count: 4,
    };
    let processed_claim = module
        .process_input(&mut ctx, &claim_input, &claim_meta)
        .await
        .unwrap();
    assert_eq!(processed_claim.amount, Some(Amount::from_msats(50_000)));
    assert_eq!(
        processed_claim.details.unwrap()["amount_source"],
        serde_json::json!("contract")
    );

    dbtx.commit().await.unwrap();

    let conn = pool.get().await.unwrap();
    let contracts: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM fmo_lnv2.contracts
             WHERE federation_id = $1 AND contract_id = $2 AND type = 'outgoing' AND amount_msat = 50000",
            &[&fed, &contract_id.consensus_encode_to_vec()],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(contracts, 1);

    let (time_votes, epoch): (i64, f64) = {
        let row = conn
            .query_one(
                "SELECT COUNT(*)::bigint AS n,
                        COALESCE(MAX(EXTRACT(EPOCH FROM timestamp))::float8, 0) AS epoch
                 FROM session_time_votes
                 WHERE federation_id = $1 AND source_kind = 'lnv2'",
                &[&fed],
            )
            .await
            .unwrap();
        (row.get("n"), row.get("epoch"))
    };
    assert_eq!(time_votes, 1);
    assert_eq!(epoch as i64, 1_700_000_000);
}
