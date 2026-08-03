use std::str::FromStr;

use fedimint_core::core::{IntoDynInstance, ModuleKind};
use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::secp256k1::PublicKey;
use fedimint_core::{Amount, TransactionId};
use fedimint_mintv2_common::{Denomination, MintInput, MintOutput, Note};
use fmo_core::module::{ItemMeta, ObserverModule, ProcessCtx};
use fmo_core::test_util::{insert_federation, minimal_config, reset_db, test_pool, test_services};
use fmo_module_mintv2::MintV2Observer;

const NONCE: &str = "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798";

fn test_note(denomination: u8) -> Note {
    Note {
        denomination: Denomination(denomination),
        nonce: PublicKey::from_str(NONCE).unwrap(),
        signature: tbs::Signature(bls12_381::G1Affine::generator()),
    }
}

#[tokio::test]
async fn mintv2_input_records_spent_nonce_and_amounts() {
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;
    let (config, federation_id) = minimal_config();
    insert_federation(&pool, &config, federation_id).await;
    let services = test_services(&pool);

    let module = MintV2Observer;
    assert_eq!(module.kind(), ModuleKind::from_static_str("mintv2"));
    fmo_core::db::migrations::setup_module_schema(
        &pool,
        "mintv2",
        module.version(),
        module.migrations(),
    )
    .await
    .unwrap();

    let fed = federation_id.consensus_encode_to_vec();
    let txid = TransactionId::consensus_decode_whole(&[9; 32], &Default::default()).unwrap();

    // Structural rows required by the spent_nonces foreign key.
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
            "INSERT INTO transaction_inputs VALUES ($1, $2, 0, 'mintv2', NULL, NULL)",
            &[&fed, &txid.consensus_encode_to_vec()],
        )
        .await
        .unwrap();
    }

    let mut conn = pool.get().await.unwrap();
    let dbtx = conn.transaction().await.unwrap();
    dbtx.batch_execute("SET LOCAL search_path TO fmo_mintv2, public")
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

    // Spend of a 2^10 msat note
    let input = MintInput::new_v0(test_note(10)).into_dyn(0);
    let processed = module.process_input(&mut ctx, &input, &meta).await.unwrap();
    assert_eq!(processed.amount, Some(Amount::from_msats(1024)));
    assert!(processed.details.is_some());

    // Issuance of a 2^12 msat note
    let output = MintOutput::new_v0(
        Denomination(12),
        tbs::BlindedMessage(bls12_381::G1Affine::generator()),
        [0; 16],
    )
    .into_dyn(0);
    let processed = module
        .process_output(&mut ctx, &output, &meta)
        .await
        .unwrap();
    assert_eq!(processed.amount, Some(Amount::from_msats(4096)));
    assert!(processed.details.is_some());

    dbtx.commit().await.unwrap();

    let conn = pool.get().await.unwrap();
    let spent: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM fmo_mintv2.spent_nonces
             WHERE federation_id = $1 AND nonce = $2 AND denomination = 10 AND amount_msat = 1024",
            &[&fed, &NONCE],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(spent, 1);
}
