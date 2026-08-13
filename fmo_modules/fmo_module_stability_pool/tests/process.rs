use std::str::FromStr;
use std::time::{Duration, UNIX_EPOCH};

use fedimint_core::core::{
    DynInput, DynModuleConsensusItem, DynOutput, IntoDynInstance, ModuleKind,
};
use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::module::registry::ModuleDecoderRegistry;
use fedimint_core::secp256k1::PublicKey;
use fedimint_core::{Amount, PeerId, TransactionId};
use fmo_core::module::{CiMeta, ItemMeta, ObserverModule, ProcessCtx};
use fmo_core::test_util::{insert_federation, minimal_config, reset_db, test_pool, test_services};
use fmo_module_stability_pool::spec::{
    Account, AccountType, BtcBalanceDepositMetadata, DepositToBtcBalanceOutput,
    DepositToProvideOutput, DepositToSeekOutput, FeeRate, FiatAmount, FiatOrAll, ProvideRequest,
    SeekRequest, StabilityPoolConsensusItem, StabilityPoolConsensusItemV0, StabilityPoolInput,
    StabilityPoolInputV0, StabilityPoolOutput, StabilityPoolOutputV0, StabilityPoolOutputV1,
    UnlockForWithdrawalInput, WithdrawalInput,
};
use fmo_module_stability_pool::StabilityPoolObserver;

const NONCE: &str = "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798";

fn seeker_account() -> Account {
    Account::single(PublicKey::from_str(NONCE).unwrap(), AccountType::Seeker)
}

fn provider_account() -> Account {
    Account::single(PublicKey::from_str(NONCE).unwrap(), AccountType::Provider)
}

/// Decodes a value through the module's real `Decoder` (as the dispatch engine
/// does with production bytes) into its dynamic type, proving the vendored
/// consensus types decode the on-the-wire encoding.
fn decode_dyn<T, D: 'static + std::any::Any>(value: &T) -> D
where
    T: Encodable,
{
    let module = StabilityPoolObserver;
    let decoder = module.decoder();
    let registry = ModuleDecoderRegistry::new([(
        0,
        ModuleKind::from_static_str("multi_sig_stability_pool"),
        decoder.clone(),
    )]);
    let bytes = value.consensus_encode_to_vec();
    let mut slice = &bytes[..];
    decoder
        .decode_complete::<D>(&mut slice, bytes.len() as u64, 0, &registry)
        .expect("stability pool item should decode")
}

#[tokio::test]
async fn stability_pool_resolves_input_output_and_ci_amounts() {
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;
    // reset_db only drops `public`; drop our module schema too so a schema
    // change between runs against the persistent test DB takes effect.
    pool.get()
        .await
        .unwrap()
        .batch_execute("DROP SCHEMA IF EXISTS fmo_multi_sig_stability_pool CASCADE")
        .await
        .unwrap();
    let (config, federation_id) = minimal_config();
    insert_federation(&pool, &config, federation_id).await;
    let services = test_services(&pool);

    let module = StabilityPoolObserver;
    assert_eq!(
        module.kind(),
        ModuleKind::from_static_str("multi_sig_stability_pool")
    );
    fmo_core::db::migrations::setup_module_schema(
        &pool,
        "multi_sig_stability_pool",
        module.version(),
        module.migrations(),
    )
    .await
    .unwrap();

    let fed = federation_id.consensus_encode_to_vec();
    let txid = TransactionId::consensus_decode_whole(&[7; 32], &Default::default()).unwrap();
    let txid_bytes = txid.consensus_encode_to_vec();

    // Structural rows required by the withdrawals/deposits foreign keys.
    // Inputs and outputs at indices 0..4.
    {
        let conn = pool.get().await.unwrap();
        conn.execute("INSERT INTO sessions VALUES ($1, 0, ''::bytea)", &[&fed])
            .await
            .unwrap();
        conn.execute(
            "INSERT INTO transactions VALUES ($1, $2, 0, 0, ''::bytea)",
            &[&fed, &txid_bytes],
        )
        .await
        .unwrap();
        for idx in 0..4i32 {
            conn.execute(
                "INSERT INTO transaction_inputs
                 VALUES ($1, $2, $3, 'multi_sig_stability_pool', NULL, NULL)",
                &[&fed, &txid_bytes, &idx],
            )
            .await
            .unwrap();
            conn.execute(
                "INSERT INTO transaction_outputs
                 VALUES ($1, $2, $3, 'multi_sig_stability_pool', NULL, NULL)",
                &[&fed, &txid_bytes, &idx],
            )
            .await
            .unwrap();
        }
    }

    let mut conn = pool.get().await.unwrap();
    let dbtx = conn.transaction().await.unwrap();
    dbtx.batch_execute("SET LOCAL search_path TO fmo_multi_sig_stability_pool, public")
        .await
        .unwrap();
    let mut ctx = ProcessCtx {
        dbtx: &dbtx,
        federation_id,
        config,
        services,
    };
    let item = |index: u64| ItemMeta {
        federation_id,
        txid,
        session_index: 0,
        item_index: 0,
        index,
        peer_count: 4,
    };

    // Output 0: deposit 5000 msat as a seek. Amount leaves the tx into the pool.
    let seek_output =
        StabilityPoolOutput::V0(StabilityPoolOutputV0::DepositToSeek(DepositToSeekOutput {
            account_id: seeker_account().id(),
            seek_request: SeekRequest(Amount::from_msats(5000)),
        }));
    let dyn_output: DynOutput = decode_dyn(&seek_output);
    let processed = module
        .process_output(&mut ctx, &dyn_output, &item(0))
        .await
        .unwrap();
    assert_eq!(processed.amount, Some(Amount::from_msats(5000)));
    assert!(processed.details.is_some());

    // Output 1: deposit 8000 msat as a provide with a min fee rate.
    let provide_output = StabilityPoolOutput::V0(StabilityPoolOutputV0::DepositToProvide(
        DepositToProvideOutput {
            account_id: provider_account().id(),
            provide_request: ProvideRequest {
                amount: Amount::from_msats(8000),
                min_fee_rate: FeeRate(150),
            },
        },
    ));
    let dyn_output: DynOutput = decode_dyn(&provide_output);
    let processed = module
        .process_output(&mut ctx, &dyn_output, &item(1))
        .await
        .unwrap();
    assert_eq!(processed.amount, Some(Amount::from_msats(8000)));

    // Output 2: V1-exclusive DepositToBtcBalance of 12000 msat. The newest
    // output variant; only reachable through the V1 enum.
    let btc_balance_output = StabilityPoolOutput::V1(StabilityPoolOutputV1::DepositToBtcBalance(
        DepositToBtcBalanceOutput {
            account_id: Account::single(
                PublicKey::from_str(NONCE).unwrap(),
                AccountType::BtcDepositor,
            )
            .id(),
            seek_request: SeekRequest(Amount::from_msats(12000)),
            metadata: BtcBalanceDepositMetadata(vec![0xde, 0xad, 0xbe, 0xef]),
        },
    ));
    let dyn_output: DynOutput = decode_dyn(&btc_balance_output);
    let processed = module
        .process_output(&mut ctx, &dyn_output, &item(2))
        .await
        .unwrap();
    assert_eq!(processed.amount, Some(Amount::from_msats(12000)));
    assert!(processed.details.is_some());

    // Output 3: an unknown future output version (the extensible-type Default
    // variant). The module must degrade gracefully: no amount, JSON details,
    // no panic. Built via into_dyn so the concrete Default value reaches the
    // hook without relying on the decoder's own default framing.
    let unknown_output = StabilityPoolOutput::Default {
        variant: 42,
        bytes: vec![1, 2, 3, 4],
    }
    .into_dyn(0);
    let processed = module
        .process_output(&mut ctx, &unknown_output, &item(3))
        .await
        .unwrap();
    assert_eq!(processed.amount, None);
    assert!(processed.details.is_some());

    // Input 0: withdraw 3000 msat from the pool into the tx.
    let withdrawal: StabilityPoolInput = StabilityPoolInputV0::Withdrawal(WithdrawalInput {
        account: seeker_account(),
        amount: Amount::from_msats(3000),
    })
    .into();
    let dyn_input: DynInput = decode_dyn(&withdrawal);
    let processed = module
        .process_input(&mut ctx, &dyn_input, &item(0))
        .await
        .unwrap();
    assert_eq!(processed.amount, Some(Amount::from_msats(3000)));

    // Input 1: unlock request (reserves funds, moves 0 msat) targeting fiat.
    let unlock: StabilityPoolInput =
        StabilityPoolInputV0::UnlockForWithdrawal(UnlockForWithdrawalInput {
            account: seeker_account(),
            amount: FiatOrAll::Fiat(FiatAmount(4200)),
        })
        .into();
    let dyn_input: DynInput = decode_dyn(&unlock);
    let processed = module
        .process_input(&mut ctx, &dyn_input, &item(1))
        .await
        .unwrap();
    assert_eq!(processed.amount, Some(Amount::ZERO));

    // Consensus item: a V0 cycle-turnover vote carrying a wall-clock time.
    let vote_time = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let ci: StabilityPoolConsensusItem =
        StabilityPoolConsensusItem::V0(StabilityPoolConsensusItemV0 {
            next_cycle_index: 42,
            time: vote_time,
            price: FiatAmount(6_500_000),
        });
    let dyn_ci: DynModuleConsensusItem = decode_dyn(&ci);
    let ci_meta = CiMeta {
        federation_id,
        session_index: 0,
        item_index: 0,
        peer: PeerId::from(0),
        peer_count: 4,
    };
    let details = module
        .process_ci(&mut ctx, &dyn_ci, &ci_meta)
        .await
        .unwrap();
    assert!(details.is_some());

    dbtx.commit().await.unwrap();

    // Verify the silver rows.
    let conn = pool.get().await.unwrap();
    let deposits: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM fmo_multi_sig_stability_pool.deposits
             WHERE federation_id = $1 AND amount_msat IN (5000, 8000)",
            &[&fed],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(deposits, 2);

    let provide_fee: Option<i64> = conn
        .query_one(
            "SELECT min_fee_rate_ppb FROM fmo_multi_sig_stability_pool.deposits
             WHERE federation_id = $1 AND action = 'deposit_to_provide'",
            &[&fed],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(provide_fee, Some(150));

    // The V1 btc-balance deposit is recorded with version 1 and its amount.
    let btc_balance_amount: i64 = conn
        .query_one(
            "SELECT amount_msat FROM fmo_multi_sig_stability_pool.deposits
             WHERE federation_id = $1 AND action = 'deposit_to_btc_balance' AND version = 1",
            &[&fed],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(btc_balance_amount, 12000);

    // The unknown-version output degraded gracefully: no deposits row written.
    let unknown_rows: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM fmo_multi_sig_stability_pool.deposits
             WHERE federation_id = $1 AND out_index = 3",
            &[&fed],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(unknown_rows, 0);

    let unlock_fiat: Option<i64> = conn
        .query_one(
            "SELECT unlock_fiat FROM fmo_multi_sig_stability_pool.withdrawals
             WHERE federation_id = $1 AND kind = 'unlock_for_withdrawal'",
            &[&fed],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(unlock_fiat, Some(4200));

    let votes: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM fmo_multi_sig_stability_pool.cycle_votes
             WHERE federation_id = $1 AND next_cycle_index = 42 AND price_fiat = 6500000",
            &[&fed],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(votes, 1);
}
