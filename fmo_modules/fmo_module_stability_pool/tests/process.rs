use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use std::time::{Duration, UNIX_EPOCH};

use fedimint_core::core::{
    DynInput, DynModuleConsensusItem, DynOutput, IntoDynInstance, ModuleKind,
};
use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::module::registry::ModuleDecoderRegistry;
use fedimint_core::secp256k1::{Keypair, Message, PublicKey, Secp256k1, SecretKey};
use fedimint_core::{Amount, PeerId, TransactionId};
use fmo_core::module::{CiMeta, ItemMeta, ObserverModule, ProcessCtx};
use fmo_core::test_util::{insert_federation, minimal_config, reset_db, test_pool, test_services};
use fmo_module_stability_pool::spec::{
    Account, AccountType, AccountUnchecked, BtcBalanceDepositMetadata, DepositToBtcBalanceOutput,
    DepositToProvideOutput, DepositToSeekOutput, FeeRate, FiatAmount, FiatOrAll, ProvideRequest,
    SeekRequest, SignedTransferRequest, StabilityPoolConsensusItem, StabilityPoolConsensusItemV0,
    StabilityPoolInput, StabilityPoolInputV0, StabilityPoolOutput, StabilityPoolOutputV0,
    StabilityPoolOutputV1, TransferOutput, TransferRequest, TransferRequestId,
    UnlockForWithdrawalInput, WithdrawalInput,
};
use fmo_module_stability_pool::StabilityPoolObserver;

const NONCE: &str = "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798";

/// Tests share one database (`reset_db` drops/recreates the public schema), so
/// serialize the DB-touching tests within this binary.
static DB_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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
    let _guard = DB_LOCK.lock().await;
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

fn keypair(secp: &Secp256k1<fedimint_core::secp256k1::All>, seed: u8) -> Keypair {
    Keypair::from_secret_key(secp, &SecretKey::from_slice(&[seed; 32]).unwrap())
}

/// End-to-end: process deposits, a folded unlock+withdrawal, a multisig
/// withdrawal, a signed transfer and a cycle vote, then refresh the fiat gold
/// matviews and assert the folded/valued rows and per-account totals.
#[tokio::test]
async fn stability_pool_fiat_gold_layer() {
    let _guard = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else {
        eprintln!("skipping: FMO_TEST_DATABASE unset");
        return;
    };
    reset_db(&pool).await;
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
    fmo_core::db::migrations::setup_module_schema(
        &pool,
        "multi_sig_stability_pool",
        module.version(),
        module.migrations(),
    )
    .await
    .unwrap();

    let secp = Secp256k1::new();
    let kp_a = keypair(&secp, 1);
    let account_a = Account::single(kp_a.public_key(), AccountType::Seeker);
    let account_b = Account::single(keypair(&secp, 2).public_key(), AccountType::Seeker);
    let account_p = Account::single(keypair(&secp, 3).public_key(), AccountType::Provider);
    // 2-of-3 multisig seeker (a "multispend" account).
    let account_m: Account = AccountUnchecked {
        acc_type: AccountType::Seeker,
        pub_keys: BTreeSet::from([
            keypair(&secp, 4).public_key(),
            keypair(&secp, 5).public_key(),
            keypair(&secp, 6).public_key(),
        ]),
        threshold: 2,
    }
    .try_into()
    .unwrap();

    let id_a = account_a.id().to_string();
    let id_b = account_b.id().to_string();
    let id_m = account_m.id().to_string();
    let id_p = account_p.id().to_string();

    // Cycle 1 started at T0 (from the vote); the session's transactions happen
    // an hour later, so they value at cycle 1's price.
    const T0: i64 = 1_700_000_000;
    const PRICE: u64 = 6_000_000_000;

    let fed = federation_id.consensus_encode_to_vec();
    let txid = TransactionId::consensus_decode_whole(&[9; 32], &Default::default()).unwrap();
    let txid_bytes = txid.consensus_encode_to_vec();

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
        conn.execute(
            "INSERT INTO session_times
             VALUES ($1, 0, TIMESTAMP 'epoch' + ($2::bigint + 3600) * INTERVAL '1 second')",
            &[&fed, &T0],
        )
        .await
        .unwrap();
        for idx in 0..3i32 {
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

    // Output 0: seek deposit of 0.005 BTC by account A.
    let seek = StabilityPoolOutput::V1(StabilityPoolOutputV1::DepositToSeek(DepositToSeekOutput {
        account_id: account_a.id(),
        seek_request: SeekRequest(Amount::from_msats(500_000_000)),
    }));
    let dyn_output: DynOutput = decode_dyn(&seek);
    module
        .process_output(&mut ctx, &dyn_output, &item(0))
        .await
        .unwrap();

    // Output 1: provide deposit by account P.
    let provide = StabilityPoolOutput::V1(StabilityPoolOutputV1::DepositToProvide(
        DepositToProvideOutput {
            account_id: account_p.id(),
            provide_request: ProvideRequest {
                amount: Amount::from_msats(100_000_000),
                min_fee_rate: FeeRate(200),
            },
        },
    ));
    let dyn_output: DynOutput = decode_dyn(&provide);
    module
        .process_output(&mut ctx, &dyn_output, &item(1))
        .await
        .unwrap();

    // Output 2: signed transfer of 2500 fiat from A to B.
    let req = TransferRequest::new(
        1,
        account_a.clone(),
        FiatAmount(2500),
        account_b.id(),
        vec![],
        100,
        None,
    )
    .unwrap();
    let msg = Message::from(&TransferRequestId::from(&req));
    let sig = secp.sign_schnorr_no_aux_rand(&msg, &kp_a);
    let signed = SignedTransferRequest::new(req, BTreeMap::from([(0u64, sig)])).unwrap();
    let transfer = StabilityPoolOutput::V1(StabilityPoolOutputV1::Transfer(TransferOutput {
        signed_request: signed,
    }));
    let dyn_output: DynOutput = decode_dyn(&transfer);
    module
        .process_output(&mut ctx, &dyn_output, &item(2))
        .await
        .unwrap();

    // Input 0: unlock request by A (fiat target, 0 msat).
    let unlock: StabilityPoolInput =
        StabilityPoolInputV0::UnlockForWithdrawal(UnlockForWithdrawalInput {
            account: account_a.clone(),
            amount: FiatOrAll::Fiat(FiatAmount(12_000_000)),
        })
        .into();
    let dyn_input: DynInput = decode_dyn(&unlock);
    module
        .process_input(&mut ctx, &dyn_input, &item(0))
        .await
        .unwrap();

    // Input 1: A's actual withdrawal of 0.002 BTC (folds with the unlock above).
    let withdrawal: StabilityPoolInput = StabilityPoolInputV0::Withdrawal(WithdrawalInput {
        account: account_a.clone(),
        amount: Amount::from_msats(200_000_000),
    })
    .into();
    let dyn_input: DynInput = decode_dyn(&withdrawal);
    module
        .process_input(&mut ctx, &dyn_input, &item(1))
        .await
        .unwrap();

    // Input 2: multisig account M withdraws (no preceding unlock observed).
    let withdrawal_m: StabilityPoolInput = StabilityPoolInputV0::Withdrawal(WithdrawalInput {
        account: account_m.clone(),
        amount: Amount::from_msats(50_000_000),
    })
    .into();
    let dyn_input: DynInput = decode_dyn(&withdrawal_m);
    module
        .process_input(&mut ctx, &dyn_input, &item(2))
        .await
        .unwrap();

    // Cycle vote: starts cycle 1 at T0 with the reference price.
    let ci: StabilityPoolConsensusItem =
        StabilityPoolConsensusItem::V0(StabilityPoolConsensusItemV0 {
            next_cycle_index: 1,
            time: UNIX_EPOCH + Duration::from_secs(T0 as u64),
            price: FiatAmount(PRICE),
        });
    let dyn_ci: DynModuleConsensusItem = decode_dyn(&ci);
    module
        .process_ci(
            &mut ctx,
            &dyn_ci,
            &CiMeta {
                federation_id,
                session_index: 0,
                item_index: 0,
                peer: PeerId::from(0),
                peer_count: 4,
            },
        )
        .await
        .unwrap();

    dbtx.commit().await.unwrap();

    // Refresh the gold matviews (dependency order; non-concurrent is fine).
    let conn = pool.get().await.unwrap();
    for view in [
        "cycles",
        "account_tx",
        "account_tx_legs",
        "account_totals",
        "transfer_edges",
        "sp_daily",
        "pool_flows",
    ] {
        conn.batch_execute(&format!(
            "REFRESH MATERIALIZED VIEW fmo_multi_sig_stability_pool.{view}"
        ))
        .await
        .unwrap();
    }

    // cycles: the single vote reconstructs cycle 1's price.
    let (cycle_price, num_votes): (i64, i64) = {
        let row = conn
            .query_one(
                "SELECT start_price_fiat, num_votes FROM fmo_multi_sig_stability_pool.cycles
                 WHERE federation_id = $1 AND cycle_index = 1",
                &[&fed],
            )
            .await
            .unwrap();
        (row.get(0), row.get(1))
    };
    assert_eq!(cycle_price, PRICE as i64);
    assert_eq!(num_votes, 1);

    // A's withdraw row is folded (one row) and valued at the cycle price.
    let (wd_kind_count, wd_amount, wd_fiat): (i64, Option<i64>, Option<i64>) = {
        let row = conn
            .query_one(
                "SELECT COUNT(*)::bigint, MIN(amount_msat), MIN(fiat_amount)
                 FROM fmo_multi_sig_stability_pool.account_tx
                 WHERE federation_id = $1 AND account_id = $2 AND kind = 'withdraw'",
                &[&fed, &id_a],
            )
            .await
            .unwrap();
        (row.get(0), row.get(1), row.get(2))
    };
    assert_eq!(
        wd_kind_count, 1,
        "unlock+withdrawal should fold into one row"
    );
    assert_eq!(wd_amount, Some(200_000_000));
    // 200_000_000 msat * 6_000_000_000 / 1e11 = 12_000_000
    assert_eq!(wd_fiat, Some(12_000_000));

    // The folded withdraw has both an unlock and a withdrawal leg.
    let legs: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM fmo_multi_sig_stability_pool.account_tx_legs l
             JOIN fmo_multi_sig_stability_pool.account_tx t USING (federation_id, tx_key)
             WHERE t.federation_id = $1 AND t.account_id = $2 AND t.kind = 'withdraw'",
            &[&fed, &id_a],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(legs, 2);

    // A's totals: net flows in msat and fiat, plus the outgoing transfer.
    let a_totals = conn
        .query_one(
            "SELECT fiat_deposited, fiat_withdrawn, fiat_net, transfers_out_fiat,
                    is_multisig, threshold
             FROM fmo_multi_sig_stability_pool.account_totals
             WHERE federation_id = $1 AND account_id = $2",
            &[&fed, &id_a],
        )
        .await
        .unwrap();
    assert_eq!(a_totals.get::<_, i64>(0), 30_000_000); // seek 0.005 BTC
    assert_eq!(a_totals.get::<_, i64>(1), 12_000_000); // withdraw 0.002 BTC
    assert_eq!(a_totals.get::<_, i64>(2), 18_000_000); // net
    assert_eq!(a_totals.get::<_, i64>(3), 2500); // transfer out
    assert!(!a_totals.get::<_, bool>(4));
    assert_eq!(a_totals.get::<_, Option<i64>>(5), Some(1));

    // M is recognized as a 2-of-3 multisig account.
    let m_totals = conn
        .query_one(
            "SELECT is_multisig, threshold, n_keys, fiat_withdrawn
             FROM fmo_multi_sig_stability_pool.account_totals
             WHERE federation_id = $1 AND account_id = $2",
            &[&fed, &id_m],
        )
        .await
        .unwrap();
    assert!(m_totals.get::<_, bool>(0));
    assert_eq!(m_totals.get::<_, Option<i64>>(1), Some(2));
    assert_eq!(m_totals.get::<_, Option<i64>>(2), Some(3));
    assert_eq!(m_totals.get::<_, i64>(3), 3_000_000); // 50_000_000 msat valued

    // B received the transfer.
    let b_in: i64 = conn
        .query_one(
            "SELECT transfers_in_fiat FROM fmo_multi_sig_stability_pool.account_totals
             WHERE federation_id = $1 AND account_id = $2",
            &[&fed, &id_b],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(b_in, 2500);

    // Provider deposit valued.
    let p_dep: i64 = conn
        .query_one(
            "SELECT fiat_deposited FROM fmo_multi_sig_stability_pool.account_totals
             WHERE federation_id = $1 AND account_id = $2",
            &[&fed, &id_p],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(p_dep, 6_000_000);

    // Silver transfer row + aggregated edge.
    let (transfer_fiat, account_keys_m): (i64, i64) = {
        let t: i64 = conn
            .query_one(
                "SELECT transfer_fiat FROM fmo_multi_sig_stability_pool.transfers
                 WHERE federation_id = $1 AND from_account_id = $2 AND to_account_id = $3",
                &[&fed, &id_a, &id_b],
            )
            .await
            .unwrap()
            .get(0);
        let k: i64 = conn
            .query_one(
                "SELECT COUNT(*) FROM fmo_multi_sig_stability_pool.account_keys
                 WHERE federation_id = $1 AND account_id = $2",
                &[&fed, &id_m],
            )
            .await
            .unwrap()
            .get(0);
        (t, k)
    };
    assert_eq!(transfer_fiat, 2500);
    assert_eq!(account_keys_m, 3);

    let edge_total: i64 = conn
        .query_one(
            "SELECT total_fiat FROM fmo_multi_sig_stability_pool.transfer_edges
             WHERE federation_id = $1 AND from_account_id = $2 AND to_account_id = $3",
            &[&fed, &id_a, &id_b],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(edge_total, 2500);

    // Transfers are no longer written to the deposits table.
    let deposit_transfers: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM fmo_multi_sig_stability_pool.deposits
             WHERE federation_id = $1 AND action = 'transfer'",
            &[&fed],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(deposit_transfers, 0);
}
