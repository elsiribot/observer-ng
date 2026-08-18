use std::time::UNIX_EPOCH;

use chrono::DateTime;
use fedimint_core::core::{Decoder, DynInput, DynModuleConsensusItem, DynOutput, ModuleKind};
use fedimint_core::encoding::Encodable;
use fedimint_core::module::CommonModuleInit;
use fedimint_core::Amount;
use fmo_core::module::{CiMeta, ItemMeta, Migration, ObserverModule, ProcessCtx, ProcessedItem};
use tracing::warn;

pub mod spec;

use spec::{
    Account, AccountType, FiatOrAll, StabilityPoolCommonGen, StabilityPoolConsensusItem,
    StabilityPoolInput, StabilityPoolInputV0, StabilityPoolOutput, StabilityPoolOutputV0,
    StabilityPoolOutputV1, TransferOutput,
};

/// Observer module for the fedi `multi_sig_stability_pool` module: records
/// deposits into the pool (fedimint outputs) and withdrawals from it (fedimint
/// inputs) with their exact msat amounts, plus guardian cycle-turnover votes
/// (which carry a wall-clock timestamp we feed into core session time
/// estimation).
///
/// Consensus types are vendored in [`mod@spec`] because fedi's
/// `stability-pool-common` targets the fedibtc fedimint fork rather than this
/// workspace's upstream fedimint; see the `spec` module docs.
pub struct StabilityPoolObserver;

const KIND: ModuleKind = ModuleKind::from_static_str("multi_sig_stability_pool");

#[async_trait::async_trait]
impl ObserverModule for StabilityPoolObserver {
    fn kind(&self) -> ModuleKind {
        KIND
    }

    fn decoder(&self) -> Decoder {
        StabilityPoolCommonGen::decoder()
    }

    fn version(&self) -> u32 {
        2
    }

    fn migrations(&self) -> &'static [Migration] {
        &[
            Migration {
                sql: include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/schema/v0.sql")),
            },
            Migration {
                sql: include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/schema/v1.sql")),
            },
        ]
    }

    /// Gold-layer materialized views core refreshes each cycle (after
    /// `session_times` / `heal_gold`), listed in dependency order: `cycles`
    /// (price series) feeds `account_tx` (folded, fiat-valued history), which
    /// feeds the rollups.
    fn matviews(&self) -> &'static [&'static str] {
        &[
            "fmo_multi_sig_stability_pool.cycles",
            "fmo_multi_sig_stability_pool.account_tx",
            "fmo_multi_sig_stability_pool.account_tx_legs",
            "fmo_multi_sig_stability_pool.account_totals",
            "fmo_multi_sig_stability_pool.transfer_edges",
            "fmo_multi_sig_stability_pool.sp_daily",
            "fmo_multi_sig_stability_pool.pool_flows",
        ]
    }

    async fn process_input(
        &self,
        ctx: &mut ProcessCtx<'_>,
        input: &DynInput,
        meta: &ItemMeta,
    ) -> anyhow::Result<ProcessedItem> {
        let Some(sp_input) = input.as_any().downcast_ref::<StabilityPoolInput>() else {
            warn!("could not downcast stability_pool input (check decoders registry). {input:?}");
            return Ok(ProcessedItem::default());
        };

        let Some(input_v0) = sp_input.maybe_v0_ref() else {
            warn!("Unknown stability_pool input version, storing JSON only: {sp_input:?}");
            return Ok(ProcessedItem {
                amount: None,
                details: serde_json::to_value(sp_input).ok(),
            });
        };

        // Withdrawals pull msats from the pool into the transaction; the first
        // "unlock" step only reserves funds and moves nothing (amount 0). Both
        // are legitimately non-NULL amounts.
        let (kind, amount, unlock_fiat, unlock_all) = match input_v0 {
            StabilityPoolInputV0::UnlockForWithdrawal(unlock) => {
                let (fiat, all) = match unlock.amount {
                    FiatOrAll::Fiat(fiat) => (Some(fiat.0 as i64), false),
                    FiatOrAll::All => (None, true),
                };
                ("unlock_for_withdrawal", Amount::ZERO, fiat, all)
            }
            StabilityPoolInputV0::Withdrawal(withdrawal) => {
                ("withdrawal", withdrawal.amount, None, false)
            }
        };

        let account = input_v0.account();
        let account_id = account.id().to_string();
        let fed = meta.federation_id.consensus_encode_to_vec();

        ctx.dbtx
            .execute(
                "INSERT INTO withdrawals VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                 ON CONFLICT DO NOTHING",
                &[
                    &fed,
                    &meta.txid.consensus_encode_to_vec(),
                    &(meta.index as i32),
                    &kind,
                    &account_id,
                    &(amount.msats as i64),
                    &unlock_fiat,
                    &unlock_all,
                ],
            )
            .await?;

        // A withdrawal/unlock input carries the full `Account`, so we can record
        // its (multi-sig) structure — deposit outputs only carry the id hash.
        record_account(ctx, &fed, meta.session_index as i32, &account).await?;

        Ok(ProcessedItem {
            amount: Some(amount),
            details: serde_json::to_value(sp_input).ok(),
        })
    }

    async fn process_output(
        &self,
        ctx: &mut ProcessCtx<'_>,
        output: &DynOutput,
        meta: &ItemMeta,
    ) -> anyhow::Result<ProcessedItem> {
        let Some(sp_output) = output.as_any().downcast_ref::<StabilityPoolOutput>() else {
            warn!("could not downcast stability_pool output (check decoders registry). {output:?}");
            return Ok(ProcessedItem::default());
        };

        // Deposits move msats out of the transaction into the pool; transfers
        // shuffle balances between pool accounts and move nothing (amount 0).
        // `version` records which output-enum version carried the item.
        let resolved = match sp_output {
            StabilityPoolOutput::V0(v0) => Some(classify_v0(v0)),
            StabilityPoolOutput::V1(v1) => Some(classify_v1(v1)),
            StabilityPoolOutput::Default { .. } => None,
        };

        let Some(output) = resolved else {
            warn!("Unknown stability_pool output version, storing JSON only: {sp_output:?}");
            return Ok(ProcessedItem {
                amount: None,
                details: serde_json::to_value(sp_output).ok(),
            });
        };

        let fed = meta.federation_id.consensus_encode_to_vec();
        let txid = meta.txid.consensus_encode_to_vec();

        let amount = match output {
            SpOutput::Deposit(deposit) => {
                ctx.dbtx
                    .execute(
                        "INSERT INTO deposits VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                         ON CONFLICT DO NOTHING",
                        &[
                            &fed,
                            &txid,
                            &(meta.index as i32),
                            &(deposit.version as i16),
                            &deposit.action,
                            &deposit.account_id,
                            &(deposit.amount.msats as i64),
                            &deposit.min_fee_rate_ppb,
                        ],
                    )
                    .await?;
                deposit.amount
            }
            SpOutput::Transfer(transfer) => {
                // Transfers carry no msats but a signed, fiat-denominated request
                // with sender/recipient. They get their own table (not
                // `deposits`) and we record the sender's multi-sig structure.
                ctx.dbtx
                    .execute(
                        "INSERT INTO transfers
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                         ON CONFLICT DO NOTHING",
                        &[
                            &fed,
                            &txid,
                            &(meta.index as i32),
                            &(transfer.version as i16),
                            &transfer.acc_type,
                            &transfer.from_account.id().to_string(),
                            &transfer.to_account_id,
                            &transfer.transfer_fiat,
                            &transfer.valid_until_cycle,
                            &transfer.new_fee_rate_ppb,
                            &transfer.meta,
                        ],
                    )
                    .await?;
                record_account(ctx, &fed, meta.session_index as i32, &transfer.from_account)
                    .await?;
                Amount::ZERO
            }
        };

        Ok(ProcessedItem {
            amount: Some(amount),
            details: serde_json::to_value(sp_output).ok(),
        })
    }

    async fn process_ci(
        &self,
        ctx: &mut ProcessCtx<'_>,
        ci: &DynModuleConsensusItem,
        meta: &CiMeta,
    ) -> anyhow::Result<Option<serde_json::Value>> {
        let Some(sp_ci) = ci.as_any().downcast_ref::<StabilityPoolConsensusItem>() else {
            warn!("could not downcast stability_pool CI (check decoders registry). {ci:?}");
            return Ok(None);
        };

        // The V0 cycle-turnover vote carries the guardian's wall-clock time for
        // the next cycle; record it as a session time estimate and persist the
        // vote. Version-vote (V1) and unknown items are stored as JSON only.
        if let StabilityPoolConsensusItem::V0(v0) = sp_ci {
            if let Some(timestamp) = system_time_to_naive(v0.time) {
                ctx.dbtx
                    .execute(
                        "INSERT INTO cycle_votes VALUES ($1, $2, $3, $4, $5, $6, $7)
                         ON CONFLICT DO NOTHING",
                        &[
                            &meta.federation_id.consensus_encode_to_vec(),
                            &(meta.session_index as i32),
                            &(meta.item_index as i32),
                            &(meta.peer.to_usize() as i32),
                            &(v0.next_cycle_index as i64),
                            &timestamp,
                            &(v0.price.0 as i64),
                        ],
                    )
                    .await?;

                ctx.record_session_time_vote(&KIND, meta.session_index, meta.peer, timestamp)
                    .await?;
            }
        }

        Ok(serde_json::to_value(sp_ci).ok())
    }
}

/// A classified stability-pool output: either a value-bearing deposit or a
/// (0-msat) fiat-denominated transfer between pool accounts.
enum SpOutput {
    Deposit(Deposit),
    Transfer(TransferInfo),
}

/// A stability-pool deposit output normalized for storage.
struct Deposit {
    version: u8,
    action: &'static str,
    account_id: String,
    amount: Amount,
    min_fee_rate_ppb: Option<i64>,
}

/// A stability-pool transfer output normalized for storage. Carries the full
/// sender `Account` so its multi-sig structure can be recorded.
struct TransferInfo {
    version: u8,
    acc_type: &'static str,
    from_account: Account,
    to_account_id: String,
    transfer_fiat: i64,
    valid_until_cycle: i64,
    new_fee_rate_ppb: Option<i64>,
    meta: Vec<u8>,
}

/// Human-readable account-type tag matching the account-id bech32 HRP.
fn acc_type_str(acc_type: AccountType) -> &'static str {
    match acc_type {
        AccountType::Seeker => "seeker",
        AccountType::Provider => "provider",
        AccountType::BtcDepositor => "btc_depositor",
    }
}

/// Normalizes a transfer output (version-agnostic — V0/V1 transfers are
/// identical) into a [`TransferInfo`].
fn transfer_info(version: u8, t: &TransferOutput) -> TransferInfo {
    let req = t.signed_request.details();
    TransferInfo {
        version,
        acc_type: acc_type_str(req.from().acc_type()),
        from_account: req.from().clone(),
        to_account_id: req.to().to_string(),
        transfer_fiat: req.amount().0 as i64,
        valid_until_cycle: req.valid_until_cycle() as i64,
        new_fee_rate_ppb: req.new_fee_rate().map(|rate| rate.0 as i64),
        meta: req.meta().to_vec(),
    }
}

fn classify_v0(v0: &StabilityPoolOutputV0) -> SpOutput {
    match v0 {
        StabilityPoolOutputV0::DepositToSeek(o) => SpOutput::Deposit(Deposit {
            version: 0,
            action: "deposit_to_seek",
            account_id: o.account_id.to_string(),
            amount: o.seek_request.0,
            min_fee_rate_ppb: None,
        }),
        StabilityPoolOutputV0::DepositToProvide(o) => SpOutput::Deposit(Deposit {
            version: 0,
            action: "deposit_to_provide",
            account_id: o.account_id.to_string(),
            amount: o.provide_request.amount,
            min_fee_rate_ppb: Some(o.provide_request.min_fee_rate.0 as i64),
        }),
        StabilityPoolOutputV0::Transfer(t) => SpOutput::Transfer(transfer_info(0, t)),
    }
}

fn classify_v1(v1: &StabilityPoolOutputV1) -> SpOutput {
    match v1 {
        StabilityPoolOutputV1::DepositToSeek(o) => SpOutput::Deposit(Deposit {
            version: 1,
            action: "deposit_to_seek",
            account_id: o.account_id.to_string(),
            amount: o.seek_request.0,
            min_fee_rate_ppb: None,
        }),
        StabilityPoolOutputV1::DepositToProvide(o) => SpOutput::Deposit(Deposit {
            version: 1,
            action: "deposit_to_provide",
            account_id: o.account_id.to_string(),
            amount: o.provide_request.amount,
            min_fee_rate_ppb: Some(o.provide_request.min_fee_rate.0 as i64),
        }),
        StabilityPoolOutputV1::Transfer(t) => SpOutput::Transfer(transfer_info(1, t)),
        StabilityPoolOutputV1::DepositToBtcBalance(o) => SpOutput::Deposit(Deposit {
            version: 1,
            action: "deposit_to_btc_balance",
            account_id: o.account_id.to_string(),
            amount: o.seek_request.0,
            min_fee_rate_ppb: None,
        }),
    }
}

/// Records an observed `Account`'s multi-sig structure (`account_multisig`) and
/// its signing keys (`account_keys`), idempotently. Only callable where the
/// full `Account` is on the wire (withdrawal/unlock inputs, transfer senders).
async fn record_account(
    ctx: &mut ProcessCtx<'_>,
    fed: &[u8],
    session_index: i32,
    account: &Account,
) -> anyhow::Result<()> {
    let account_id = account.id().to_string();
    ctx.dbtx
        .execute(
            "INSERT INTO account_multisig VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT DO NOTHING",
            &[
                &fed,
                &account_id,
                &acc_type_str(account.acc_type()),
                &(account.threshold() as i64),
                &(account.pub_keys().count() as i64),
                &session_index,
            ],
        )
        .await?;
    for (idx, pubkey) in account.pub_keys().enumerate() {
        ctx.dbtx
            .execute(
                "INSERT INTO account_keys VALUES ($1, $2, $3, $4)
                 ON CONFLICT DO NOTHING",
                &[
                    &fed,
                    &account_id,
                    &(idx as i32),
                    &pubkey.serialize().to_vec(),
                ],
            )
            .await?;
    }
    Ok(())
}

/// Converts a `SystemTime` cycle vote into a UTC `NaiveDateTime`, or `None` if
/// it predates the unix epoch or overflows.
fn system_time_to_naive(time: std::time::SystemTime) -> Option<chrono::NaiveDateTime> {
    let secs = time.duration_since(UNIX_EPOCH).ok()?.as_secs();
    DateTime::from_timestamp(secs as i64, 0).map(|dt| dt.naive_utc())
}
