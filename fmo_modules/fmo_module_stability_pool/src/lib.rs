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
    FiatOrAll, StabilityPoolCommonGen, StabilityPoolConsensusItem, StabilityPoolInput,
    StabilityPoolInputV0, StabilityPoolOutput, StabilityPoolOutputV0, StabilityPoolOutputV1,
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
        1
    }

    fn migrations(&self) -> &'static [Migration] {
        &[Migration {
            sql: include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/schema/v0.sql")),
        }]
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

        let account_id = input_v0.account().id().to_string();

        ctx.dbtx
            .execute(
                "INSERT INTO withdrawals VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                 ON CONFLICT DO NOTHING",
                &[
                    &meta.federation_id.consensus_encode_to_vec(),
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

        let Some(deposit) = resolved else {
            warn!("Unknown stability_pool output version, storing JSON only: {sp_output:?}");
            return Ok(ProcessedItem {
                amount: None,
                details: serde_json::to_value(sp_output).ok(),
            });
        };

        ctx.dbtx
            .execute(
                "INSERT INTO deposits VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                 ON CONFLICT DO NOTHING",
                &[
                    &meta.federation_id.consensus_encode_to_vec(),
                    &meta.txid.consensus_encode_to_vec(),
                    &(meta.index as i32),
                    &(deposit.version as i16),
                    &deposit.action,
                    &deposit.account_id,
                    &(deposit.amount.msats as i64),
                    &deposit.min_fee_rate_ppb,
                ],
            )
            .await?;

        Ok(ProcessedItem {
            amount: Some(deposit.amount),
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

/// A stability-pool deposit output normalized for storage.
struct Deposit {
    version: u8,
    action: &'static str,
    account_id: String,
    amount: Amount,
    min_fee_rate_ppb: Option<i64>,
}

fn classify_v0(v0: &StabilityPoolOutputV0) -> Deposit {
    match v0 {
        StabilityPoolOutputV0::DepositToSeek(o) => Deposit {
            version: 0,
            action: "deposit_to_seek",
            account_id: o.account_id.to_string(),
            amount: o.seek_request.0,
            min_fee_rate_ppb: None,
        },
        StabilityPoolOutputV0::DepositToProvide(o) => Deposit {
            version: 0,
            action: "deposit_to_provide",
            account_id: o.account_id.to_string(),
            amount: o.provide_request.amount,
            min_fee_rate_ppb: Some(o.provide_request.min_fee_rate.0 as i64),
        },
        StabilityPoolOutputV0::Transfer(t) => Deposit {
            version: 0,
            action: "transfer",
            account_id: t.signed_request.details().from().id().to_string(),
            amount: Amount::ZERO,
            min_fee_rate_ppb: None,
        },
    }
}

fn classify_v1(v1: &StabilityPoolOutputV1) -> Deposit {
    match v1 {
        StabilityPoolOutputV1::DepositToSeek(o) => Deposit {
            version: 1,
            action: "deposit_to_seek",
            account_id: o.account_id.to_string(),
            amount: o.seek_request.0,
            min_fee_rate_ppb: None,
        },
        StabilityPoolOutputV1::DepositToProvide(o) => Deposit {
            version: 1,
            action: "deposit_to_provide",
            account_id: o.account_id.to_string(),
            amount: o.provide_request.amount,
            min_fee_rate_ppb: Some(o.provide_request.min_fee_rate.0 as i64),
        },
        StabilityPoolOutputV1::Transfer(t) => Deposit {
            version: 1,
            action: "transfer",
            account_id: t.signed_request.details().from().id().to_string(),
            amount: Amount::ZERO,
            min_fee_rate_ppb: None,
        },
        StabilityPoolOutputV1::DepositToBtcBalance(o) => Deposit {
            version: 1,
            action: "deposit_to_btc_balance",
            account_id: o.account_id.to_string(),
            amount: o.seek_request.0,
            min_fee_rate_ppb: None,
        },
    }
}

/// Converts a `SystemTime` cycle vote into a UTC `NaiveDateTime`, or `None` if
/// it predates the unix epoch or overflows.
fn system_time_to_naive(time: std::time::SystemTime) -> Option<chrono::NaiveDateTime> {
    let secs = time.duration_since(UNIX_EPOCH).ok()?.as_secs();
    DateTime::from_timestamp(secs as i64, 0).map(|dt| dt.naive_utc())
}
