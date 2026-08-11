use chrono::DateTime;
use fedimint_core::core::{Decoder, DynInput, DynModuleConsensusItem, DynOutput, ModuleKind};
use fedimint_core::encoding::Encodable;
use fedimint_core::module::CommonModuleInit;
use fedimint_core::Amount;
use fedimint_lnv2_common::{
    LightningCommonInit, LightningConsensusItem, LightningInput, LightningInputV0, LightningOutput,
    LightningOutputV0,
};
use fmo_core::module::{CiMeta, ItemMeta, Migration, ObserverModule, ProcessCtx, ProcessedItem};
use tracing::warn;

pub mod status;

/// Observer module for the next-generation fedimint `lnv2` lightning module:
/// tracks incoming/outgoing contracts and contributes the module's unix time
/// votes to the core session time estimation.
pub struct LnV2Observer;

const KIND: ModuleKind = ModuleKind::from_static_str("lnv2");

#[async_trait::async_trait]
impl ObserverModule for LnV2Observer {
    fn kind(&self) -> ModuleKind {
        KIND
    }

    fn decoder(&self) -> Decoder {
        LightningCommonInit::decoder()
    }

    fn version(&self) -> u32 {
        3
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
        let Some(lnv2_input) = input.as_any().downcast_ref::<LightningInput>() else {
            warn!("could not downcast lnv2 input (check decoders registry). {input:?}");
            return Ok(ProcessedItem::default());
        };

        let Some(input_v0) = lnv2_input.maybe_v0_ref() else {
            warn!("Unknown lnv2 input version, storing JSON only: {lnv2_input:?}");
            return Ok(ProcessedItem {
                amount: None,
                details: serde_json::to_value(lnv2_input).ok(),
            });
        };

        use fedimint_lnv2_common::OutgoingWitness;
        let (contract_type, variant, outpoint) = match input_v0 {
            LightningInputV0::Outgoing(outpoint, witness) => {
                let variant = match witness {
                    OutgoingWitness::Claim(_) => "claim",
                    OutgoingWitness::Refund | OutgoingWitness::Cancel(_) => "refund",
                };
                ("outgoing", variant, outpoint)
            }
            LightningInputV0::Incoming(outpoint, _agg_decryption_key) => {
                ("incoming", "claim", outpoint)
            }
        };

        ctx.dbtx
            .execute(
                "INSERT INTO input_outpoints VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT DO NOTHING",
                &[
                    &meta.federation_id.consensus_encode_to_vec(),
                    &meta.txid.consensus_encode_to_vec(),
                    &(meta.index as i32),
                    &contract_type,
                    &variant,
                    &outpoint.txid.consensus_encode_to_vec(),
                    &(outpoint.out_idx as i32),
                ],
            )
            .await?;

        // LNv2 inputs don't carry an amount in their consensus encoding, but
        // the funding contract does and is always processed before any claim
        // of it, so resolve the amount from our own contracts table. Marked
        // via details.amount_source so consumers can tell resolved amounts
        // from consensus-carried ones.
        let amount = ctx
            .dbtx
            .query_opt(
                "SELECT amount_msat FROM contracts
                 WHERE federation_id = $1 AND txid = $2 AND out_index = $3",
                &[
                    &meta.federation_id.consensus_encode_to_vec(),
                    &outpoint.txid.consensus_encode_to_vec(),
                    &(outpoint.out_idx as i32),
                ],
            )
            .await?
            .map(|row| Amount::from_msats(row.get::<_, i64>(0) as u64));

        crate::status::recompute_contract_status(
            ctx.dbtx,
            &meta.federation_id.consensus_encode_to_vec(),
            &outpoint.txid.consensus_encode_to_vec(),
        )
        .await?;

        let details = serde_json::to_value(lnv2_input).ok().map(|mut value| {
            if amount.is_some() {
                if let Some(object) = value.as_object_mut() {
                    object.insert("amount_source".to_owned(), "contract".into());
                }
            }
            value
        });

        Ok(ProcessedItem { amount, details })
    }

    async fn process_output(
        &self,
        ctx: &mut ProcessCtx<'_>,
        output: &DynOutput,
        meta: &ItemMeta,
    ) -> anyhow::Result<ProcessedItem> {
        let Some(lnv2_output) = output.as_any().downcast_ref::<LightningOutput>() else {
            warn!("could not downcast lnv2 output (check decoders registry). {output:?}");
            return Ok(ProcessedItem::default());
        };

        let Some(output_v0) = lnv2_output.maybe_v0_ref() else {
            warn!("Unknown lnv2 output version, storing JSON only: {lnv2_output:?}");
            return Ok(ProcessedItem {
                amount: None,
                details: serde_json::to_value(lnv2_output).ok(),
            });
        };

        let (contract_type, contract_id, amount) = match output_v0 {
            LightningOutputV0::Outgoing(contract) => {
                ("outgoing", contract.contract_id(), contract.amount)
            }
            LightningOutputV0::Incoming(contract) => (
                "incoming",
                contract.contract_id(),
                contract.commitment.amount,
            ),
        };

        ctx.dbtx
            .execute(
                "INSERT INTO contracts VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING",
                &[
                    &meta.federation_id.consensus_encode_to_vec(),
                    &contract_id.consensus_encode_to_vec(),
                    &contract_type,
                    &(amount.msats as i64),
                    &meta.txid.consensus_encode_to_vec(),
                    &(meta.index as i32),
                ],
            )
            .await?;

        Ok(ProcessedItem {
            amount: Some(amount),
            details: serde_json::to_value(lnv2_output).ok(),
        })
    }

    async fn process_ci(
        &self,
        ctx: &mut ProcessCtx<'_>,
        ci: &DynModuleConsensusItem,
        meta: &CiMeta,
    ) -> anyhow::Result<Option<serde_json::Value>> {
        let Some(lnv2_ci) = ci.as_any().downcast_ref::<LightningConsensusItem>() else {
            warn!("could not downcast lnv2 CI (check decoders registry). {ci:?}");
            return Ok(None);
        };

        match lnv2_ci {
            LightningConsensusItem::UnixTimeVote(unix_time_secs) => {
                if let Some(timestamp) =
                    DateTime::from_timestamp(*unix_time_secs as i64, 0).map(|dt| dt.naive_utc())
                {
                    ctx.record_session_time_vote(&KIND, meta.session_index, meta.peer, timestamp)
                        .await?;
                }
            }
            LightningConsensusItem::BlockCountVote(height_vote) => {
                if let Some(timestamp) = ctx.block_time(*height_vote as u32).await? {
                    ctx.record_session_time_vote(&KIND, meta.session_index, meta.peer, timestamp)
                        .await?;
                }
            }
            _ => {}
        }

        Ok(serde_json::to_value(lnv2_ci).ok())
    }
}
