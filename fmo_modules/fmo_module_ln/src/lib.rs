use fedimint_core::core::{Decoder, DynInput, DynModuleConsensusItem, DynOutput, ModuleKind};
use fedimint_core::encoding::Encodable;
use fedimint_core::module::CommonModuleInit;
use fedimint_core::Amount;
use fedimint_ln_common::contracts::{Contract, IdentifiableContract};
use fedimint_ln_common::{
    LightningCommonInit, LightningConsensusItem, LightningInput, LightningOutput,
    LightningOutputV0,
};
use fmo_core::module::{
    CiMeta, ItemMeta, Migration, ObserverModule, ProcessCtx, ProcessedItem,
};
use tracing::warn;

/// Observer module for the fedimint `ln` (lightning v1) module: tracks
/// incoming/outgoing contracts and how transaction inputs/outputs interact
/// with them.
pub struct LnObserver;

const KIND: ModuleKind = ModuleKind::from_static_str("ln");

#[async_trait::async_trait]
impl ObserverModule for LnObserver {
    fn kind(&self) -> ModuleKind {
        KIND
    }

    fn decoder(&self) -> Decoder {
        LightningCommonInit::decoder()
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
        let Some(ln_input) = input.as_any().downcast_ref::<LightningInput>() else {
            warn!("could not downcast ln input (check decoders registry). {input:?}");
            return Ok(ProcessedItem::default());
        };

        let Some(input_v0) = ln_input.maybe_v0_ref() else {
            warn!("Unknown ln input version, storing JSON only: {ln_input:?}");
            return Ok(ProcessedItem {
                amount: None,
                details: serde_json::to_value(ln_input).ok(),
            });
        };

        ctx.dbtx
            .execute(
                "INSERT INTO input_contracts VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
                &[
                    &meta.federation_id.consensus_encode_to_vec(),
                    &meta.txid.consensus_encode_to_vec(),
                    &(meta.index as i32),
                    &input_v0.contract_id.consensus_encode_to_vec(),
                ],
            )
            .await?;

        Ok(ProcessedItem {
            amount: Some(input_v0.amount),
            details: serde_json::to_value(ln_input).ok(),
        })
    }

    async fn process_output(
        &self,
        ctx: &mut ProcessCtx<'_>,
        output: &DynOutput,
        meta: &ItemMeta,
    ) -> anyhow::Result<ProcessedItem> {
        let Some(ln_output) = output.as_any().downcast_ref::<LightningOutput>() else {
            warn!("could not downcast ln output (check decoders registry). {output:?}");
            return Ok(ProcessedItem::default());
        };

        let Some(output_v0) = ln_output.maybe_v0_ref() else {
            warn!("Unknown ln output version, storing JSON only: {ln_output:?}");
            return Ok(ProcessedItem {
                amount: None,
                details: serde_json::to_value(ln_output).ok(),
            });
        };

        let (amount, interaction_kind, contract_id) = match output_v0 {
            LightningOutputV0::Contract(contract) => {
                let contract_id = contract.contract.contract_id();
                let (contract_type, payment_hash) = match &contract.contract {
                    Contract::Incoming(incoming) => ("incoming", incoming.hash),
                    Contract::Outgoing(outgoing) => ("outgoing", outgoing.hash),
                };

                ctx.dbtx
                    .execute(
                        "INSERT INTO contracts VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
                        &[
                            &meta.federation_id.consensus_encode_to_vec(),
                            &contract_id.consensus_encode_to_vec(),
                            &contract_type,
                            &payment_hash.consensus_encode_to_vec(),
                        ],
                    )
                    .await?;

                (contract.amount, "fund", contract_id)
            }
            LightningOutputV0::Offer(offer) => {
                // For incoming contracts payment hash == contract id
                (Amount::ZERO, "offer", offer.hash.into())
            }
            LightningOutputV0::CancelOutgoing { contract, .. } => {
                (Amount::ZERO, "cancel", *contract)
            }
        };

        ctx.dbtx
            .execute(
                "INSERT INTO output_contracts VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING",
                &[
                    &meta.federation_id.consensus_encode_to_vec(),
                    &meta.txid.consensus_encode_to_vec(),
                    &(meta.index as i32),
                    &interaction_kind,
                    &contract_id.consensus_encode_to_vec(),
                ],
            )
            .await?;

        Ok(ProcessedItem {
            amount: Some(amount),
            details: serde_json::to_value(ln_output).ok(),
        })
    }

    async fn process_ci(
        &self,
        _ctx: &mut ProcessCtx<'_>,
        ci: &DynModuleConsensusItem,
        _meta: &CiMeta,
    ) -> anyhow::Result<Option<serde_json::Value>> {
        let Some(ln_ci) = ci.as_any().downcast_ref::<LightningConsensusItem>() else {
            warn!("could not downcast ln CI (check decoders registry). {ci:?}");
            return Ok(None);
        };

        Ok(serde_json::to_value(ln_ci).ok())
    }
}
