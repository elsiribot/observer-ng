use fedimint_core::core::{Decoder, DynInput, DynModuleConsensusItem, DynOutput, ModuleKind};
use fedimint_core::encoding::Encodable;
use fedimint_core::module::CommonModuleInit;
use fedimint_mintv2_common::{MintCommonInit, MintConsensusItem, MintInput, MintOutput};
use fmo_core::module::{CiMeta, ItemMeta, Migration, ObserverModule, ProcessCtx, ProcessedItem};
use tracing::warn;

/// Observer module for the next-generation fedimint `mintv2` (e-cash) module:
/// records amounts of issued and spent notes and tracks spent note nonces for
/// e-cash analytics such as spend lookups.
pub struct MintV2Observer;

const KIND: ModuleKind = ModuleKind::from_static_str("mintv2");

#[async_trait::async_trait]
impl ObserverModule for MintV2Observer {
    fn kind(&self) -> ModuleKind {
        KIND
    }

    fn decoder(&self) -> Decoder {
        MintCommonInit::decoder()
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
        let Some(mint_input) = input.as_any().downcast_ref::<MintInput>() else {
            warn!("could not downcast mintv2 input (check decoders registry). {input:?}");
            return Ok(ProcessedItem::default());
        };

        let Some(input_v0) = mint_input.maybe_v0_ref() else {
            warn!("Unknown mintv2 input version, storing JSON only: {mint_input:?}");
            return Ok(ProcessedItem {
                amount: None,
                details: serde_json::to_value(mint_input).ok(),
            });
        };

        let note = &input_v0.note;
        let amount = note.amount();

        ctx.dbtx
            .execute(
                "INSERT INTO spent_nonces VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING",
                &[
                    &meta.federation_id.consensus_encode_to_vec(),
                    &note.nonce.to_string(),
                    &(note.denomination.0 as i16),
                    &(amount.msats as i64),
                    &meta.txid.consensus_encode_to_vec(),
                    &(meta.index as i32),
                ],
            )
            .await?;

        Ok(ProcessedItem {
            amount: Some(amount),
            details: serde_json::to_value(mint_input).ok(),
        })
    }

    async fn process_output(
        &self,
        _ctx: &mut ProcessCtx<'_>,
        output: &DynOutput,
        _meta: &ItemMeta,
    ) -> anyhow::Result<ProcessedItem> {
        let Some(mint_output) = output.as_any().downcast_ref::<MintOutput>() else {
            warn!("could not downcast mintv2 output (check decoders registry). {output:?}");
            return Ok(ProcessedItem::default());
        };

        let amount = mint_output
            .maybe_v0_ref()
            .map(|output_v0| output_v0.amount());
        if amount.is_none() {
            warn!("Unknown mintv2 output version, storing JSON only: {mint_output:?}");
        }

        Ok(ProcessedItem {
            amount,
            details: serde_json::to_value(mint_output).ok(),
        })
    }

    async fn process_ci(
        &self,
        _ctx: &mut ProcessCtx<'_>,
        ci: &DynModuleConsensusItem,
        _meta: &CiMeta,
    ) -> anyhow::Result<Option<serde_json::Value>> {
        let Some(mint_ci) = ci.as_any().downcast_ref::<MintConsensusItem>() else {
            warn!("could not downcast mintv2 CI (check decoders registry). {ci:?}");
            return Ok(None);
        };

        // mintv2 defines no consensus items today; any observed item is an
        // unknown future variant and only stored as JSON.
        Ok(serde_json::to_value(mint_ci).ok())
    }
}
