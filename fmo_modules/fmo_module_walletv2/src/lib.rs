use fedimint_core::core::{Decoder, DynInput, DynModuleConsensusItem, DynOutput, ModuleKind};
use fedimint_core::encoding::Encodable;
use fedimint_core::module::CommonModuleInit;
use fedimint_core::Amount;
use fedimint_walletv2_common::{WalletCommonInit, WalletConsensusItem, WalletInput, WalletOutput};
use fmo_core::module::{CiMeta, ItemMeta, Migration, ObserverModule, ProcessCtx, ProcessedItem};
use tracing::warn;

/// Observer module for the next-generation fedimint `walletv2` (on-chain)
/// module: tracks peg-in claims (receives), peg-outs (sends) and block count
/// votes (which double as session time votes, analogous to `wallet`).
pub struct WalletV2Observer;

const KIND: ModuleKind = ModuleKind::from_static_str("walletv2");

#[async_trait::async_trait]
impl ObserverModule for WalletV2Observer {
    fn kind(&self) -> ModuleKind {
        KIND
    }

    fn decoder(&self) -> Decoder {
        WalletCommonInit::decoder()
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
        let Some(wallet_input) = input.as_any().downcast_ref::<WalletInput>() else {
            warn!("could not downcast walletv2 input (check decoders registry). {input:?}");
            return Ok(ProcessedItem::default());
        };

        let Some(input_v0) = wallet_input.maybe_v0_ref() else {
            warn!("Unknown walletv2 input version, storing JSON only: {wallet_input:?}");
            return Ok(ProcessedItem {
                amount: None,
                details: serde_json::to_value(wallet_input).ok(),
            });
        };

        ctx.dbtx
            .execute(
                "INSERT INTO receives VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING",
                &[
                    &meta.federation_id.consensus_encode_to_vec(),
                    &meta.txid.consensus_encode_to_vec(),
                    &(meta.index as i32),
                    &(input_v0.output_index as i64),
                    &input_v0.tweak.serialize().to_vec(),
                    &((input_v0.fee.to_sat() * 1000) as i64),
                ],
            )
            .await?;

        // The claimed value is the tracked on-chain output's value minus the
        // fee; the output value is only known to the federation's wallet, so
        // no amount can be attributed from the input alone.
        Ok(ProcessedItem {
            amount: None,
            details: serde_json::to_value(wallet_input).ok(),
        })
    }

    async fn process_output(
        &self,
        ctx: &mut ProcessCtx<'_>,
        output: &DynOutput,
        meta: &ItemMeta,
    ) -> anyhow::Result<ProcessedItem> {
        let Some(wallet_output) = output.as_any().downcast_ref::<WalletOutput>() else {
            warn!("could not downcast walletv2 output (check decoders registry). {output:?}");
            return Ok(ProcessedItem::default());
        };

        let Some(output_v0) = wallet_output.maybe_v0_ref() else {
            warn!("Unknown walletv2 output version, storing JSON only: {wallet_output:?}");
            return Ok(ProcessedItem {
                amount: None,
                details: serde_json::to_value(wallet_output).ok(),
            });
        };

        // Unknown destination script variants are stored with a NULL address
        // instead of failing; the raw script data is still in the JSON details.
        let address = output_v0.destination.script_pubkey().and_then(|script| {
            bitcoin::Address::from_script(&script, bitcoin::Network::Bitcoin)
                .map(|address| address.to_string())
                .ok()
        });

        let value_msat = output_v0.value.to_sat() * 1000;
        let fee_msat = output_v0.fee.to_sat() * 1000;

        ctx.dbtx
            .execute(
                "INSERT INTO sends VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING",
                &[
                    &meta.federation_id.consensus_encode_to_vec(),
                    &meta.txid.consensus_encode_to_vec(),
                    &(meta.index as i32),
                    &address,
                    &(value_msat as i64),
                    &(fee_msat as i64),
                ],
            )
            .await?;

        // The fedimint transaction is debited value + fee.
        Ok(ProcessedItem {
            amount: Some(Amount::from_msats(value_msat + fee_msat)),
            details: serde_json::to_value(wallet_output).ok(),
        })
    }

    async fn process_ci(
        &self,
        ctx: &mut ProcessCtx<'_>,
        ci: &DynModuleConsensusItem,
        meta: &CiMeta,
    ) -> anyhow::Result<Option<serde_json::Value>> {
        let Some(wallet_ci) = ci.as_any().downcast_ref::<WalletConsensusItem>() else {
            warn!("could not downcast walletv2 CI (check decoders registry). {ci:?}");
            return Ok(None);
        };

        if let WalletConsensusItem::BlockCount(height_vote) = wallet_ci {
            ctx.dbtx
                .execute(
                    "INSERT INTO block_height_votes VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING",
                    &[
                        &meta.federation_id.consensus_encode_to_vec(),
                        &(meta.session_index as i32),
                        &(meta.item_index as i32),
                        &(meta.peer.to_usize() as i32),
                        &(*height_vote as i32),
                    ],
                )
                .await?;

            // Height votes are our best estimate of when a session happened;
            // contribute them to the core session time votes.
            if let Some(timestamp) = ctx.services.block_time(*height_vote as u32).await? {
                ctx.record_session_time_vote(&KIND, meta.session_index, meta.peer, timestamp)
                    .await?;
            }
        }

        Ok(serde_json::to_value(wallet_ci).ok())
    }
}
