use std::str::FromStr;

use anyhow::{bail, Context};
use axum::extract::{Path, State};
use axum::routing::get;
use axum::{Json, Router};
use bitcoin::hashes::Hash;
use bitcoin::{Address, OutPoint, Txid};
use fedimint_core::config::FederationId;
use fedimint_core::core::{Decoder, DynInput, DynModuleConsensusItem, DynOutput, ModuleKind};
use fedimint_core::encoding::Encodable;
use fedimint_core::module::CommonModuleInit;
use fedimint_core::util::backoff_util::background_backoff;
use fedimint_core::util::retry;
use fedimint_core::Amount;
use fedimint_wallet_common::{
    WalletCommonInit, WalletConsensusItem, WalletInput, WalletOutput, WalletOutputV0,
};
use fmo_api_types::FederationUtxo;
use fmo_core::api::ModuleApiState;
use fmo_core::module::{CiMeta, ItemMeta, Migration, ObserverModule, ProcessCtx, ProcessedItem};
use fmo_core::query::query;
use postgres_from_row::FromRow;
use tracing::warn;

/// Observer module for the fedimint `wallet` (on-chain) module: tracks
/// peg-ins, withdrawals including their on-chain transactions, the resulting
/// UTXO set and block height votes (which double as session time votes).
pub struct WalletObserver;

const KIND: ModuleKind = ModuleKind::from_static_str("wallet");

#[async_trait::async_trait]
impl ObserverModule for WalletObserver {
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

    fn matviews(&self) -> &'static [&'static str] {
        &["fmo_wallet.utxos"]
    }

    async fn process_input(
        &self,
        ctx: &mut ProcessCtx<'_>,
        input: &DynInput,
        meta: &ItemMeta,
    ) -> anyhow::Result<ProcessedItem> {
        let Some(wallet_input) = input.as_any().downcast_ref::<WalletInput>() else {
            warn!("could not downcast wallet input (check decoders registry). {input:?}");
            return Ok(ProcessedItem::default());
        };

        let (outpoint, script, amount) = match wallet_input {
            WalletInput::V0(peg_in_proof) => {
                let outpoint = peg_in_proof.outpoint();
                let script = peg_in_proof.tx_output().script_pubkey;
                let amount = peg_in_proof.tx_output().value;
                (outpoint, script, amount)
            }
            WalletInput::V1(input_v1) => (
                input_v1.outpoint,
                input_v1.tx_out.script_pubkey.clone(),
                input_v1.tx_out.value,
            ),
            unknown => {
                warn!("Unknown wallet input version, storing JSON only: {unknown:?}");
                return Ok(ProcessedItem {
                    amount: None,
                    details: serde_json::to_value(wallet_input).ok(),
                });
            }
        };

        let amount_msat = amount.to_sat() * 1000;
        let address = bitcoin::Address::from_script(&script, bitcoin::Network::Bitcoin)
            .context("Invalid peg-in address")?;

        ctx.dbtx
            .execute(
                "INSERT INTO peg_ins VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT DO NOTHING",
                &[
                    &outpoint.txid[..].to_owned(),
                    &(outpoint.vout as i32),
                    &address.to_string(),
                    &(amount_msat as i64),
                    &meta.federation_id.consensus_encode_to_vec(),
                    &meta.txid.consensus_encode_to_vec(),
                    &(meta.index as i32),
                ],
            )
            .await?;

        Ok(ProcessedItem {
            amount: Some(Amount::from_msats(amount_msat)),
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
            warn!("could not downcast wallet output (check decoders registry). {output:?}");
            return Ok(ProcessedItem::default());
        };

        let Some(wallet_output_v0) = wallet_output.maybe_v0_ref() else {
            warn!("Unknown wallet output version, storing JSON only: {wallet_output:?}");
            return Ok(ProcessedItem {
                amount: None,
                details: serde_json::to_value(wallet_output).ok(),
            });
        };

        let amount_msat = wallet_output_v0.amount().to_sat() * 1000;

        match wallet_output_v0 {
            WalletOutputV0::PegOut(peg_out) => {
                let withdrawal_address = peg_out.recipient.clone().assume_checked();
                ctx.dbtx
                    .execute(
                        "INSERT INTO withdrawal_addresses VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING",
                        &[
                            &withdrawal_address.to_string(),
                            &meta.federation_id.consensus_encode_to_vec(),
                            &(meta.session_index as i32),
                            &(meta.item_index as i32),
                            &meta.txid.consensus_encode_to_vec(),
                            &(meta.index as i32),
                        ],
                    )
                    .await?;
            }
            WalletOutputV0::Rbf(_) => {
                // Unlike the pre-modularization code this no longer takes the
                // whole observer down; only the wallet module stalls on this
                // federation until the situation is resolved manually.
                // For context, see: https://github.com/fedimint/fedimint/pull/5496
                bail!(
                    "Discovered an RBF wallet output in federation {}. \
                     Wallet processing is halted for this federation; please \
                     alert the federation's guardians (chat.fedimint.org).",
                    meta.federation_id
                );
            }
        }

        Ok(ProcessedItem {
            amount: Some(Amount::from_msats(amount_msat)),
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
            warn!("could not downcast wallet CI (check decoders registry). {ci:?}");
            return Ok(None);
        };

        let details = serde_json::to_value(wallet_ci).ok();

        match wallet_ci {
            WalletConsensusItem::BlockCount(height_vote) => {
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

                // Height votes are our best estimate of when a session
                // happened; contribute them to the core session time votes.
                if let Some(timestamp) = ctx.services.block_time(*height_vote).await? {
                    ctx.record_session_time_vote(&KIND, meta.session_index, meta.peer, timestamp)
                        .await?;
                }
            }
            WalletConsensusItem::PegOutSignature(peg_out_sig) => {
                self.process_peg_out_signature(ctx, meta, peg_out_sig)
                    .await?;
            }
            _ => {
                // other WalletConsensusItems are not needed yet
            }
        }

        Ok(details)
    }

    fn api_router(&self) -> Option<Router<ModuleApiState>> {
        Some(Router::new().route("/utxos", get(get_federation_utxos)))
    }
}

impl WalletObserver {
    async fn process_peg_out_signature(
        &self,
        ctx: &mut ProcessCtx<'_>,
        meta: &CiMeta,
        peg_out_sig: &fedimint_wallet_common::PegOutSignatureItem,
    ) -> anyhow::Result<()> {
        let peg_out_txid = peg_out_sig.txid.to_string();
        let peg_out_txid_encoded = fedimint_core::TransactionId::from_str(peg_out_txid.as_str())
            .expect("Invalid on chain txid")
            .consensus_encode_to_vec();

        ctx.dbtx
            .execute(
                "INSERT INTO withdrawal_transactions VALUES ($1, $2) ON CONFLICT DO NOTHING",
                &[
                    &peg_out_txid_encoded,
                    &meta.federation_id.consensus_encode_to_vec(),
                ],
            )
            .await?;

        ctx.dbtx
            .execute(
                "INSERT INTO withdrawal_signatures VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
                &[
                    &peg_out_txid_encoded,
                    &(meta.session_index as i32),
                    &(meta.item_index as i32),
                    &(meta.peer.to_usize() as i32),
                ],
            )
            .await?;

        let num_sigs = ctx
            .dbtx
            .query_one(
                "
                SELECT COUNT(peer_id)::INT num_sigs
                FROM withdrawal_signatures
                WHERE on_chain_txid = $1
                GROUP BY on_chain_txid
                ",
                &[&peg_out_txid_encoded],
            )
            .await?
            .get::<_, i32>("num_sigs") as usize;

        // 3n + 1 <= num_peers
        // n <= (num_peers - 1) / 3
        // threshold = num_peers - floor((num_peers - 1) / 3)
        let threshold = {
            let num_peers = meta.peer_count;
            num_peers - (num_peers - 1) / 3
        };

        if num_sigs < threshold {
            return Ok(());
        }

        // at this point, the transaction reached threshold and should broadcast

        let esplora_txid = esplora_client::Txid::from_str(peg_out_txid.as_str())
            .expect("Couldn't create esplora txid");

        let client = ctx.services.esplora()?;

        let fetched_tx = retry(
            "fetching tx from esplora".to_string(),
            background_backoff(),
            || async {
                client.get_tx_no_opt(&esplora_txid).await.map_err(|e| {
                    warn!("failed to fetch tx: {e:?}");
                    anyhow::anyhow!("failed fetching tx from esplora")
                })
            },
        )
        .await
        .expect("Reached usize::MAX retries");

        for input in fetched_tx.input {
            let prev_out_txid = fedimint_core::TransactionId::from_str(
                input.previous_output.txid.to_string().as_str(),
            )
            .expect("Invalid txid")
            .consensus_encode_to_vec();

            ctx.dbtx
                .execute(
                    "INSERT INTO withdrawal_transaction_inputs VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                    &[
                        &prev_out_txid,
                        &(input.previous_output.vout as i32),
                        &peg_out_txid_encoded,
                    ],
                )
                .await?;
        }

        for (out_idx, output) in fetched_tx.output.iter().enumerate() {
            let address = bitcoin::Address::from_script(
                bitcoin::Script::from_bytes(output.script_pubkey.as_bytes()),
                bitcoin::Network::Bitcoin,
            )
            .expect("Invalid bitcoin address");

            ctx.dbtx
                .execute(
                    "INSERT INTO withdrawal_transaction_outputs VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
                    &[
                        &peg_out_txid_encoded,
                        &(out_idx as i32),
                        &address.to_string(),
                        &((output.value.to_sat() as i64) * 1000),
                    ],
                )
                .await?;

            // update federation_txid if we found a matching withdrawal address
            ctx.dbtx
                .execute(
                    "
                    UPDATE withdrawal_transactions
                    SET federation_txid = (
                        SELECT txid
                        FROM withdrawal_addresses wwa
                        WHERE address = $1
                          AND NOT EXISTS (
                            SELECT *
                            FROM withdrawal_transactions wwt
                            WHERE wwa.txid = wwt.federation_txid
                          )
                        -- if address reuse, assume earliest withdrawal request first
                        ORDER BY session_index, item_index
                        LIMIT 1
                    )
                    WHERE on_chain_txid = $2
                      AND federation_txid IS NULL
                    ",
                    &[&address.to_string(), &peg_out_txid_encoded],
                )
                .await?;
        }

        Ok(())
    }
}

async fn get_federation_utxos(
    Path(federation_id): Path<FederationId>,
    State(state): State<ModuleApiState>,
) -> fmo_core::error::Result<Json<Vec<FederationUtxo>>> {
    Ok(Json(federation_utxos(&state, federation_id).await?))
}

async fn federation_utxos(
    state: &ModuleApiState,
    federation_id: FederationId,
) -> anyhow::Result<Vec<FederationUtxo>> {
    #[derive(Debug, FromRow)]
    struct FederationUtxoRaw {
        on_chain_txid: Vec<u8>,
        on_chain_vout: i32,
        address: String,
        amount_msat: i64,
    }

    query::<FederationUtxoRaw>(
        &state.pool.get().await?,
        // language=postgresql
        "SELECT on_chain_txid, on_chain_vout, address, amount_msat FROM fmo_wallet.utxos WHERE federation_id = $1 ORDER BY amount_msat DESC",
        &[&federation_id.consensus_encode_to_vec()],
    )
    .await?
    .into_iter()
    .map(|utxo| {
        Result::<_, anyhow::Error>::Ok(FederationUtxo {
            address: Address::from_str(&utxo.address)?,
            out_point: OutPoint {
                txid: Txid::from_slice(&utxo.on_chain_txid)?,
                vout: utxo.on_chain_vout.try_into()?,
            },
            amount: Amount::from_msats(utxo.amount_msat.try_into()?),
        })
    })
    .collect()
}
