use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use bitcoin::hashes::Hash;
use bitcoin::Txid;
use fedimint_core::core::{Decoder, DynInput, DynModuleConsensusItem, DynOutput, ModuleKind};
use fedimint_core::encoding::Encodable;
use fedimint_core::module::CommonModuleInit;
use fedimint_core::Amount;
use fedimint_walletv2_common::{WalletCommonInit, WalletConsensusItem, WalletInput, WalletOutput};
use fmo_core::module::{
    CiMeta, ItemMeta, Migration, ModuleTaskCtx, ObserverModule, ProcessCtx, ProcessedItem,
};
use fmo_core::query::query;
use postgres_from_row::FromRow;
use tracing::{debug, warn};

/// Observer module for the next-generation fedimint `walletv2` (on-chain)
/// module: tracks peg-in claims (receives), peg-outs (sends), block count
/// votes (which double as session time votes, analogous to `wallet`) and the
/// federation's single consolidated on-chain UTXO for exact balance tracking.
pub struct WalletV2Observer;

const KIND: ModuleKind = ModuleKind::from_static_str("walletv2");

/// How many unresolved wallet-tx txids to fetch from esplora per resolver
/// cycle. Bounds the one-time historical backfill (hundreds per federation,
/// thousands fleet-wide) so it doesn't hammer the explorer; distinct txids are
/// fetched concurrently up to this many at a time.
const RESOLVE_BATCH_SIZE: i64 = 20;

/// Pause between resolver cycles once all currently-unresolved rows have been
/// handled (or a cycle errored). Live transitions are rare, so a slow poll is
/// plenty; the backfill drains `RESOLVE_BATCH_SIZE` txids each tick.
fn resolver_idle_sleep() -> Duration {
    Duration::from_secs(30)
}

#[async_trait::async_trait]
impl ObserverModule for WalletV2Observer {
    fn kind(&self) -> ModuleKind {
        KIND
    }

    fn decoder(&self) -> Decoder {
        WalletCommonInit::decoder()
    }

    fn version(&self) -> u32 {
        // v2: added the `wallet_utxos` table (schema/v1.sql) and the
        // Signatures consensus-item handling that populates it. Bumping forces
        // a schema rebuild + full replay so historical transitions are
        // recorded, then the background resolver backfills their UTXO values.
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
                if let Some(timestamp) = ctx.block_time(*height_vote as u32).await? {
                    ctx.record_session_time_vote(&KIND, meta.session_index, meta.peer, timestamp)
                        .await?;
                }
            }
            WalletConsensusItem::Signatures(txid, _signatures) => {
                // Every wallet-tx transition (deposit or withdrawal) is
                // announced here, once per signing peer, carrying the on-chain
                // txid of the transaction that creates the new consolidated
                // UTXO (at vout 0). Record the txid with a NULL value; the
                // background resolver task looks up the value on an explorer —
                // we must NOT do network I/O inside this processing
                // transaction (it holds DB locks; see the block_times pattern).
                ctx.dbtx
                    .execute(
                        "INSERT INTO wallet_utxos (federation_id, session_index, item_index, txid)
                         VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
                        &[
                            &meta.federation_id.consensus_encode_to_vec(),
                            &(meta.session_index as i32),
                            &(meta.item_index as i32),
                            // Internal byte order; reconstructed via
                            // Txid::from_slice in the resolver / API.
                            &txid.to_byte_array().to_vec(),
                        ],
                    )
                    .await?;
            }
            _ => {
                // Feerate votes and unknown variants are not needed yet; the
                // raw JSON is still returned below.
            }
        }

        Ok(serde_json::to_value(wallet_ci).ok())
    }

    /// Resolves the on-chain values of recorded wallet-tx txids from an
    /// explorer, out-of-band from the processing transaction. This also
    /// throttles the one-time historical backfill triggered by the version
    /// bump.
    async fn run_federation_task(self: Arc<Self>, ctx: ModuleTaskCtx) {
        let federation_id = ctx.federation_id;
        loop {
            match resolve_utxo_values(&ctx).await {
                Ok(resolved) if resolved > 0 => {
                    debug!("walletv2 resolved {resolved} UTXO value(s) for {federation_id}");
                    // More may remain; loop again promptly to drain the
                    // backfill without a full idle sleep.
                    continue;
                }
                Ok(_) => {}
                Err(e) => warn!("walletv2 UTXO resolver for {federation_id} failed: {e:?}"),
            }
            tokio::time::sleep(resolver_idle_sleep()).await;
        }
    }
}

/// One resolver cycle: fetch up to `RESOLVE_BATCH_SIZE` distinct still-
/// unresolved txids for this federation from the explorer and fill in their
/// UTXO value (output at vout 0). Returns how many rows were updated. All
/// network I/O happens here, on a background task holding no processing
/// transaction; a short-lived pool connection is only taken to read the work
/// list and to write results back.
async fn resolve_utxo_values(ctx: &ModuleTaskCtx) -> anyhow::Result<u64> {
    let fed = ctx.federation_id.consensus_encode_to_vec();

    // ---- read phase: which distinct txids still need resolving ----
    #[derive(FromRow)]
    struct UnresolvedTxid {
        txid: Vec<u8>,
    }

    let unresolved = {
        let conn = ctx.pool.get().await?;
        query::<UnresolvedTxid>(
            &conn,
            "SELECT DISTINCT txid FROM fmo_walletv2.wallet_utxos
             WHERE federation_id = $1 AND utxo_value_msat IS NULL
             LIMIT $2",
            &[&fed, &RESOLVE_BATCH_SIZE],
        )
        .await?
    };

    if unresolved.is_empty() {
        return Ok(0);
    }

    let client = ctx.services.esplora()?;

    // ---- network phase: no pool connection held ----
    let fetched = futures::future::join_all(unresolved.into_iter().map(|row| {
        let client = client.clone();
        async move {
            let resolved = resolve_one(&client, &row.txid).await;
            (row.txid, resolved)
        }
    }))
    .await;

    // ---- write phase: short-lived connection ----
    let conn = ctx.pool.get().await?;
    let mut updated = 0u64;
    for (txid, resolved) in fetched {
        let (value_msat, address) = match resolved {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    "walletv2: failed to resolve txid {} for {}: {e:?}",
                    hex_display(&txid),
                    ctx.federation_id
                );
                continue;
            }
        };

        // Update every row sharing this txid (peers announce it separately),
        // so one explorer fetch resolves them all.
        updated += conn
            .execute(
                "UPDATE fmo_walletv2.wallet_utxos
                 SET utxo_value_msat = $3, address = $4, resolved_at = NOW()::timestamp
                 WHERE federation_id = $1 AND txid = $2 AND utxo_value_msat IS NULL",
                &[&fed, &txid, &value_msat, &address],
            )
            .await?;
    }

    Ok(updated)
}

/// Fetches one transaction and returns `(value_msat, address)` of its output
/// at vout 0 — the new consolidated federation UTXO.
async fn resolve_one(
    client: &esplora_client::AsyncClient,
    txid_bytes: &[u8],
) -> anyhow::Result<(i64, Option<String>)> {
    let txid = Txid::from_slice(txid_bytes).context("invalid stored txid")?;
    let esplora_txid =
        esplora_client::Txid::from_str(&txid.to_string()).context("invalid esplora txid")?;

    let tx = client
        .get_tx_no_opt(&esplora_txid)
        .await
        .context("fetching tx from esplora")?;

    let utxo = tx.output.first().context("wallet tx has no outputs")?;
    let value_msat = (utxo.value.to_sat() * 1000) as i64;
    let address = bitcoin::Address::from_script(
        bitcoin::Script::from_bytes(utxo.script_pubkey.as_bytes()),
        bitcoin::Network::Bitcoin,
    )
    .map(|address| address.to_string())
    .ok();

    Ok((value_msat, address))
}

fn hex_display(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
