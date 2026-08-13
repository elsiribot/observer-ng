use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use axum::extract::{Path, State};
use axum::routing::get;
use axum::{Json, Router};
use bitcoin::hashes::Hash;
use bitcoin::{Address, OutPoint, Txid};
use fedimint_api_client::api::{DynGlobalApi, FederationApiExt};
use fedimint_core::config::FederationId;
use fedimint_core::core::{Decoder, DynInput, DynModuleConsensusItem, DynOutput, ModuleKind};
use fedimint_core::encoding::Encodable;
use fedimint_core::module::{ApiRequestErased, CommonModuleInit};
use fedimint_core::Amount;
use fedimint_walletv2_common::endpoint_constants::FEDERATION_WALLET_ENDPOINT;
use fedimint_walletv2_common::{
    FederationWallet, WalletCommonInit, WalletConsensusItem, WalletInput, WalletOutput,
};
use fmo_api_types::FederationUtxo;
use fmo_core::api::ModuleApiState;
use fmo_core::module::{
    CiMeta, ItemMeta, Migration, ModuleTaskCtx, ObserverModule, ProcessCtx, ProcessedItem,
};
use fmo_core::query::{query, query_value};
use postgres_from_row::FromRow;
use tracing::{debug, info, warn};

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

/// How often the verification poll asks the guardians for their live
/// `federation_wallet` and compares it to our derived balance.
const VERIFY_POLL_INTERVAL: Duration = Duration::from_secs(300);

/// How long a value mismatch must persist before we `warn!`. Our derived
/// balance legitimately lags the live one briefly (a new transition, or the
/// resolver / esplora catching up); only a mismatch sustained past this is a
/// real problem worth surfacing.
const VERIFY_SUSTAINED_DIVERGENCE: Duration = Duration::from_secs(30 * 60);

/// Values must match exactly once caught up; any nonzero difference sustained
/// past `VERIFY_SUSTAINED_DIVERGENCE` is flagged.
const VERIFY_TOLERANCE_MSAT: i64 = 0;

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

    /// Runs the two walletv2 background loops concurrently on this single
    /// per-(module, federation) task:
    /// - the UTXO-value resolver (fills in exact on-chain balances), and
    /// - the `federation_wallet` verification poll (a monitoring cross-check
    ///   against the guardians' authoritative live UTXO).
    async fn run_federation_task(self: Arc<Self>, ctx: ModuleTaskCtx) {
        tokio::join!(run_resolver_loop(&ctx), run_verification_loop(&ctx));
    }

    fn api_router(&self) -> Option<Router<ModuleApiState>> {
        Some(Router::new().route("/utxos", get(get_federation_utxos)))
    }
}

/// Resolver loop: repeatedly drains unresolved wallet-tx txids from the
/// explorer, out-of-band from the processing transaction. Also throttles the
/// one-time historical backfill triggered by the version bump.
async fn run_resolver_loop(ctx: &ModuleTaskCtx) {
    let federation_id = ctx.federation_id;
    loop {
        match resolve_utxo_values(ctx).await {
            Ok(resolved) if resolved > 0 => {
                debug!("walletv2 resolved {resolved} UTXO value(s) for {federation_id}");
                // More may remain; loop again promptly to drain the backfill
                // without a full idle sleep.
                continue;
            }
            Ok(_) => {}
            Err(e) => warn!("walletv2 UTXO resolver for {federation_id} failed: {e:?}"),
        }
        tokio::time::sleep(resolver_idle_sleep()).await;
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
            Ok(Some(v)) => v,
            // A freshly-signed live transition can be invisible to the explorer
            // for a few minutes until it propagates/confirms; that's expected,
            // so log at debug and just retry next cycle. (Historical backfill
            // txids are all long confirmed, so this only affects live tips.)
            Ok(None) => {
                debug!(
                    "walletv2: txid {} not yet visible on explorer for {}, will retry",
                    hex_display(&txid),
                    ctx.federation_id
                );
                continue;
            }
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
/// at vout 0 — the new consolidated federation UTXO. Returns `Ok(None)` when
/// the explorer doesn't know the tx yet (not an error: a just-signed live tx
/// hasn't propagated/confirmed); `Err` only for genuine transport failures.
async fn resolve_one(
    client: &esplora_client::AsyncClient,
    txid_bytes: &[u8],
) -> anyhow::Result<Option<(i64, Option<String>)>> {
    let txid = Txid::from_slice(txid_bytes).context("invalid stored txid")?;
    let esplora_txid =
        esplora_client::Txid::from_str(&txid.to_string()).context("invalid esplora txid")?;

    // `get_tx` returns `None` (rather than erroring) when the tx is unknown to
    // the explorer, letting us treat "not yet visible" separately from a
    // transport error.
    let Some(tx) = client
        .get_tx(&esplora_txid)
        .await
        .context("fetching tx from esplora")?
    else {
        return Ok(None);
    };

    let utxo = tx.output.first().context("wallet tx has no outputs")?;
    let value_msat = (utxo.value.to_sat() * 1000) as i64;
    let address = bitcoin::Address::from_script(
        bitcoin::Script::from_bytes(utxo.script_pubkey.as_bytes()),
        bitcoin::Network::Bitcoin,
    )
    .map(|address| address.to_string())
    .ok();

    Ok(Some((value_msat, address)))
}

fn hex_display(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Verification poll: periodically fetches the guardians' authoritative live
/// `FederationWallet` and cross-checks its value against our derived balance
/// (the latest resolved consolidated UTXO). This is a monitoring signal only —
/// NOT the balance source — so a divergence is `warn!`-logged (once sustained)
/// rather than acted upon. Silently no-ops for federations without a walletv2
/// module.
async fn run_verification_loop(ctx: &ModuleTaskCtx) {
    let federation_id = ctx.federation_id;

    let Some(instance_id) = ctx
        .config
        .modules
        .iter()
        .find_map(|(&id, module)| (module.kind.as_str() == KIND.as_str()).then_some(id))
    else {
        return;
    };

    let peers = ctx
        .config
        .global
        .api_endpoints
        .iter()
        .map(|(&id, url)| (id, url.url.clone()))
        .collect();
    let api = match DynGlobalApi::new(ctx.connectors.clone(), peers, None) {
        Ok(api) => api.with_module(instance_id),
        Err(e) => {
            warn!("walletv2 verification poll for {federation_id} could not build API: {e:?}");
            return;
        }
    };

    let mut interval = tokio::time::interval(VERIFY_POLL_INTERVAL);
    let mut divergence_since: Option<Instant> = None;
    // Whether we've already emitted the sustained-divergence warning for the
    // current divergence episode, so we warn once when it crosses the threshold
    // (and log once when it clears) rather than every poll.
    let mut warned = false;

    loop {
        interval.tick().await;

        let Some(live) = fetch_live_federation_wallet(&api, ctx).await else {
            // No peer returned a wallet yet (unfunded, or all unreachable);
            // nothing to compare against, don't treat as divergence.
            continue;
        };
        let live_msat = (live.value.to_sat() * 1000) as i64;

        let derived_msat = match latest_resolved_value_msat(ctx).await {
            Ok(Some(v)) => v,
            // Nothing resolved yet (backfill in progress): can't compare, so
            // don't accrue divergence.
            Ok(None) => {
                divergence_since = None;
                continue;
            }
            Err(e) => {
                warn!("walletv2 verification poll for {federation_id} DB read failed: {e:?}");
                continue;
            }
        };

        if (live_msat - derived_msat).abs() > VERIFY_TOLERANCE_MSAT {
            let since = *divergence_since.get_or_insert_with(Instant::now);
            if since.elapsed() >= VERIFY_SUSTAINED_DIVERGENCE && !warned {
                warn!(
                    federation = %federation_id,
                    live_msat,
                    derived_msat,
                    live_outpoint = %live.outpoint,
                    "walletv2 derived on-chain balance diverges from the live \
                     federation_wallet by {} msat, sustained for {}s — investigate \
                     (derived balance may be stale or a transition was missed)",
                    live_msat - derived_msat,
                    since.elapsed().as_secs(),
                );
                warned = true;
            }
        } else {
            // Back in agreement; note the recovery once if we had warned.
            if warned {
                info!(
                    federation = %federation_id,
                    live_msat,
                    "walletv2 derived on-chain balance re-agrees with the live \
                     federation_wallet",
                );
            }
            divergence_since = None;
            warned = false;
        }
    }
}

/// Asks each peer in turn for its live `federation_wallet`, returning the first
/// `Some` response. A monitoring cross-check, so a single honest peer's view is
/// sufficient; unreachable peers and `None` (unfunded) are skipped.
async fn fetch_live_federation_wallet(
    api: &fedimint_api_client::api::DynModuleApi,
    ctx: &ModuleTaskCtx,
) -> Option<FederationWallet> {
    for &peer_id in ctx.config.global.api_endpoints.keys() {
        match api
            .request_single_peer::<Option<FederationWallet>>(
                FEDERATION_WALLET_ENDPOINT.to_owned(),
                ApiRequestErased::default(),
                peer_id,
            )
            .await
        {
            Ok(Some(wallet)) => return Some(wallet),
            // Peer reachable but wallet unfunded — keep asking others in case
            // one is ahead, but a unanimous None just means "nothing to check".
            Ok(None) => continue,
            Err(_) => continue,
        }
    }
    None
}

/// `FROM ... ORDER BY ... LIMIT 1` fragment selecting the single latest
/// RESOLVED consolidated-UTXO row for `$1 = federation_id`, ranking each txid
/// by its FIRST appearance (min session_index, then min item_index within that
/// session) and picking the txid whose first appearance is greatest.
///
/// Why first-appearance and not any appearance: the walletv2 server re-emits a
/// `Signatures` CI for every still-unfinalized tx, once per peer, each session
/// until it finalizes, and within a session those items are ordered by txid
/// (BTreeMap key order), not creation order. So an older-but-still-pending tx
/// re-announced in a later session can carry a higher `(session, item)` than
/// the genuinely newer tx; ranking by first appearance avoids returning its
/// stale value.
///
/// Residual limitation (accepted, transient): two DISTINCT txids first
/// announced in the SAME session still can't be disambiguated from
/// `(session, item)` alone (item order there is txid order, not creation
/// order). Resolves itself once one of them finalizes.
const LATEST_RESOLVED_UTXO_FROM: &str = "
    FROM (
        SELECT DISTINCT ON (txid)
               txid, session_index, item_index, utxo_value_msat, address
        FROM fmo_walletv2.wallet_utxos
        WHERE federation_id = $1 AND utxo_value_msat IS NOT NULL
        ORDER BY txid, session_index ASC, item_index ASC
    ) firsts
    ORDER BY session_index DESC, item_index DESC
    LIMIT 1";

/// The value (msat) of our latest RESOLVED consolidated UTXO, or `None` if the
/// resolver hasn't produced one yet.
async fn latest_resolved_value_msat(ctx: &ModuleTaskCtx) -> anyhow::Result<Option<i64>> {
    let conn = ctx.pool.get().await?;
    query_value::<Option<i64>>(
        &conn,
        // Wrapped in a scalar subselect so exactly one row (NULL when none) is
        // returned, keeping `query_value` happy.
        &format!("SELECT (SELECT utxo_value_msat {LATEST_RESOLVED_UTXO_FROM})"),
        &[&ctx.federation_id.consensus_encode_to_vec()],
    )
    .await
}

/// The federation's current on-chain UTXO(s). walletv2 keeps everything in a
/// single consolidated UTXO, so this returns at most one entry (the latest
/// resolved transition). Shape matches the v1 wallet `/utxos` endpoint so the
/// frontend can render it the same way.
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
    struct WalletUtxoRaw {
        txid: Vec<u8>,
        utxo_value_msat: i64,
        address: Option<String>,
    }

    let latest = query::<WalletUtxoRaw>(
        &state.pool.get().await?,
        // Latest resolved UTXO ranked by first appearance (see
        // `LATEST_RESOLVED_UTXO_FROM`).
        &format!("SELECT txid, utxo_value_msat, address {LATEST_RESOLVED_UTXO_FROM}"),
        &[&federation_id.consensus_encode_to_vec()],
    )
    .await?;

    latest
        .into_iter()
        // The consolidated UTXO always has a standard P2WSH address, so
        // `address` is populated on resolution; skip the (unexpected) NULL
        // case rather than fabricate an address.
        .filter_map(|utxo| {
            let address = utxo.address?;
            Some((utxo.txid, utxo.utxo_value_msat, address))
        })
        .map(|(txid_bytes, value_msat, address)| {
            Result::<_, anyhow::Error>::Ok(FederationUtxo {
                address: Address::<bitcoin::address::NetworkUnchecked>::from_str(&address)?,
                out_point: OutPoint {
                    txid: Txid::from_slice(&txid_bytes)?,
                    vout: 0,
                },
                amount: Amount::from_msats(value_msat.try_into()?),
            })
        })
        .collect()
}
