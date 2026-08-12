use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use deadpool_postgres::{Pool, Transaction};
use fedimint_core::config::{ClientConfig, FederationId};
use fedimint_core::core::ModuleKind;
use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::epoch::ConsensusItem;
use fedimint_core::session_outcome::SessionOutcome;
use futures::StreamExt;
use tracing::{debug, warn};

use crate::module::{CiMeta, ItemMeta, ObserverModule, ProcessCtx};
use crate::query::query_value;
use crate::registry::{instance_to_kind, ModuleRegistry};
use crate::services::CoreServices;

/// Processes pending sessions for every installed module that is behind the
/// fetch cursor. Live processing and historical replay are the same code
/// path: a freshly added module simply starts with its cursor at 0.
///
/// For throughput, each module processes a whole batch of sessions in one
/// transaction together with a single cursor advance (still atomic, so the
/// per-module progress invariant holds). If the batch fails, it falls back to
/// per-session transactions so only the actually-failing session stalls the
/// module while everything before it commits.
///
/// Returns the number of (module, session) units processed.
pub async fn process_pending(
    pool: &Pool,
    registry: &ModuleRegistry,
    services: &Arc<CoreServices>,
    federation_id: FederationId,
    config: &ClientConfig,
    batch_limit: u32,
) -> anyhow::Result<u64> {
    if registry.is_empty() {
        return Ok(0);
    }

    let federation_id_bytes = federation_id.consensus_encode_to_vec();
    let conn = pool.get().await?;

    let fetched = query_value::<Option<i32>>(
        &conn,
        "SELECT MAX(session_index) FROM sessions WHERE federation_id = $1 AND data IS NOT NULL",
        &[&federation_id_bytes],
    )
    .await?;
    let Some(fetched) = fetched else {
        return Ok(0);
    };

    let mut cursors: BTreeMap<ModuleKind, i32> =
        registry.iter().map(|(kind, _)| (kind.clone(), 0)).collect();
    for row in conn
        .query(
            "SELECT module_kind, next_session_index FROM module_progress WHERE federation_id = $1",
            &[&federation_id_bytes],
        )
        .await?
    {
        let kind_str: String = row.get(0);
        let kind = ModuleKind::clone_from_str(&kind_str);
        if let Some(cursor) = cursors.get_mut(&kind) {
            *cursor = row.get(1);
        }
    }

    let min_next = cursors.values().copied().min().expect("registry not empty");
    if min_next > fetched {
        return Ok(0);
    }

    let rows = conn
        .query(
            "SELECT session_index, data FROM sessions
             WHERE federation_id = $1 AND session_index >= $2 AND data IS NOT NULL
             ORDER BY session_index
             LIMIT $3",
            &[&federation_id_bytes, &min_next, &(batch_limit as i64)],
        )
        .await?;
    drop(conn);

    // Decode the batch in parallel; consensus decoding is the CPU-heavy part
    // of replay.
    let num_cpus = std::thread::available_parallelism()
        .map(|cpus| cpus.get())
        .unwrap_or(8);
    let decoders = registry.decoders(config);
    let decoded: Vec<(i32, SessionOutcome)> = futures::stream::iter(rows.into_iter())
        .map(|row| {
            let decoders = decoders.clone();
            tokio::task::spawn_blocking(move || {
                let session_index: i32 = row.get(0);
                let data: Vec<u8> = row.get(1);
                SessionOutcome::consensus_decode_whole(&data, &decoders)
                    .map(|session| (session_index, session))
            })
        })
        .buffered(num_cpus)
        .map(|join_result| join_result.expect("decode task panicked"))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<_, _>>()?;

    let mut processed = 0u64;
    for (kind, module) in registry.iter() {
        let cursor = cursors.get(kind).copied().unwrap_or(0);
        let pending: Vec<&(i32, SessionOutcome)> = decoded
            .iter()
            .filter(|(session_index, _)| *session_index >= cursor)
            .collect();
        if pending.is_empty() {
            continue;
        }

        match process_module_batch(
            pool,
            module.as_ref(),
            services,
            federation_id,
            config,
            &pending,
        )
        .await
        {
            Ok(units) => {
                processed += units;
            }
            Err(e) => {
                debug!(
                    "Module {kind} failed batch processing, retrying per session \
                     to isolate the failure: {e:?}"
                );
                for (session_index, session) in &pending {
                    match process_module_single(
                        pool,
                        module.as_ref(),
                        services,
                        federation_id,
                        config,
                        *session_index,
                        session,
                    )
                    .await
                    {
                        Ok(()) => processed += 1,
                        Err(e) => {
                            // The module's transaction rolled back and its
                            // cursor stays put; it will retry on the next
                            // round while other modules continue.
                            warn!(
                                "Module {kind} failed processing session {session_index} \
                                 of federation {federation_id}: {e:?}"
                            );
                            break;
                        }
                    }
                }
            }
        }
    }

    debug!("Processed {processed} (module, session) units for federation {federation_id}");
    Ok(processed)
}

/// Checks whether a session contains any item belonging to the module's kind.
fn session_touches_module(
    config: &ClientConfig,
    module_kind: &ModuleKind,
    session: &SessionOutcome,
) -> bool {
    session
        .items
        .iter()
        .any(|accepted_item| match &accepted_item.item {
            ConsensusItem::Transaction(transaction) => transaction
                .inputs
                .iter()
                .map(|input| input.module_instance_id())
                .chain(
                    transaction
                        .outputs
                        .iter()
                        .map(|output| output.module_instance_id()),
                )
                .any(|instance_id| instance_to_kind(config, instance_id) == module_kind.as_str()),
            ConsensusItem::Module(module_ci) => {
                instance_to_kind(config, module_ci.module_instance_id()) == module_kind.as_str()
            }
            _ => false,
        })
}

/// Processes a contiguous run of sessions for one module in a single
/// transaction, advancing the cursor once at the end.
async fn process_module_batch(
    pool: &Pool,
    module: &dyn ObserverModule,
    services: &Arc<CoreServices>,
    federation_id: FederationId,
    config: &ClientConfig,
    pending: &[&(i32, SessionOutcome)],
) -> anyhow::Result<u64> {
    let kind = module.kind();
    let federation_id_bytes = federation_id.consensus_encode_to_vec();

    let mut conn = pool.get().await?;
    let dbtx = conn.transaction().await?;
    dbtx.batch_execute(&format!(
        "SET LOCAL search_path TO {}, public",
        crate::db::migrations::schema_name(kind.as_str())
    ))
    .await?;

    let mut units = 0u64;
    let mut last_session_index = None;
    for (session_index, session) in pending {
        if session_touches_module(config, &kind, session) {
            dispatch_session_to_module(
                &dbtx,
                module,
                services,
                federation_id,
                config,
                *session_index as u64,
                session,
            )
            .await?;
        }
        units += 1;
        last_session_index = Some(*session_index);
    }

    if let Some(last_session_index) = last_session_index {
        dbtx.execute(
            "INSERT INTO public.module_progress VALUES ($1, $2, $3)
             ON CONFLICT (module_kind, federation_id)
             DO UPDATE SET next_session_index = EXCLUDED.next_session_index",
            &[
                &kind.as_str(),
                &federation_id_bytes,
                &(last_session_index + 1),
            ],
        )
        .await?;
    }
    dbtx.commit().await?;

    Ok(units)
}

/// Processes a single session for one module in its own transaction. Used as
/// the fallback path to isolate failing sessions.
async fn process_module_single(
    pool: &Pool,
    module: &dyn ObserverModule,
    services: &Arc<CoreServices>,
    federation_id: FederationId,
    config: &ClientConfig,
    session_index: i32,
    session: &SessionOutcome,
) -> anyhow::Result<()> {
    let single = (session_index, session.clone());
    process_module_batch(pool, module, services, federation_id, config, &[&single]).await?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn dispatch_session_to_module(
    dbtx: &Transaction<'_>,
    module: &dyn ObserverModule,
    services: &Arc<CoreServices>,
    federation_id: FederationId,
    config: &ClientConfig,
    session_index: u64,
    session: &SessionOutcome,
) -> anyhow::Result<()> {
    let module_kind = module.kind();
    let federation_id_bytes = federation_id.consensus_encode_to_vec();
    let peer_count = config.global.api_endpoints.len();

    let mut ctx = ProcessCtx {
        dbtx,
        federation_id,
        config: config.clone(),
        services: services.clone(),
    };

    // Cached statements: write-backs run for every item during replay.
    let update_input = dbtx
        .prepare_cached(
            "UPDATE public.transaction_inputs
             SET amount_msat = $4, details = $5
             WHERE federation_id = $1 AND txid = $2 AND in_index = $3",
        )
        .await?;
    let update_output = dbtx
        .prepare_cached(
            "UPDATE public.transaction_outputs
             SET amount_msat = $4, details = $5
             WHERE federation_id = $1 AND txid = $2 AND out_index = $3",
        )
        .await?;
    let update_ci = dbtx
        .prepare_cached(
            "UPDATE public.consensus_items
             SET details = $4
             WHERE federation_id = $1 AND session_index = $2 AND item_index = $3",
        )
        .await?;

    for (item_index, accepted_item) in session.items.iter().enumerate() {
        match &accepted_item.item {
            ConsensusItem::Transaction(transaction) => {
                let txid = transaction.tx_hash();

                for (in_index, input) in transaction.inputs.iter().enumerate() {
                    if instance_to_kind(config, input.module_instance_id()) != module_kind.as_str()
                    {
                        continue;
                    }
                    let meta = ItemMeta {
                        federation_id,
                        txid,
                        session_index,
                        item_index: item_index as u64,
                        index: in_index as u64,
                        peer_count,
                    };
                    let processed = module.process_input(&mut ctx, input, &meta).await?;
                    dbtx.execute(
                        &update_input,
                        &[
                            &federation_id_bytes,
                            &txid.consensus_encode_to_vec(),
                            &(in_index as i32),
                            &processed.amount.map(|amount| amount.msats as i64),
                            &processed.details,
                        ],
                    )
                    .await?;
                }

                for (out_index, output) in transaction.outputs.iter().enumerate() {
                    if instance_to_kind(config, output.module_instance_id()) != module_kind.as_str()
                    {
                        continue;
                    }
                    let meta = ItemMeta {
                        federation_id,
                        txid,
                        session_index,
                        item_index: item_index as u64,
                        index: out_index as u64,
                        peer_count,
                    };
                    let processed = module.process_output(&mut ctx, output, &meta).await?;
                    dbtx.execute(
                        &update_output,
                        &[
                            &federation_id_bytes,
                            &txid.consensus_encode_to_vec(),
                            &(out_index as i32),
                            &processed.amount.map(|amount| amount.msats as i64),
                            &processed.details,
                        ],
                    )
                    .await?;
                }
            }
            ConsensusItem::Module(module_ci) => {
                if instance_to_kind(config, module_ci.module_instance_id()) != module_kind.as_str()
                {
                    continue;
                }
                let meta = CiMeta {
                    federation_id,
                    session_index,
                    item_index: item_index as u64,
                    peer: accepted_item.peer,
                    peer_count,
                };
                let details = module.process_ci(&mut ctx, module_ci, &meta).await?;
                if details.is_some() {
                    dbtx.execute(
                        &update_ci,
                        &[
                            &federation_id_bytes,
                            &(session_index as i32),
                            &(item_index as i32),
                            &details,
                        ],
                    )
                    .await?;
                }
            }
            _ => {}
        }
    }

    Ok(())
}

/// Background task: keeps processing pending sessions for one federation.
pub async fn run_processor(
    pool: Pool,
    registry: Arc<ModuleRegistry>,
    services: Arc<CoreServices>,
    federation_id: FederationId,
    config: ClientConfig,
) -> anyhow::Result<()> {
    loop {
        let processed =
            process_pending(&pool, &registry, &services, federation_id, &config, 500).await?;
        if processed == 0 {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}
