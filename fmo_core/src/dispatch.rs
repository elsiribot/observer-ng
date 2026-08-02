use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use deadpool_postgres::{Pool, Transaction};
use fedimint_core::config::{ClientConfig, FederationId};
use fedimint_core::core::ModuleKind;
use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::epoch::ConsensusItem;
use fedimint_core::session_outcome::SessionOutcome;
use tracing::{debug, warn};

use crate::module::{CiMeta, ItemMeta, ObserverModule, ProcessCtx};
use crate::query::query_value;
use crate::registry::{instance_to_kind, ModuleRegistry};
use crate::services::CoreServices;

/// Processes pending sessions for every installed module that is behind the
/// fetch cursor. Live processing and historical replay are the same code
/// path: a freshly added module simply starts with its cursor at 0.
///
/// Each (module, session) pair is processed in its own transaction together
/// with the module's cursor advance, so modules progress independently and a
/// failing module only stalls itself.
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
        "SELECT MAX(session_index) FROM sessions WHERE federation_id = $1",
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
    drop(conn);

    let min_next = cursors.values().copied().min().expect("registry not empty");
    if min_next > fetched {
        return Ok(0);
    }

    let decoders = registry.decoders(config);
    let mut processed = 0u64;

    let conn = pool.get().await?;
    let rows = conn
        .query(
            "SELECT session_index, data FROM sessions
             WHERE federation_id = $1 AND session_index >= $2
             ORDER BY session_index
             LIMIT $3",
            &[&federation_id_bytes, &min_next, &(batch_limit as i64)],
        )
        .await?;
    drop(conn);

    for row in rows {
        let session_index: i32 = row.get(0);
        let session = SessionOutcome::consensus_decode_whole(&row.get::<_, Vec<u8>>(1), &decoders)?;

        for (kind, module) in registry.iter() {
            if cursors.get(kind).copied().unwrap_or(0) != session_index {
                continue;
            }

            let mut module_conn = pool.get().await?;
            let dbtx = module_conn.transaction().await?;
            dbtx.batch_execute(&format!(
                "SET LOCAL search_path TO {}, public",
                crate::db::migrations::schema_name(kind.as_str())
            ))
            .await?;

            let result = dispatch_session_to_module(
                &dbtx,
                module.as_ref(),
                services,
                federation_id,
                config,
                session_index as u64,
                &session,
            )
            .await;

            match result {
                Ok(()) => {
                    dbtx.execute(
                        "INSERT INTO public.module_progress VALUES ($1, $2, $3)
                         ON CONFLICT (module_kind, federation_id)
                         DO UPDATE SET next_session_index = EXCLUDED.next_session_index",
                        &[&kind.as_str(), &federation_id_bytes, &(session_index + 1)],
                    )
                    .await?;
                    dbtx.commit().await?;
                    *cursors.get_mut(kind).expect("cursor exists") = session_index + 1;
                    processed += 1;
                }
                Err(e) => {
                    // The module's transaction rolls back and its cursor stays
                    // put; it will retry on the next round while other modules
                    // continue to make progress.
                    warn!(
                        "Module {kind} failed processing session {session_index} \
                         of federation {federation_id}: {e:?}"
                    );
                }
            }
        }
    }

    debug!("Processed {processed} (module, session) units for federation {federation_id}");
    Ok(processed)
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
                        "UPDATE public.transaction_inputs
                         SET amount_msat = $4, details = $5
                         WHERE federation_id = $1 AND txid = $2 AND in_index = $3",
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
                        "UPDATE public.transaction_outputs
                         SET amount_msat = $4, details = $5
                         WHERE federation_id = $1 AND txid = $2 AND out_index = $3",
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
                        "UPDATE public.consensus_items
                         SET details = $4
                         WHERE federation_id = $1 AND session_index = $2 AND item_index = $3",
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
            process_pending(&pool, &registry, &services, federation_id, &config, 100).await?;
        if processed == 0 {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
}
