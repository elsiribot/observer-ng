use deadpool_postgres::Transaction;
use fedimint_core::config::{ClientConfig, FederationId};
use fedimint_core::encoding::Encodable;
use fedimint_core::epoch::ConsensusItem;
use fedimint_core::session_outcome::{AcceptedItem, SessionOutcome};

use crate::registry::instance_to_kind;

/// Writes a session (with its real, signed data) and its structural facts
/// into the core tables, then delegates the per-item work to
/// [`ingest_items`].
///
/// Shared by the live fetcher and the import tool. Idempotent.
pub async fn ingest_session(
    dbtx: &Transaction<'_>,
    config: &ClientConfig,
    federation_id: FederationId,
    session_index: u64,
    session: &SessionOutcome,
) -> anyhow::Result<()> {
    let federation_id_bytes = federation_id.consensus_encode_to_vec();

    // DO UPDATE (not DO NOTHING): a live poll may have already inserted this
    // row open (`data = NULL`) once the session signed; this call must still
    // fill in the real data.
    dbtx.execute(
        "INSERT INTO sessions VALUES ($1, $2, $3)
         ON CONFLICT (federation_id, session_index) DO UPDATE SET data = EXCLUDED.data",
        &[
            &federation_id_bytes,
            &(session_index as i32),
            &session.consensus_encode_to_vec(),
        ],
    )
    .await?;

    // Historical/import path: no first-seen stamp. `synced_at` is only set on
    // the live path (see `live_process`).
    ingest_items(
        dbtx,
        config,
        federation_id,
        session_index,
        &session.items,
        0,
        None,
    )
    .await
}

/// Writes the structural facts (transactions, input/output/CI rows with
/// their module kind) into the core tables for `items[start..]` of a
/// session, and upserts `session_stats` with counts computed over the WHOLE
/// `items` list. Module-specific columns (`amount_msat`, `details`) stay
/// NULL until the dispatch engine hands the items to their observer
/// modules.
///
/// Ensures an open (`data = NULL`) session row exists, so structural facts
/// (e.g. a consensus item) can already reference an in-progress session via
/// the `(federation_id, session_index)` FK before it signs. A no-op if
/// `ingest_session` already inserted the row with real data.
///
/// `synced_at`, when `Some`, is stamped onto the `transactions` /
/// `consensus_items` rows this call inserts. It records the wall-clock time
/// an item was first observed live and is only set on the live path
/// ([`live_process`](crate::live::live_process)); the historical/import path
/// passes `None`. Because every insert is `ON CONFLICT DO NOTHING`, the FIRST
/// ingest's value wins, so a later historical replay of the same item never
/// overwrites (nor clears) the original live first-seen stamp.
///
/// Shared by the live fetcher (which calls this incrementally as items
/// arrive) and the historical/import path (which calls it once with
/// `start = 0` via [`ingest_session`]). Idempotent.
pub async fn ingest_items(
    dbtx: &Transaction<'_>,
    config: &ClientConfig,
    federation_id: FederationId,
    session_index: u64,
    items: &[AcceptedItem],
    start: usize,
    synced_at: Option<chrono::NaiveDateTime>,
) -> anyhow::Result<()> {
    let federation_id_bytes = federation_id.consensus_encode_to_vec();

    // Prepared statements: these inserts run millions of times during import,
    // re-preparing them each call doubles the round trips.
    let insert_session = dbtx
        .prepare_cached(
            "INSERT INTO sessions (federation_id, session_index, data)
             VALUES ($1, $2, NULL) ON CONFLICT DO NOTHING",
        )
        .await?;
    let insert_transaction = dbtx
        .prepare_cached(
            "INSERT INTO transactions (federation_id, txid, session_index, item_index, data, synced_at)
             VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING",
        )
        .await?;
    let insert_input = dbtx
        .prepare_cached(
            "INSERT INTO transaction_inputs (federation_id, txid, in_index, kind)
             VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
        )
        .await?;
    let insert_output = dbtx
        .prepare_cached(
            "INSERT INTO transaction_outputs (federation_id, txid, out_index, kind)
             VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
        )
        .await?;
    let insert_ci = dbtx
        .prepare_cached(
            "INSERT INTO consensus_items (federation_id, session_index, item_index, peer_id, kind, synced_at)
             VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING",
        )
        .await?;

    dbtx.execute(
        &insert_session,
        &[&federation_id_bytes, &(session_index as i32)],
    )
    .await?;

    // Tallied over the WHOLE `items` list (not just `items[start..]`) and
    // written into `session_stats` below, so the session-list API can read
    // per-session counts in O(1) instead of counting rows on request.
    // Running totals stay correct across partial (live) calls because of the
    // `DO UPDATE` below.
    let mut tx_count: i32 = 0;
    let mut ci_count: i32 = 0;
    let mut ci_by_kind: std::collections::BTreeMap<String, i64> = std::collections::BTreeMap::new();
    for accepted_item in items {
        match &accepted_item.item {
            ConsensusItem::Transaction(_) => tx_count += 1,
            ConsensusItem::Module(module_ci) => {
                let kind = instance_to_kind(config, module_ci.module_instance_id());
                ci_count += 1;
                *ci_by_kind.entry(kind).or_insert(0) += 1;
            }
            _ => {
                // Unknown consensus item variants are ignored, same as before
            }
        }
    }

    // Structural inserts only cover the new suffix: `items[..start]` was
    // already ingested by an earlier call.
    for (rel, accepted_item) in items[start..].iter().enumerate() {
        let item_index = start + rel;
        match &accepted_item.item {
            ConsensusItem::Transaction(transaction) => {
                let txid = transaction.tx_hash();

                dbtx.execute(
                    &insert_transaction,
                    &[
                        &federation_id_bytes,
                        &txid.consensus_encode_to_vec(),
                        &(session_index as i32),
                        &(item_index as i32),
                        &transaction.consensus_encode_to_vec(),
                        &synced_at,
                    ],
                )
                .await?;

                for (in_index, input) in transaction.inputs.iter().enumerate() {
                    let kind = instance_to_kind(config, input.module_instance_id());
                    dbtx.execute(
                        &insert_input,
                        &[
                            &federation_id_bytes,
                            &txid.consensus_encode_to_vec(),
                            &(in_index as i32),
                            &kind,
                        ],
                    )
                    .await?;
                }

                for (out_index, output) in transaction.outputs.iter().enumerate() {
                    let kind = instance_to_kind(config, output.module_instance_id());
                    dbtx.execute(
                        &insert_output,
                        &[
                            &federation_id_bytes,
                            &txid.consensus_encode_to_vec(),
                            &(out_index as i32),
                            &kind,
                        ],
                    )
                    .await?;
                }
            }
            ConsensusItem::Module(module_ci) => {
                let kind = instance_to_kind(config, module_ci.module_instance_id());
                dbtx.execute(
                    &insert_ci,
                    &[
                        &federation_id_bytes,
                        &(session_index as i32),
                        &(item_index as i32),
                        &(accepted_item.peer.to_usize() as i32),
                        &kind,
                        &synced_at,
                    ],
                )
                .await?;
            }
            _ => {
                // Unknown consensus item variants are ignored, same as before
            }
        }
    }

    dbtx.execute(
        "INSERT INTO session_stats (federation_id, session_index, tx_count, ci_count, items_by_kind)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (federation_id, session_index) DO UPDATE SET
             tx_count = EXCLUDED.tx_count,
             ci_count = EXCLUDED.ci_count,
             items_by_kind = EXCLUDED.items_by_kind",
        &[
            &federation_id_bytes,
            &(session_index as i32),
            &tx_count,
            &ci_count,
            &serde_json::to_value(&ci_by_kind)?,
        ],
    )
    .await?;

    Ok(())
}
