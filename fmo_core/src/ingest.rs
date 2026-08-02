use deadpool_postgres::Transaction;
use fedimint_core::config::{ClientConfig, FederationId};
use fedimint_core::encoding::Encodable;
use fedimint_core::epoch::ConsensusItem;
use fedimint_core::session_outcome::SessionOutcome;

use crate::registry::instance_to_kind;

/// Writes a session and its structural facts (transactions, input/output/CI
/// rows with their module kind) into the core tables. Module-specific columns
/// (`amount_msat`, `details`) stay NULL until the dispatch engine hands the
/// items to their observer modules.
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

    // Prepared statements: these inserts run millions of times during import,
    // re-preparing them each call doubles the round trips.
    let insert_session = dbtx
        .prepare_cached("INSERT INTO sessions VALUES ($1, $2, $3) ON CONFLICT DO NOTHING")
        .await?;
    let insert_transaction = dbtx
        .prepare_cached(
            "INSERT INTO transactions VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING",
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
            "INSERT INTO consensus_items (federation_id, session_index, item_index, peer_id, kind)
             VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING",
        )
        .await?;

    dbtx.execute(
        &insert_session,
        &[
            &federation_id_bytes,
            &(session_index as i32),
            &session.consensus_encode_to_vec(),
        ],
    )
    .await?;

    for (item_index, accepted_item) in session.items.iter().enumerate() {
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
                    ],
                )
                .await?;
            }
            _ => {
                // Unknown consensus item variants are ignored, same as before
            }
        }
    }

    Ok(())
}
