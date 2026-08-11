//! Per-contract terminal status for LNv2, derived from the spending input's
//! variant (claim/refund). Keyed by the funding outpoint, which is what an
//! input references. Pure function of the legs → idempotent and replay-stable.

use tokio_postgres::Transaction;

pub async fn recompute_contract_status(
    dbtx: &Transaction<'_>,
    federation_id: &[u8],
    outpoint_txid: &[u8],
) -> anyhow::Result<()> {
    dbtx.execute(
        "UPDATE contracts c SET status = CASE
             WHEN EXISTS (SELECT 1 FROM input_outpoints io
                          WHERE io.federation_id = c.federation_id
                            AND io.outpoint_txid = c.txid
                            AND io.outpoint_out_index = c.out_index
                            AND io.variant = 'claim')  THEN 'succeeded'
             WHEN EXISTS (SELECT 1 FROM input_outpoints io
                          WHERE io.federation_id = c.federation_id
                            AND io.outpoint_txid = c.txid
                            AND io.outpoint_out_index = c.out_index
                            AND io.variant = 'refund') THEN 'refunded'
             ELSE 'pending'
         END
         WHERE c.federation_id = $1 AND c.txid = $2",
        &[&federation_id, &outpoint_txid],
    )
    .await?;
    Ok(())
}
