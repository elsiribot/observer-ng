//! Per-contract terminal status, derived from the contract's own legs
//! (funding/cancel outputs, claim/refund inputs, preimage-decryption shares).
//! Called after each leg is recorded, so the status advances incrementally
//! without a global recompute; because it is a pure function of the current
//! legs it is idempotent and replay-stable regardless of processing order.

use tokio_postgres::Transaction;

/// Recompute one contract's `status`. `threshold` is the preimage-decryption
/// threshold `n - (n-1)/3`; an incoming contract counts as decrypted once it
/// has shares from at least `threshold` distinct guardians.
pub async fn recompute_contract_status(
    dbtx: &Transaction<'_>,
    federation_id: &[u8],
    contract_id: &[u8],
    threshold: i64,
) -> anyhow::Result<()> {
    dbtx.execute(
        "UPDATE contracts c SET status = CASE
             WHEN c.type = 'outgoing'
                  AND EXISTS (SELECT 1 FROM output_contracts oc
                              WHERE oc.federation_id = c.federation_id
                                AND oc.contract_id = c.contract_id
                                AND oc.interaction_kind = 'cancel')
                  THEN 'refunded'
             WHEN c.type = 'outgoing'
                  AND EXISTS (SELECT 1 FROM input_contracts ic
                              WHERE ic.federation_id = c.federation_id
                                AND ic.contract_id = c.contract_id)
                  THEN 'succeeded'
             WHEN c.type = 'incoming'
                  AND EXISTS (SELECT 1 FROM input_contracts ic
                              WHERE ic.federation_id = c.federation_id
                                AND ic.contract_id = c.contract_id)
                  AND (SELECT COUNT(DISTINCT ds.peer_id) FROM decryption_shares ds
                       WHERE ds.federation_id = c.federation_id
                         AND ds.contract_id = c.contract_id) >= $3
                  THEN 'succeeded'
             WHEN c.type = 'incoming'
                  AND EXISTS (SELECT 1 FROM input_contracts ic
                              WHERE ic.federation_id = c.federation_id
                                AND ic.contract_id = c.contract_id)
                  THEN 'refunded'
             WHEN c.type = 'incoming'
                  AND (SELECT COUNT(DISTINCT ds.peer_id) FROM decryption_shares ds
                       WHERE ds.federation_id = c.federation_id
                         AND ds.contract_id = c.contract_id) >= $3
                  THEN 'decrypted'
             ELSE 'pending'
         END
         WHERE c.federation_id = $1 AND c.contract_id = $2",
        &[&federation_id, &contract_id, &threshold],
    )
    .await?;
    Ok(())
}
