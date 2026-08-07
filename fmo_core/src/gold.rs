//! Gold processor: folds the structural (silver) `transactions` /
//! `transaction_inputs` / `transaction_outputs` tables into deduplicated
//! user-transaction tables (`user_transactions`, `user_transaction_txs`).
//!
//! This module currently implements only the standalone (non-LN)
//! classification: peg-in / peg-out / ecash transfer / stability pool (v1 and
//! v2 analogues), by input/output kind signature, with the exact fedimint fee
//! `Σinputs − Σoutputs`. Transactions that touch an LN or LNv2 contract are
//! skipped here; grouping them into multi-tx user transactions (offer → fund
//! → claim/refund) is a later task. `fold_sessions` is the single entry point
//! both the background processor and tests call; it will grow an `fold_ln`
//! call alongside `fold_standalone` once LN grouping lands.

use std::time::Duration;

use deadpool_postgres::Transaction;
use fedimint_core::config::FederationId;
use fedimint_core::encoding::Encodable;

use crate::observer::FederationObserver;

/// Classifies standalone (non-LN) transactions in session range `[start,
/// end)` by their input/output kind signature and upserts them into
/// `user_transactions`, plus a `self` membership row per txid in
/// `user_transaction_txs`. Idempotent: safe to re-run over the same range.
///
/// LN-leg transactions (inputs/outputs that reference an `fmo_ln` /
/// `fmo_lnv2` contract) are skipped; they are folded by `fold_ln` (added in a
/// later task).
pub async fn fold_standalone(
    dbtx: &Transaction<'_>,
    fed: &[u8],
    start: i32,
    end: i32,
) -> anyhow::Result<()> {
    dbtx.execute(
        "INSERT INTO user_transactions
           (federation_id, user_tx_key, kind, direction, amount_msat,
            fedimint_fee_msat, num_fedimint_txs, first_session_index,
            first_timestamp, last_timestamp, status)
         SELECT t.federation_id, t.txid,
                CASE
                  WHEN i.kinds @> ARRAY['wallet'] AND NOT (i.kinds && ARRAY['ln','lnv2']) THEN 'peg_in'
                  WHEN o.kinds @> ARRAY['wallet'] AND NOT (o.kinds && ARRAY['ln','lnv2']) THEN 'peg_out'
                  WHEN o.kinds @> ARRAY['walletv2'] THEN 'peg_in_v2'
                  WHEN i.kinds @> ARRAY['walletv2'] THEN 'peg_out_v2'
                  WHEN (i.kinds && ARRAY['stability_pool','multi_sig_stability_pool'])
                    OR (o.kinds && ARRAY['stability_pool','multi_sig_stability_pool']) THEN 'stability_pool'
                  WHEN i.kinds <@ ARRAY['mint'] AND o.kinds <@ ARRAY['mint'] THEN 'ecash_transfer'
                  WHEN i.kinds <@ ARRAY['mintv2'] AND o.kinds <@ ARRAY['mintv2'] THEN 'ecash_transfer_v2'
                  ELSE 'other'
                END AS kind,
                CASE
                  WHEN i.kinds @> ARRAY['wallet'] OR o.kinds @> ARRAY['walletv2'] THEN 'in'
                  WHEN o.kinds @> ARRAY['wallet'] OR i.kinds @> ARRAY['walletv2'] THEN 'out'
                  ELSE 'internal'
                END AS direction,
                -- primary value: wallet side for pegs, else input side
                CASE
                  WHEN i.kinds @> ARRAY['wallet'] THEN i.wallet_amt
                  WHEN o.kinds @> ARRAY['wallet'] THEN o.wallet_amt
                  ELSE i.amt END AS amount_msat,
                (i.amt - o.amt) AS fedimint_fee_msat,
                1, t.session_index, st.estimated_session_timestamp, st.estimated_session_timestamp, 'completed'
         FROM transactions t
         JOIN LATERAL (SELECT array_agg(DISTINCT kind) kinds, SUM(amount_msat) amt,
                              SUM(amount_msat) FILTER (WHERE kind='wallet') wallet_amt
                       FROM transaction_inputs WHERE federation_id=t.federation_id AND txid=t.txid) i ON true
         JOIN LATERAL (SELECT array_agg(DISTINCT kind) kinds, SUM(amount_msat) amt,
                              SUM(amount_msat) FILTER (WHERE kind='wallet') wallet_amt
                       FROM transaction_outputs WHERE federation_id=t.federation_id AND txid=t.txid) o ON true
         LEFT JOIN session_times st ON st.federation_id=t.federation_id AND st.session_index=t.session_index
         WHERE t.federation_id=$1 AND t.session_index>=$2 AND t.session_index<$3
           AND NOT EXISTS (SELECT 1 FROM fmo_ln.output_contracts oc WHERE oc.federation_id=t.federation_id AND oc.txid=t.txid)
           AND NOT EXISTS (SELECT 1 FROM fmo_ln.input_contracts  ic WHERE ic.federation_id=t.federation_id AND ic.txid=t.txid)
           AND NOT EXISTS (SELECT 1 FROM fmo_lnv2.contracts        c2 WHERE c2.federation_id=t.federation_id AND c2.txid=t.txid)
           AND NOT EXISTS (SELECT 1 FROM fmo_lnv2.input_outpoints  io WHERE io.federation_id=t.federation_id AND io.txid=t.txid)
         ON CONFLICT (federation_id, user_tx_key) DO UPDATE SET
            kind=EXCLUDED.kind, direction=EXCLUDED.direction, amount_msat=EXCLUDED.amount_msat,
            fedimint_fee_msat=EXCLUDED.fedimint_fee_msat, first_timestamp=EXCLUDED.first_timestamp,
            last_timestamp=EXCLUDED.last_timestamp",
        &[&fed, &start, &end],
    )
    .await?;

    // self membership rows
    dbtx.execute(
        "INSERT INTO user_transaction_txs (federation_id, txid, user_tx_key, role, session_index)
         SELECT federation_id, user_tx_key, user_tx_key, 'self', first_session_index
         FROM user_transactions WHERE federation_id=$1 AND first_session_index>=$2 AND first_session_index<$3
           AND user_tx_key IN (SELECT txid FROM transactions WHERE federation_id=$1)
         ON CONFLICT DO NOTHING",
        &[&fed, &start, &end],
    )
    .await?;

    Ok(())
}

/// Folds session range `[start, end)` into the gold layer. For now this is
/// just standalone classification; a later task adds a `fold_ln` call
/// alongside it for LN/LNv2 contract-based grouping.
pub async fn fold_sessions(
    dbtx: &Transaction<'_>,
    fed: &[u8],
    start: i32,
    end: i32,
) -> anyhow::Result<()> {
    fold_standalone(dbtx, fed, start, end).await
}

/// Background task: incrementally folds one federation's structural
/// transactions into the gold (user-transaction) tables. The gold cursor
/// trails `min(module_progress.next_session_index)` — i.e. it never reads
/// past what every installed module has already processed — and rewinds if a
/// module replay drops its cursor below the gold cursor.
pub async fn run_gold_processor(
    observer: FederationObserver,
    fed: FederationId,
) -> anyhow::Result<()> {
    const BATCH: i32 = 500;
    let fedb = fed.consensus_encode_to_vec();
    loop {
        let conn = observer.pool().get().await?;
        conn.execute(
            "INSERT INTO gold_progress (federation_id, next_session_index) VALUES ($1,0) ON CONFLICT DO NOTHING",
            &[&fedb],
        )
        .await?;
        // target = min over installed module cursors for this federation
        let target: i32 = conn
            .query_one(
                "SELECT COALESCE(MIN(next_session_index), 0) FROM module_progress WHERE federation_id=$1",
                &[&fedb],
            )
            .await?
            .get(0);
        let mut next: i32 = conn
            .query_one(
                "SELECT next_session_index FROM gold_progress WHERE federation_id=$1",
                &[&fedb],
            )
            .await?
            .get(0);
        // rewind if a module replayed below us
        if target < next {
            next = target;
            conn.execute(
                "UPDATE gold_progress SET next_session_index=$2 WHERE federation_id=$1",
                &[&fedb, &next],
            )
            .await?;
        }
        if next >= target {
            drop(conn);
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        }
        let end = (next + BATCH).min(target);
        // Never hold two pool connections at once: with every federation's
        // processor doing the same, that starves the pool once federation
        // count exceeds pool size (see commit "fix: pool deadlock when many
        // federations process concurrently").
        drop(conn);
        let mut conn = observer.pool().get().await?;
        let dbtx = conn.transaction().await?;
        fold_sessions(&dbtx, &fedb, next, end).await?;
        dbtx.execute(
            "UPDATE gold_progress SET next_session_index=$2 WHERE federation_id=$1",
            &[&fedb, &end],
        )
        .await?;
        dbtx.commit().await?;
    }
}
