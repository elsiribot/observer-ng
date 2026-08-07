//! Gold processor: folds the structural (silver) `transactions` /
//! `transaction_inputs` / `transaction_outputs` tables into deduplicated
//! user-transaction tables (`user_transactions`, `user_transaction_txs`).
//!
//! Two classifiers feed `fold_sessions`, the single entry point both the
//! background processor and tests call:
//! - `fold_standalone`: peg-in / peg-out / ecash transfer / stability pool
//!   (v1 and v2 analogues), by input/output kind signature, with the exact
//!   fedimint fee `Σinputs − Σoutputs`. One row per non-LN-leg txid.
//! - `fold_ln`: LN/LNv2 payments, folding every leg of a contract's lifecycle
//!   (offer/fund/claim/cancel/refund, possibly spanning many sessions) into
//!   one `user_transactions` row keyed by `contract_id`, with a
//!   `user_transaction_txs` membership row per leg.
//!
//! Both a federation's LN module and its LNv2 module are optional (a given
//! `FederationObserver` instance may not install either), so both
//! `fold_standalone`'s LN-leg guards and `fold_ln`'s LN/LNv2 blocks check
//! `to_regclass` before touching `fmo_ln`/`fmo_lnv2` and skip gracefully if
//! the schema is absent.

use std::time::Duration;

use deadpool_postgres::Transaction;
use fedimint_core::config::FederationId;
use fedimint_core::encoding::Encodable;

use crate::observer::FederationObserver;

/// True if `schema.table` exists — used to gracefully skip `fmo_ln` /
/// `fmo_lnv2` handling for federations/instances that don't have that module
/// installed, instead of erroring on a missing relation.
async fn table_exists(dbtx: &Transaction<'_>, qualified_name: &str) -> anyhow::Result<bool> {
    Ok(dbtx
        .query_one(
            "SELECT to_regclass($1) IS NOT NULL",
            &[&qualified_name],
        )
        .await?
        .get(0))
}

/// Classifies standalone (non-LN) transactions in session range `[start,
/// end)` by their input/output kind signature and upserts them into
/// `user_transactions`, plus a `self` membership row per txid in
/// `user_transaction_txs`. Idempotent: safe to re-run over the same range.
///
/// LN-leg transactions (inputs/outputs that reference an `fmo_ln` /
/// `fmo_lnv2` contract) are skipped; they are folded by `fold_ln` instead.
/// The guards against each are only included when that module's schema
/// exists (see `table_exists`) — a federation/instance without the LN or
/// LNv2 module installed has no `fmo_ln`/`fmo_lnv2` schema at all.
pub async fn fold_standalone(
    dbtx: &Transaction<'_>,
    fed: &[u8],
    start: i32,
    end: i32,
) -> anyhow::Result<()> {
    let mut ln_guards = String::new();
    if table_exists(dbtx, "fmo_ln.contracts").await? {
        ln_guards.push_str(
            " AND NOT EXISTS (SELECT 1 FROM fmo_ln.output_contracts oc WHERE oc.federation_id=t.federation_id AND oc.txid=t.txid)
              AND NOT EXISTS (SELECT 1 FROM fmo_ln.input_contracts  ic WHERE ic.federation_id=t.federation_id AND ic.txid=t.txid)",
        );
    }
    if table_exists(dbtx, "fmo_lnv2.contracts").await? {
        ln_guards.push_str(
            " AND NOT EXISTS (SELECT 1 FROM fmo_lnv2.contracts        c2 WHERE c2.federation_id=t.federation_id AND c2.txid=t.txid)
              AND NOT EXISTS (SELECT 1 FROM fmo_lnv2.input_outpoints  io WHERE io.federation_id=t.federation_id AND io.txid=t.txid)",
        );
    }

    let query = format!(
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
           {ln_guards}
         ON CONFLICT (federation_id, user_tx_key) DO UPDATE SET
            kind=EXCLUDED.kind, direction=EXCLUDED.direction, amount_msat=EXCLUDED.amount_msat,
            fedimint_fee_msat=EXCLUDED.fedimint_fee_msat, first_timestamp=EXCLUDED.first_timestamp,
            last_timestamp=EXCLUDED.last_timestamp"
    );
    dbtx.execute(&query, &[&fed, &start, &end]).await?;

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

/// `(federation_id, contract_id)` pairs for lnv1 contracts touched by a leg
/// (`fmo_ln.output_contracts` or `fmo_ln.input_contracts` row) whose tx falls
/// in session range `[$2,$3)` for federation `$1`. Shared by the main
/// recompute query and the membership-row query below so both agree on
/// exactly which contracts are in scope for this fold.
const LN_TOUCHED_CONTRACTS: &str = "(SELECT oc.federation_id, oc.contract_id
       FROM fmo_ln.output_contracts oc JOIN transactions t USING (federation_id, txid)
       WHERE t.federation_id=$1 AND t.session_index>=$2 AND t.session_index<$3
     UNION
     SELECT ic.federation_id, ic.contract_id
       FROM fmo_ln.input_contracts ic JOIN transactions t USING (federation_id, txid)
       WHERE t.federation_id=$1 AND t.session_index>=$2 AND t.session_index<$3)";

/// Same as `LN_TOUCHED_CONTRACTS` but for lnv2: a leg is a `fmo_lnv2.contracts`
/// row (the funding tx) or a `fmo_lnv2.input_outpoints` row that spends a
/// contract's funding outpoint (claim).
const LNV2_TOUCHED_CONTRACTS: &str = "(SELECT c.federation_id, c.contract_id
       FROM fmo_lnv2.contracts c JOIN transactions t USING (federation_id, txid)
       WHERE t.federation_id=$1 AND t.session_index>=$2 AND t.session_index<$3
     UNION
     SELECT c2.federation_id, c2.contract_id
       FROM fmo_lnv2.input_outpoints io
       JOIN fmo_lnv2.contracts c2 ON c2.federation_id=io.federation_id
         AND c2.txid=io.outpoint_txid AND c2.out_index=io.outpoint_out_index
       JOIN transactions t ON t.federation_id=io.federation_id AND t.txid=io.txid
       WHERE t.federation_id=$1 AND t.session_index>=$2 AND t.session_index<$3)";

/// Folds LN (lnv1) and LNv2 contracts touched by a tx in session range
/// `[start, end)` into `user_transactions` (keyed by `contract_id`) plus
/// `user_transaction_txs` membership rows (one per leg, with its role).
/// Recomputes every touched contract from scratch each time — idempotent and
/// independent of leg order, so replay and out-of-order processing are safe.
/// Skips the lnv1/lnv2 blocks entirely (via `table_exists`) when that
/// module's schema isn't installed for this observer instance.
pub async fn fold_ln(
    dbtx: &Transaction<'_>,
    fed: &[u8],
    start: i32,
    end: i32,
) -> anyhow::Result<()> {
    if table_exists(dbtx, "fmo_ln.contracts").await? {
        fold_ln_v1(dbtx, fed, start, end).await?;
    }
    if table_exists(dbtx, "fmo_lnv2.contracts").await? {
        fold_lnv2(dbtx, fed, start, end).await?;
    }
    Ok(())
}

async fn fold_ln_v1(dbtx: &Transaction<'_>, fed: &[u8], start: i32, end: i32) -> anyhow::Result<()> {
    let query = format!(
        "INSERT INTO user_transactions (federation_id, user_tx_key, kind, direction, amount_msat,
            fedimint_fee_msat, num_fedimint_txs, first_session_index, first_timestamp, last_timestamp, status)
         SELECT c.federation_id, c.contract_id,
                CASE WHEN c.type='incoming' THEN 'ln_receive' ELSE 'ln_send' END,
                CASE WHEN c.type='incoming' THEN 'in' ELSE 'out' END,
                funds.amount_msat,
                fees.fee_msat,
                legs.n,
                legs.first_session,
                fst.estimated_session_timestamp, lst.estimated_session_timestamp,
                CASE WHEN spends.\"any\" THEN 'completed'
                     WHEN cancels.\"any\" THEN 'cancelled'
                     ELSE 'in_flight' END
         FROM fmo_ln.contracts c
         JOIN (SELECT federation_id, contract_id, SUM(o.amount_msat) amount_msat
               FROM fmo_ln.output_contracts oc JOIN transaction_outputs o USING (federation_id, txid, out_index)
               WHERE oc.interaction_kind='fund' GROUP BY 1,2) funds USING (federation_id, contract_id)
         JOIN (SELECT federation_id, contract_id, COUNT(DISTINCT txid) n,
                      MIN(session_index) first_session, MAX(session_index) last_session
               FROM (SELECT oc.federation_id, oc.contract_id, oc.txid, t.session_index
                       FROM fmo_ln.output_contracts oc JOIN transactions t USING (federation_id, txid)
                     UNION
                     SELECT ic.federation_id, ic.contract_id, ic.txid, t.session_index
                       FROM fmo_ln.input_contracts ic JOIN transactions t USING (federation_id, txid)) all_legs
               GROUP BY 1,2) legs USING (federation_id, contract_id)
         JOIN LATERAL (SELECT COALESCE(SUM(f.fee),0) fee_msat FROM (
                 SELECT (SELECT SUM(amount_msat) FROM transaction_inputs  WHERE federation_id=x.federation_id AND txid=x.txid)
                      - (SELECT SUM(amount_msat) FROM transaction_outputs WHERE federation_id=x.federation_id AND txid=x.txid) fee
                 FROM (SELECT DISTINCT federation_id, txid FROM (
                        SELECT federation_id, txid FROM fmo_ln.output_contracts WHERE federation_id=c.federation_id AND contract_id=c.contract_id
                        UNION SELECT federation_id, txid FROM fmo_ln.input_contracts WHERE federation_id=c.federation_id AND contract_id=c.contract_id) u) x) f) fees ON true
         JOIN LATERAL (SELECT bool_or(true) AS \"any\" FROM fmo_ln.input_contracts
                       WHERE federation_id=c.federation_id AND contract_id=c.contract_id) spends ON true
         LEFT JOIN LATERAL (SELECT bool_or(true) AS \"any\" FROM fmo_ln.output_contracts
                       WHERE federation_id=c.federation_id AND contract_id=c.contract_id AND interaction_kind='cancel') cancels ON true
         LEFT JOIN session_times fst ON fst.federation_id=c.federation_id AND fst.session_index=legs.first_session
         LEFT JOIN session_times lst ON lst.federation_id=c.federation_id AND lst.session_index=legs.last_session
         WHERE (c.federation_id, c.contract_id) IN {LN_TOUCHED_CONTRACTS}
         ON CONFLICT (federation_id, user_tx_key) DO UPDATE SET
            kind=EXCLUDED.kind, direction=EXCLUDED.direction, amount_msat=EXCLUDED.amount_msat,
            fedimint_fee_msat=EXCLUDED.fedimint_fee_msat, num_fedimint_txs=EXCLUDED.num_fedimint_txs,
            first_session_index=EXCLUDED.first_session_index, first_timestamp=EXCLUDED.first_timestamp,
            last_timestamp=EXCLUDED.last_timestamp, status=EXCLUDED.status"
    );
    dbtx.execute(&query, &[&fed, &start, &end]).await?;

    // Only emit membership rows for contracts that produced a parent
    // `user_transactions` row (i.e. funded contracts). An offer-only,
    // unfunded invoice moves no value and is INNER-JOINed out of the upsert
    // above, so it has no parent — inserting a membership row for it would
    // violate the FK and stall the whole federation's gold processor.
    let query = format!(
        "INSERT INTO user_transaction_txs (federation_id, txid, user_tx_key, role, session_index)
         SELECT oc.federation_id, oc.txid, oc.contract_id, oc.interaction_kind, t.session_index
           FROM fmo_ln.output_contracts oc JOIN transactions t USING (federation_id, txid)
           WHERE (oc.federation_id, oc.contract_id) IN {LN_TOUCHED_CONTRACTS}
             AND EXISTS (SELECT 1 FROM user_transactions ut
                          WHERE ut.federation_id=oc.federation_id AND ut.user_tx_key=oc.contract_id)
         UNION ALL
         SELECT ic.federation_id, ic.txid, ic.contract_id,
                CASE WHEN EXISTS (SELECT 1 FROM fmo_ln.output_contracts x
                                   WHERE x.federation_id=ic.federation_id AND x.contract_id=ic.contract_id AND x.interaction_kind='cancel')
                     THEN 'refund' ELSE 'claim' END,
                t.session_index
           FROM fmo_ln.input_contracts ic JOIN transactions t USING (federation_id, txid)
           WHERE (ic.federation_id, ic.contract_id) IN {LN_TOUCHED_CONTRACTS}
             AND EXISTS (SELECT 1 FROM user_transactions ut
                          WHERE ut.federation_id=ic.federation_id AND ut.user_tx_key=ic.contract_id)
         ON CONFLICT DO NOTHING"
    );
    dbtx.execute(&query, &[&fed, &start, &end]).await?;

    Ok(())
}

async fn fold_lnv2(dbtx: &Transaction<'_>, fed: &[u8], start: i32, end: i32) -> anyhow::Result<()> {
    let query = format!(
        "INSERT INTO user_transactions (federation_id, user_tx_key, kind, direction, amount_msat,
            fedimint_fee_msat, num_fedimint_txs, first_session_index, first_timestamp, last_timestamp, status)
         SELECT c.federation_id, c.contract_id,
                CASE WHEN c.type='incoming' THEN 'lnv2_receive' ELSE 'lnv2_send' END,
                CASE WHEN c.type='incoming' THEN 'in' ELSE 'out' END,
                c.amount_msat,
                fees.fee_msat,
                legs.n,
                legs.first_session,
                fst.estimated_session_timestamp, lst.estimated_session_timestamp,
                CASE WHEN spends.\"any\" THEN 'completed' ELSE 'in_flight' END
         FROM fmo_lnv2.contracts c
         JOIN (SELECT federation_id, contract_id, COUNT(DISTINCT txid) n,
                      MIN(session_index) first_session, MAX(session_index) last_session
               FROM (SELECT c2.federation_id, c2.contract_id, c2.txid, t.session_index
                       FROM fmo_lnv2.contracts c2 JOIN transactions t USING (federation_id, txid)
                     UNION
                     SELECT c2.federation_id, c2.contract_id, io.txid, t.session_index
                       FROM fmo_lnv2.input_outpoints io
                       JOIN fmo_lnv2.contracts c2 ON c2.federation_id=io.federation_id
                         AND c2.txid=io.outpoint_txid AND c2.out_index=io.outpoint_out_index
                       JOIN transactions t ON t.federation_id=io.federation_id AND t.txid=io.txid) all_legs
               GROUP BY 1,2) legs USING (federation_id, contract_id)
         JOIN LATERAL (SELECT COALESCE(SUM(f.fee),0) fee_msat FROM (
                 SELECT (SELECT SUM(amount_msat) FROM transaction_inputs  WHERE federation_id=x.federation_id AND txid=x.txid)
                      - (SELECT SUM(amount_msat) FROM transaction_outputs WHERE federation_id=x.federation_id AND txid=x.txid) fee
                 FROM (SELECT DISTINCT federation_id, txid FROM (
                        SELECT c.federation_id AS federation_id, c.txid AS txid
                        UNION
                        SELECT io.federation_id, io.txid FROM fmo_lnv2.input_outpoints io
                          WHERE io.federation_id=c.federation_id AND io.outpoint_txid=c.txid AND io.outpoint_out_index=c.out_index) u) x) f) fees ON true
         JOIN LATERAL (SELECT bool_or(true) AS \"any\" FROM fmo_lnv2.input_outpoints io
                       WHERE io.federation_id=c.federation_id AND io.outpoint_txid=c.txid AND io.outpoint_out_index=c.out_index) spends ON true
         LEFT JOIN session_times fst ON fst.federation_id=c.federation_id AND fst.session_index=legs.first_session
         LEFT JOIN session_times lst ON lst.federation_id=c.federation_id AND lst.session_index=legs.last_session
         WHERE (c.federation_id, c.contract_id) IN {LNV2_TOUCHED_CONTRACTS}
         ON CONFLICT (federation_id, user_tx_key) DO UPDATE SET
            kind=EXCLUDED.kind, direction=EXCLUDED.direction, amount_msat=EXCLUDED.amount_msat,
            fedimint_fee_msat=EXCLUDED.fedimint_fee_msat, num_fedimint_txs=EXCLUDED.num_fedimint_txs,
            first_session_index=EXCLUDED.first_session_index, first_timestamp=EXCLUDED.first_timestamp,
            last_timestamp=EXCLUDED.last_timestamp, status=EXCLUDED.status"
    );
    dbtx.execute(&query, &[&fed, &start, &end]).await?;

    // As in lnv1, only emit membership rows for contracts with a parent
    // `user_transactions` row. An lnv2 `contracts` row always carries an
    // amount and funding outpoint (there is no offer-only concept), so it
    // always produces a parent — but the guard is kept symmetric with lnv1 so
    // no code path can ever insert an orphan membership row.
    let query = format!(
        "INSERT INTO user_transaction_txs (federation_id, txid, user_tx_key, role, session_index)
         SELECT c.federation_id, c.txid, c.contract_id, 'fund', t.session_index
           FROM fmo_lnv2.contracts c JOIN transactions t USING (federation_id, txid)
           WHERE (c.federation_id, c.contract_id) IN {LNV2_TOUCHED_CONTRACTS}
             AND EXISTS (SELECT 1 FROM user_transactions ut
                          WHERE ut.federation_id=c.federation_id AND ut.user_tx_key=c.contract_id)
         UNION ALL
         SELECT io.federation_id, io.txid, c2.contract_id, 'claim', t.session_index
           FROM fmo_lnv2.input_outpoints io
           JOIN fmo_lnv2.contracts c2 ON c2.federation_id=io.federation_id
             AND c2.txid=io.outpoint_txid AND c2.out_index=io.outpoint_out_index
           JOIN transactions t ON t.federation_id=io.federation_id AND t.txid=io.txid
           WHERE (c2.federation_id, c2.contract_id) IN {LNV2_TOUCHED_CONTRACTS}
             AND EXISTS (SELECT 1 FROM user_transactions ut
                          WHERE ut.federation_id=c2.federation_id AND ut.user_tx_key=c2.contract_id)
         ON CONFLICT DO NOTHING"
    );
    dbtx.execute(&query, &[&fed, &start, &end]).await?;

    Ok(())
}

/// Estimates `gateway_fee_estimate_msat` for outgoing LN (`ln_send`) rows
/// touched in session range `[start, end)`. The gateway fee is not on-ledger
/// — a gateway is paid out-of-band for forwarding the payment — so it's
/// estimated by inverting the gateway's advertised fee schedule against the
/// gross contract amount: `contract = invoice + base + ppm·invoice/1e6`, so
/// `invoice = (contract − base) / (1 + ppm/1e6)` and
/// `gateway_fee_estimate_msat = contract − invoice`.
///
/// **Join path (confirmed against production data 2026-08-07, read-only
/// spike, see task-4 report):**
/// - The fund leg's output `details` JSON (serialized `LightningOutput`)
///   carries the gateway's pubkey at
///   `{V0,Contract,contract,Outgoing,gateway_key}`.
/// - That key is `OutgoingContract.gateway_key`, which fedimint sets to
///   `LightningGateway.gateway_redeem_key` — a *different* key from both
///   `gateway_id` and `node_pub_key` (verified live: 0/128 distinct
///   `gateway_key` values matched `fmo_ln.gateways.gateway_id` or
///   `.node_pub_key`; 117/128 matched `raw #>> '{info,gateway_redeem_key}'`,
///   the rest presumably gateways that have since rotated keys or
///   deregistered). So the join is on `raw->info->gateway_redeem_key`, not on
///   either indexed column.
/// - The fee schedule lives at `raw #>> '{info,fees,base_msat}'` /
///   `{info,fees,proportional_millionths}` — nested under `info` (the
///   `LightningGatewayAnnouncement` shape the gateway poller stores), not at
///   the top level.
///
/// Left NULL for `ln_receive` (no gateway fee on the receive side), all
/// non-LN kinds, and whenever no matching gateway/fee schedule is found
/// (deregistered gateway, rotated redeem key, missing raw fee fields, etc).
/// Guarded on `fmo_ln.gateways` existing so instances without the LN module
/// (or without any gateway ever polled) skip cleanly.
///
/// **Fee-schedule drift:** the join uses the gateway's *current* advertised
/// fee, but a contract was funded against whatever schedule was live then. If
/// the gateway later raised `base_msat` above an old contract's amount, the
/// inversion yields `invoice <= 0` and a "fee" >= the contract amount — a
/// nonsense fee-transparency number. Such rows are left NULL (unknown is the
/// correct answer, not a clamped/negative value): the outer `WHERE fee > 0
/// AND fee < contract` drops them.
///
/// **Deterministic gateway pick:** one physical gateway can register several
/// `fmo_ln.gateways` rows sharing a `gateway_redeem_key` (e.g. separate
/// HTTP/Iroh entries) that may advertise different fees. To keep the estimate
/// stable across re-runs (idempotency), we collapse to one fee schedule per
/// `(federation_id, redeem_key)` with `DISTINCT ON ... ORDER BY gateway_id`,
/// i.e. deterministically the lowest `gateway_id`.
///
/// No LNv2 analogue: lnv2 outgoing contracts (`fmo_lnv2.contracts`) don't
/// carry a gateway key in their structural facts, and there is no
/// `fmo_lnv2.gateways` table or any other fee-schedule source in this
/// observer at all, so `lnv2_send` rows are left NULL.
async fn estimate_ln_gateway_fees(
    dbtx: &Transaction<'_>,
    fed: &[u8],
    start: i32,
    end: i32,
) -> anyhow::Result<()> {
    if !table_exists(dbtx, "fmo_ln.gateways").await? {
        return Ok(());
    }
    dbtx.execute(
        "UPDATE user_transactions ut SET gateway_fee_estimate_msat = g.fee
         FROM (
             SELECT e.federation_id, e.contract_id, e.fee
             FROM (
                 SELECT oc.federation_id, oc.contract_id,
                        o.amount_msat AS contract,
                        o.amount_msat
                          - ROUND((o.amount_msat - gw.base) / (1 + gw.ppm / 1000000.0)) AS fee
                 FROM fmo_ln.output_contracts oc
                 JOIN transaction_outputs o USING (federation_id, txid, out_index)
                 JOIN (
                     -- one deterministic fee schedule per (federation_id, redeem_key)
                     SELECT DISTINCT ON (federation_id, redeem_key)
                            federation_id, redeem_key, base, ppm
                     FROM (
                         SELECT federation_id, gateway_id,
                                raw #>> '{info,gateway_redeem_key}' AS redeem_key,
                                (raw #>> '{info,fees,base_msat}')::numeric AS base,
                                (raw #>> '{info,fees,proportional_millionths}')::numeric AS ppm
                         FROM fmo_ln.gateways
                     ) gws
                     WHERE redeem_key IS NOT NULL AND base IS NOT NULL AND ppm IS NOT NULL
                     ORDER BY federation_id, redeem_key, gateway_id
                 ) gw ON gw.federation_id = oc.federation_id
                      AND gw.redeem_key =
                          (o.details #>> '{V0,Contract,contract,Outgoing,gateway_key}')
                 WHERE oc.interaction_kind = 'fund'
             ) e
             -- drop nonsense estimates from fee-schedule drift; NULL is correct
             WHERE e.fee > 0 AND e.fee < e.contract
         ) g
         WHERE ut.federation_id = g.federation_id AND ut.user_tx_key = g.contract_id
           AND ut.kind = 'ln_send'
           AND ut.federation_id = $1 AND ut.first_session_index >= $2 AND ut.first_session_index < $3",
        &[&fed, &start, &end],
    )
    .await?;
    Ok(())
}

/// Folds session range `[start, end)` into the gold layer: standalone
/// (non-LN) classification, LN/LNv2 contract grouping, and gateway fee
/// estimation for outgoing LN sends.
pub async fn fold_sessions(
    dbtx: &Transaction<'_>,
    fed: &[u8],
    start: i32,
    end: i32,
) -> anyhow::Result<()> {
    fold_standalone(dbtx, fed, start, end).await?;
    fold_ln(dbtx, fed, start, end).await?;
    estimate_ln_gateway_fees(dbtx, fed, start, end).await?;
    Ok(())
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
