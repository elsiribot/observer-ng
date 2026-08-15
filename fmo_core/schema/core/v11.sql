-- Per-federation, per-day, per-transaction-type rollup of RAW fedimint
-- transactions.
--
-- Backs the "Fedimint transactions" grain of the federation-detail activity
-- chart's stacked-by-type view. `federation_tx_daily` (v5/v6) already provides
-- the same per-(federation, day) totals but with no type breakdown; this view
-- adds the `kind` dimension so the chart can stack layers.
--
-- Each raw fedimint tx is classified into exactly ONE `kind` using the same
-- taxonomy as the gold `fold_standalone` CASE (see gold.rs), with an added
-- `lightning` bucket for any tx touching `ln`/`lnv2` — mirroring gold's
-- `AND NOT (i.kinds && ARRAY['ln','lnv2'])` guard on the peg cases, so a
-- lightning tx never also counts as a peg. Grain and value columns match
-- `federation_tx_daily`: `tx_count` = distinct txids, `volume_msat` = summed
-- input amounts. Days without an estimated session timestamp are excluded
-- (same as `federation_tx_daily`); they reappear once forward-fill assigns one.
CREATE MATERIALIZED VIEW federation_tx_kind_daily AS
WITH tx_kinds AS (
    SELECT t.federation_id                              AS federation_id,
           DATE(st.estimated_session_timestamp)         AS day,
           t.txid                                       AS txid,
           CASE
               WHEN (i.kinds && ARRAY['ln', 'lnv2'])
                 OR (o.kinds && ARRAY['ln', 'lnv2']) THEN 'lightning'
               WHEN i.kinds @> ARRAY['wallet'] THEN 'peg_in'
               WHEN o.kinds @> ARRAY['wallet'] THEN 'peg_out'
               WHEN i.kinds @> ARRAY['walletv2'] THEN 'peg_in_v2'
               WHEN o.kinds @> ARRAY['walletv2'] THEN 'peg_out_v2'
               WHEN (i.kinds && ARRAY['stability_pool', 'multi_sig_stability_pool'])
                 OR (o.kinds && ARRAY['stability_pool', 'multi_sig_stability_pool'])
                   THEN 'stability_pool'
               WHEN i.kinds <@ ARRAY['mint'] AND o.kinds <@ ARRAY['mint'] THEN 'ecash_transfer'
               WHEN i.kinds <@ ARRAY['mintv2'] AND o.kinds <@ ARRAY['mintv2'] THEN 'ecash_transfer_v2'
               ELSE 'other'
               END                                      AS kind,
           COALESCE(i.amt, 0)                           AS volume_msat
    FROM transactions t
             JOIN session_times st
                  ON t.session_index = st.session_index
                      AND t.federation_id = st.federation_id
             JOIN LATERAL (SELECT array_agg(DISTINCT kind) AS kinds, SUM(amount_msat) AS amt
                           FROM transaction_inputs
                           WHERE federation_id = t.federation_id AND txid = t.txid) i ON true
             JOIN LATERAL (SELECT array_agg(DISTINCT kind) AS kinds
                           FROM transaction_outputs
                           WHERE federation_id = t.federation_id AND txid = t.txid) o ON true
    WHERE st.estimated_session_timestamp IS NOT NULL
)
SELECT federation_id,
       day,
       kind,
       COUNT(*)::bigint                           AS tx_count,
       COALESCE(SUM(volume_msat), 0)::bigint      AS volume_msat
FROM tx_kinds
GROUP BY federation_id, day, kind;

-- Unique index enables REFRESH MATERIALIZED VIEW CONCURRENTLY and serves the
-- per-federation range scan the endpoint issues.
CREATE UNIQUE INDEX federation_tx_kind_daily_pk
    ON federation_tx_kind_daily (federation_id, day, kind);
