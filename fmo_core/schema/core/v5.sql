-- Per-federation, per-day fedimint-transaction rollup.
--
-- Backs the federation-detail activity histogram
-- (`transaction_histogram`) and the home-page 7-day activity sparkline
-- (`federation_activity`). Both previously recomputed the same all-history
-- aggregate live on every (uncached) request: a full scan + external sort of
-- `transaction_inputs` grouped per tx, joined to `session_times` and grouped
-- per day — 10-18s per federation-detail load, and re-run once per federation
-- on the home page. Materializing it turns those into an index range scan.
--
-- Grain matches what both callers computed: one row per (federation, day),
-- `tx_count` = distinct fedimint txs that day, `volume_msat` = summed input
-- amount across those txs. This is a DIFFERENT grain from the gold
-- `user_tx_daily` matview (deduplicated *user* transactions, split by
-- kind/direction/status), so it is its own view rather than a reuse.
--
-- `estimated_session_timestamp` comes from the `session_times` matview, which
-- is refreshed before this one each cycle. Rows without a timestamp yet (no
-- session-time vote) are excluded, mirroring `user_tx_daily`'s
-- `first_timestamp IS NOT NULL` filter; they reappear once the forward-fill
-- assigns a timestamp. Excluding NULL days also keeps the unique index
-- (required for REFRESH ... CONCURRENTLY) free of NULLs.
CREATE MATERIALIZED VIEW federation_tx_daily AS
SELECT t.federation_id                                   AS federation_id,
       DATE(st.estimated_session_timestamp)              AS day,
       COUNT(DISTINCT t.txid)::bigint                    AS tx_count,
       COALESCE(SUM(ti.total_input_amount), 0)::bigint   AS volume_msat
FROM transactions t
         JOIN session_times st
              ON t.session_index = st.session_index
                  AND t.federation_id = st.federation_id
         JOIN (SELECT federation_id,
                      txid,
                      SUM(amount_msat) AS total_input_amount
               FROM transaction_inputs
               GROUP BY federation_id, txid) ti
              ON t.txid = ti.txid AND t.federation_id = ti.federation_id
WHERE st.estimated_session_timestamp IS NOT NULL
GROUP BY t.federation_id, DATE(st.estimated_session_timestamp);

-- Unique index enables REFRESH MATERIALIZED VIEW CONCURRENTLY and serves the
-- per-federation range scans the two callers issue.
CREATE UNIQUE INDEX federation_tx_daily_pk
    ON federation_tx_daily (federation_id, day);
