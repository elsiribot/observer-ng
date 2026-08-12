-- Convert `session_times` from a fully-rebuilt materialized view into an
-- incrementally maintained regular table.
--
-- As a matview it was `REFRESH ... CONCURRENTLY`'d every cycle, which
-- re-derived the forward-fill over ALL sessions (8.4M+) joined to all
-- session-time votes (32M+). On the production dataset a single refresh took
-- 1-3 hours, so with a 60s interval it ran back-to-back continuously,
-- permanently thrashing the buffer cache. The values are a deterministic
-- function of each session's votes, and a session's votes are final once every
-- module has processed it, so the vast majority of the output never changes
-- between cycles. `refresh_session_times` (see db/session_times.rs) now only
-- recomputes the still-changing tail and freezes the finalized prefix.
--
-- `federation_tx_daily` (added in v5) reads `session_times`, so it must be
-- dropped before the matview and recreated afterwards over the new table. Its
-- definition is unchanged.

DROP MATERIALIZED VIEW IF EXISTS federation_tx_daily;

-- Preserve the already-computed values: copy the matview into the new table,
-- then drop the matview. The copied rows are correct and final for every
-- session that existed at migration time.
ALTER MATERIALIZED VIEW session_times RENAME TO session_times_old;

CREATE TABLE session_times
(
    federation_id               BYTEA   NOT NULL REFERENCES federations (federation_id),
    session_index               INTEGER NOT NULL,
    estimated_session_timestamp TIMESTAMP,
    PRIMARY KEY (federation_id, session_index)
);

INSERT INTO session_times (federation_id, session_index, estimated_session_timestamp)
SELECT federation_id, session_index, estimated_session_timestamp
FROM session_times_old;

DROP MATERIALIZED VIEW session_times_old;

CREATE INDEX session_times_ts ON session_times (federation_id, estimated_session_timestamp);

-- Per-federation cursor: `next_session_index` is the lowest session whose
-- timestamp is not yet frozen. Everything below it is finalized (recomputed
-- while final) and never revisited; `[next_session_index, max]` is recomputed
-- each cycle. Seeded lazily in Rust (see refresh_session_times) so migrated
-- federations skip re-deriving their already-correct prefix.
CREATE TABLE session_times_progress
(
    federation_id      BYTEA   NOT NULL REFERENCES federations (federation_id),
    next_session_index INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (federation_id)
);

-- Recreate federation_tx_daily verbatim over the new session_times table.
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

CREATE UNIQUE INDEX federation_tx_daily_pk
    ON federation_tx_daily (federation_id, day);
