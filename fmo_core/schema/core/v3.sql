-- Precomputed per-session aggregates so the session-list API can page
-- through sessions in O(1) per row instead of counting transactions/CIs
-- on every request. Populated at ingest and backfilled for pre-existing
-- sessions (see fmo_core::session_stats::backfill_session_stats).
CREATE TABLE session_stats
(
    federation_id BYTEA   NOT NULL REFERENCES federations (federation_id),
    session_index INTEGER NOT NULL,
    tx_count      INTEGER NOT NULL,
    ci_count      INTEGER NOT NULL,
    items_by_kind JSONB   NOT NULL,
    PRIMARY KEY (federation_id, session_index),
    FOREIGN KEY (federation_id, session_index) REFERENCES sessions (federation_id, session_index)
);
