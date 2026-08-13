-- Per-item "sync time" and per-session upper time bound for the explorer's
-- estimated-time-with-uncertainty feature.
--
-- `synced_at` records the wall-clock time an item (a transaction or a
-- consensus item) was FIRST observed live by the observer (via the live
-- fetch path). It is written only on the first ingest (`ON CONFLICT DO
-- NOTHING` in `ingest_items` preserves the earliest stamp) and stays NULL
-- for items only ever seen through historical replay / import. When present
-- it is an exact, observed time rather than a vote-derived estimate.
--
-- `next_vote_time` on `session_times` is the backward-filled counterpart of
-- `estimated_session_timestamp` (which is the forward-filled nearest vote
-- at-or-before a session): the nearest vote AT-OR-AFTER a session. Together
-- they bracket a vote-less session's true time, giving the explorer an
-- uncertainty interval. NULL for sessions after the last known vote (no
-- upper bound yet).

ALTER TABLE transactions    ADD COLUMN IF NOT EXISTS synced_at TIMESTAMP;
ALTER TABLE consensus_items ADD COLUMN IF NOT EXISTS synced_at TIMESTAMP;
ALTER TABLE session_times   ADD COLUMN IF NOT EXISTS next_vote_time TIMESTAMP;
