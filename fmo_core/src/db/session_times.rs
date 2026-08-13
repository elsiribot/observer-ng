//! Maintenance of the `session_times` table.
//!
//! `session_times` maps each `(federation_id, session_index)` to an estimated
//! wall-clock timestamp, forward-filled from module-contributed
//! `session_time_votes` so sessions without a vote inherit the previous known
//! timestamp. It used to be a materialized view rebuilt in full every refresh
//! cycle, which on the production dataset took hours (see schema/core/v6.sql).
//!
//! Each row also stores `next_vote_time`, the mirror-image backward-fill: the
//! nearest vote AT-OR-AFTER the session (NULL for sessions past the last known
//! vote). `(estimated_session_timestamp, next_vote_time)` bracket a vote-less
//! session's true time, giving the explorer an uncertainty interval.
//!
//! The value for a session is a deterministic function of that session's votes,
//! and a session's votes are final once every installed module has processed it
//! (the same "frontier" the gold cursor tracks). So the finalized prefix never
//! changes and only the still-processing tail needs recomputing.
//! [`FederationObserver::refresh_session_times`] does exactly that;
//! [`recompute_full`] recomputes everything and is used by tests and as a
//! bootstrap primitive.

use fedimint_core::encoding::Encodable;

use crate::observer::FederationObserver;

/// Recomputes `session_times` for every session of every federation from
/// scratch (the full forward-fill), upserting the results. Equivalent to the
/// old `REFRESH MATERIALIZED VIEW session_times`. Used by tests; production
/// uses the incremental [`FederationObserver::refresh_session_times`].
pub async fn recompute_full(conn: &impl deadpool_postgres::GenericClient) -> anyhow::Result<()> {
    conn.batch_execute(
        // language=postgresql
        "WITH votes AS (
             SELECT federation_id, session_index, MAX(timestamp) AS ts
             FROM session_time_votes
             GROUP BY federation_id, session_index
         ),
         all_sessions AS (
             SELECT s.federation_id, s.session_index, v.ts
             FROM sessions s
             LEFT JOIN votes v USING (federation_id, session_index)
         ),
         grouped AS (
             SELECT *,
                    -- Forward-fill group: increments at each voted session, so
                    -- FIRST_VALUE ASC carries the nearest vote at-or-before.
                    SUM(CASE WHEN ts IS NOT NULL THEN 1 ELSE 0 END)
                        OVER (PARTITION BY federation_id ORDER BY session_index) AS grp,
                    -- Backward-fill group: the same running count over the
                    -- reversed order, so FIRST_VALUE DESC carries the nearest
                    -- vote at-or-after.
                    SUM(CASE WHEN ts IS NOT NULL THEN 1 ELSE 0 END)
                        OVER (PARTITION BY federation_id ORDER BY session_index DESC) AS grp_desc
             FROM all_sessions
         )
         INSERT INTO session_times (federation_id, session_index, estimated_session_timestamp, next_vote_time)
         SELECT federation_id,
                session_index,
                FIRST_VALUE(ts) OVER (PARTITION BY federation_id, grp ORDER BY session_index)
                    AS estimated_session_timestamp,
                FIRST_VALUE(ts) OVER (PARTITION BY federation_id, grp_desc ORDER BY session_index DESC)
                    AS next_vote_time
         FROM grouped
         ON CONFLICT (federation_id, session_index)
         DO UPDATE SET estimated_session_timestamp = EXCLUDED.estimated_session_timestamp,
                       next_vote_time = EXCLUDED.next_vote_time",
    )
    .await?;
    Ok(())
}

impl FederationObserver {
    /// Incrementally maintains `session_times`: freezes the finalized prefix
    /// and recomputes only the still-changing tail, replacing the old full
    /// matview refresh. O(new/in-flight sessions) per cycle in steady state.
    ///
    /// Per federation the "frontier" is `min` over every installed module's
    /// cursor (a missing cursor counts as 0) — the same boundary the gold
    /// processor uses. Sessions below it are final: every module has processed
    /// them, so no further votes can arrive and their forward-fill inputs are
    /// fixed. The cursor in `session_times_progress` records how far the frozen
    /// prefix reaches; each cycle we recompute `[cursor, max]` and then advance
    /// the cursor to the frontier, so a session is always recomputed once more
    /// after it becomes final before being frozen.
    ///
    /// Note: like the gold cursor, session-time freshness therefore trails the
    /// slowest installed module. A perpetually-stalled module would keep the
    /// frontier low and force a full per-federation recompute each cycle.
    pub async fn refresh_session_times(&self) -> anyhow::Result<()> {
        let module_kinds: Vec<String> = self
            .registry()
            .iter()
            .map(|(kind, _)| kind.to_string())
            .collect();

        for federation in self.list_federations().await? {
            let fed = federation.federation_id.consensus_encode_to_vec();
            let conn = self.pool().get().await?;

            // frontier = min over installed module cursors (missing => 0).
            let frontier: i32 = conn
                .query_one(
                    "SELECT COALESCE(MIN(COALESCE(mp.next_session_index, 0)), 0)
                     FROM unnest($2::text[]) AS k(module_kind)
                     LEFT JOIN module_progress mp
                       ON mp.module_kind = k.module_kind AND mp.federation_id = $1",
                    &[&fed, &module_kinds],
                )
                .await?
                .get(0);

            // start = lowest session to recompute this cycle. On first sight
            // (no cursor row) seed at the frontier so a migrated federation
            // skips re-deriving its already-correct prefix; otherwise recompute
            // from the stored cursor, rewinding to the frontier if a module
            // replayed below it.
            let stored: Option<i32> = conn
                .query_opt(
                    "SELECT next_session_index FROM session_times_progress WHERE federation_id = $1",
                    &[&fed],
                )
                .await?
                .map(|row| row.get(0));
            let start = match stored {
                None => frontier,
                Some(cursor) => cursor.min(frontier),
            };

            // Recompute [start, max], forward-filling from the frozen value at
            // start-1 (the `seed`), and upsert. For rows before the first vote
            // in the window the FIRST_VALUE over their group is NULL, so they
            // fall back to the seed carry.
            conn.execute(
                // language=postgresql
                "WITH seed AS (
                     SELECT estimated_session_timestamp AS carry
                     FROM session_times
                     WHERE federation_id = $1 AND session_index = $2 - 1
                 ),
                 votes AS (
                     SELECT session_index, MAX(timestamp) AS ts
                     FROM session_time_votes
                     WHERE federation_id = $1 AND session_index >= $2
                     GROUP BY session_index
                 ),
                 tail AS (
                     SELECT s.session_index, v.ts
                     FROM sessions s
                     LEFT JOIN votes v ON v.session_index = s.session_index
                     WHERE s.federation_id = $1 AND s.session_index >= $2
                 ),
                 grouped AS (
                     SELECT session_index, ts,
                            -- Forward-fill group (nearest vote at-or-before).
                            SUM(CASE WHEN ts IS NOT NULL THEN 1 ELSE 0 END)
                                OVER (ORDER BY session_index) AS grp,
                            -- Backward-fill group (nearest vote at-or-after),
                            -- scoped to this [start, max] window only.
                            SUM(CASE WHEN ts IS NOT NULL THEN 1 ELSE 0 END)
                                OVER (ORDER BY session_index DESC) AS grp_desc
                     FROM tail
                 ),
                 filled AS (
                     SELECT session_index,
                            COALESCE(
                                FIRST_VALUE(ts) OVER (PARTITION BY grp ORDER BY session_index),
                                (SELECT carry FROM seed)
                            ) AS estimated_session_timestamp,
                            -- No seed carry: sessions after the last vote in the
                            -- window have grp_desc = 0 (all-NULL group), so
                            -- FIRST_VALUE is NULL -- an unbounded upper bound for
                            -- the most recent sessions, which is correct.
                            FIRST_VALUE(ts) OVER (PARTITION BY grp_desc ORDER BY session_index DESC)
                                AS next_vote_time
                     FROM grouped
                 )
                 INSERT INTO session_times (federation_id, session_index, estimated_session_timestamp, next_vote_time)
                 SELECT $1, session_index, estimated_session_timestamp, next_vote_time FROM filled
                 ON CONFLICT (federation_id, session_index)
                 DO UPDATE SET estimated_session_timestamp = EXCLUDED.estimated_session_timestamp,
                               next_vote_time = EXCLUDED.next_vote_time",
                &[&fed, &start],
            )
            .await?;

            // Freeze everything below the frontier: advance (or seed) the cursor.
            conn.execute(
                "INSERT INTO session_times_progress (federation_id, next_session_index)
                 VALUES ($1, $2)
                 ON CONFLICT (federation_id)
                 DO UPDATE SET next_session_index = EXCLUDED.next_session_index",
                &[&fed, &frontier],
            )
            .await?;
        }

        Ok(())
    }
}
