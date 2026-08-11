//! One-time backfill of `session_stats` for sessions ingested before the table
//! existed. Batched by session range and resumable (re-run fills only gaps),
//! so it can run as a background task without a blocking migration.
use deadpool_postgres::Pool;

const BATCH: i64 = 2000;

pub async fn backfill_session_stats(pool: &Pool, federation_id: &[u8]) -> anyhow::Result<()> {
    loop {
        let conn = pool.get().await?;
        // Next contiguous window of sessions missing stats.
        let n = conn
            .execute(
                "WITH batch AS (
                     SELECT session_index FROM sessions
                     WHERE federation_id = $1
                       AND NOT EXISTS (SELECT 1 FROM session_stats ss
                                       WHERE ss.federation_id = sessions.federation_id
                                         AND ss.session_index = sessions.session_index)
                     ORDER BY session_index
                     LIMIT $2
                 ),
                 bounds AS (SELECT min(session_index) AS lo, max(session_index) AS hi FROM batch)
                 INSERT INTO session_stats (federation_id, session_index, tx_count, ci_count, items_by_kind)
                 SELECT $1, b.session_index,
                        COALESCE(t.c, 0)::int,
                        COALESCE(c.total, 0)::int,
                        COALESCE(c.by_kind, '{}'::jsonb)
                 FROM batch b
                 LEFT JOIN (
                     SELECT session_index, count(*) AS c
                     FROM transactions
                     WHERE federation_id = $1
                       AND session_index BETWEEN (SELECT lo FROM bounds) AND (SELECT hi FROM bounds)
                     GROUP BY session_index
                 ) t ON t.session_index = b.session_index
                 LEFT JOIN (
                     SELECT session_index, sum(k) AS total, jsonb_object_agg(kind, k) AS by_kind
                     FROM (
                         SELECT session_index, kind, count(*) AS k
                         FROM consensus_items
                         WHERE federation_id = $1
                           AND session_index BETWEEN (SELECT lo FROM bounds) AND (SELECT hi FROM bounds)
                         GROUP BY session_index, kind
                     ) x
                     GROUP BY session_index
                 ) c ON c.session_index = b.session_index
                 ON CONFLICT DO NOTHING",
                &[&federation_id, &BATCH],
            )
            .await?;
        if n == 0 {
            return Ok(());
        }
    }
}
