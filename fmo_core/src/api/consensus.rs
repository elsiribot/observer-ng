use std::sync::LazyLock;

use anyhow::Context;
use axum::extract::{Path, Query, State};
use axum::Json;
use deadpool_postgres::Pool;
use fedimint_core::config::FederationId;
use fedimint_core::encoding::Encodable;
use fmo_api_types::{ConsensusPage, SessionItem};
use postgres_from_row::FromRow;
use serde::Deserialize;
use tracing::{info, warn};

use crate::api::sql_fragments::USER_TX_LATERAL;
use crate::api::AppState;
use crate::observer::FederationObserver;
use crate::query::query;

const DEFAULT_CONSENSUS_PAGE_LIMIT: i64 = 50;
const MIN_CONSENSUS_PAGE_LIMIT: i64 = 1;
const MAX_CONSENSUS_PAGE_LIMIT: i64 = 200;

/// Clamps a client-supplied `limit` to a sane range so a negative or
/// oversized value can never reach Postgres as e.g. `LIMIT -1` (which
/// Postgres treats as "no limit", not an error, but is still unbounded and
/// unintended).
fn clamp_limit(limit: Option<i64>) -> i64 {
    limit
        .unwrap_or(DEFAULT_CONSENSUS_PAGE_LIMIT)
        .clamp(MIN_CONSENSUS_PAGE_LIMIT, MAX_CONSENSUS_PAGE_LIMIT)
}

/// Floors a client-supplied cursor component at 0; negative cursor values
/// have no valid meaning and should not be forwarded to the query.
fn floor_non_negative(value: Option<i64>) -> Option<i64> {
    value.map(|v| v.max(0))
}

#[derive(Debug, Deserialize)]
pub(super) struct ConsensusStreamParams {
    filter: Option<String>,
    before_session: Option<i64>,
    before_item: Option<i64>,
    limit: Option<i64>,
}

pub(super) async fn consensus_stream(
    Path(federation_id): Path<FederationId>,
    Query(params): Query<ConsensusStreamParams>,
    State(state): State<AppState>,
) -> crate::error::Result<Json<ConsensusPage>> {
    let filter = params.filter.unwrap_or_else(|| "all".to_owned());
    let before_session = floor_non_negative(params.before_session);
    let before_item = floor_non_negative(params.before_item);
    let before = match (before_session, before_item) {
        (Some(session), Some(item)) => Some((session, item)),
        _ => None,
    };
    let limit = clamp_limit(params.limit);

    Ok(state
        .observer
        .federation_consensus_page(federation_id, &filter, before, limit)
        .await?
        .into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn negative_limit_is_clamped_not_passed_through() {
        assert_eq!(clamp_limit(Some(-1)), MIN_CONSENSUS_PAGE_LIMIT);
        assert_eq!(clamp_limit(Some(-1_000_000)), MIN_CONSENSUS_PAGE_LIMIT);
        assert_eq!(clamp_limit(Some(0)), MIN_CONSENSUS_PAGE_LIMIT);
        assert_eq!(clamp_limit(Some(100_000)), MAX_CONSENSUS_PAGE_LIMIT);
        assert_eq!(clamp_limit(None), DEFAULT_CONSENSUS_PAGE_LIMIT);
    }

    #[test]
    fn negative_before_is_floored_at_zero() {
        assert_eq!(floor_non_negative(Some(-5)), Some(0));
        assert_eq!(floor_non_negative(Some(5)), Some(5));
        assert_eq!(floor_non_negative(None), None);
    }
}

/// `pub(super)` (rather than private) so `api::live`'s live-session delta
/// query (`LIVE_QUERY`) can reuse this row shape + its `SessionItem`
/// conversion instead of re-assembling the enrichment.
#[derive(FromRow)]
pub(super) struct ConsensusItemRow {
    session_index: i64,
    item_index: i64,
    item_type: String,
    kind: Option<String>,
    peer_id: Option<i32>,
    txid: Option<String>,
    ecash_anon_bits: Option<f64>,
    user_tx_key: Option<String>,
    user_tx_kind: Option<String>,
    direction: Option<String>,
    details: Option<serde_json::Value>,
    /// The item's own first-seen live stamp (tx branch reads
    /// `transactions.synced_at`, ci branch `consensus_items.synced_at`).
    synced_at: Option<chrono::NaiveDateTime>,
    /// The session's forward-/backward-filled vote bounds.
    estimated_session_timestamp: Option<chrono::NaiveDateTime>,
    next_vote_time: Option<chrono::NaiveDateTime>,
    role: Option<String>,
}

impl From<ConsensusItemRow> for SessionItem {
    fn from(row: ConsensusItemRow) -> Self {
        let time = crate::api::time_estimate::resolve_time(
            row.synced_at,
            row.estimated_session_timestamp,
            row.next_vote_time,
        );
        SessionItem {
            session_index: row.session_index,
            item_index: row.item_index,
            item_type: row.item_type,
            kind: row.kind,
            peer_id: row.peer_id,
            txid: row.txid,
            ecash_anon_bits: row.ecash_anon_bits,
            user_tx_key: row.user_tx_key,
            user_tx_kind: row.user_tx_kind,
            direction: row.direction,
            details: row.details,
            estimated_time: time.estimated_time,
            time_lower: time.time_lower,
            time_upper: time.time_upper,
            time_source: time.time_source.map(str::to_owned),
            role: row.role,
        }
    }
}

// Each transaction-producing query below resolves `user_tx_key` +
// `user_tx_kind` + `direction` together via a single LATERAL join
// (`USER_TX_LATERAL`, shared with `sessions::federation_session_items`),
// avoiding row multiplication if a txid ever maps to more than one
// user_tx_key (LIMIT 1) and avoiding three separate correlated subqueries.
// language=postgresql
static TRANSACTION_ONLY_QUERY: LazyLock<String> = LazyLock::new(|| {
    format!(
        "
    SELECT t.session_index::bigint, t.item_index::bigint, 'transaction' AS item_type,
           NULL::text AS kind, NULL::int AS peer_id,
           encode(t.txid,'hex') AS txid,
           tp.ecash_anon_bits AS ecash_anon_bits,
           uxt.user_tx_key, uxt.user_tx_kind, uxt.direction,
           NULL::jsonb AS details,
           t.synced_at AS synced_at,
           st.estimated_session_timestamp AS estimated_session_timestamp,
           st.next_vote_time AS next_vote_time,
           uxt.role
    FROM transactions t
    {USER_TX_LATERAL}
    LEFT JOIN transaction_privacy tp ON tp.federation_id = t.federation_id AND tp.txid = t.txid
    LEFT JOIN session_times st ON st.federation_id = t.federation_id AND st.session_index = t.session_index
    WHERE t.federation_id = $1
      AND ($2::int IS NULL OR (t.session_index, t.item_index) < ($2::int, $3::int))
    ORDER BY t.session_index DESC, t.item_index DESC
    LIMIT $4
"
    )
});

// The keyset predicate and `LIMIT` are pushed into EACH `UNION ALL` branch
// (rather than applied once on the outer union) so that, regardless of
// planner choices, each branch is a bounded backward index scan on its own
// index (`transactions_by_session_item` / the `consensus_items` PK) that
// reads at most `$4` rows -- instead of depending on the planner pushing the
// outer predicate/limit through the appendrel on the 127M-row
// `consensus_items` table. The outer merge then sorts at most `2 * $4` rows,
// cheap independent of the plan chosen for either branch.
// language=postgresql
static ALL_QUERY: LazyLock<String> = LazyLock::new(|| {
    format!(
        "
    SELECT session_index, item_index, item_type, kind, peer_id, txid, ecash_anon_bits, user_tx_key, user_tx_kind, direction, details,
           synced_at, estimated_session_timestamp, next_vote_time, role
    FROM (
        ( SELECT t.session_index::bigint AS session_index, t.item_index::bigint AS item_index,
                 'transaction' AS item_type, NULL::text AS kind, NULL::int AS peer_id,
                 encode(t.txid,'hex') AS txid,
                 tp.ecash_anon_bits AS ecash_anon_bits,
                 uxt.user_tx_key, uxt.user_tx_kind, uxt.direction,
                 NULL::jsonb AS details,
                 t.synced_at AS synced_at,
                 st.estimated_session_timestamp AS estimated_session_timestamp,
                 st.next_vote_time AS next_vote_time,
                 uxt.role
          FROM transactions t
          {USER_TX_LATERAL}
          LEFT JOIN transaction_privacy tp ON tp.federation_id = t.federation_id AND tp.txid = t.txid
          LEFT JOIN session_times st ON st.federation_id = t.federation_id AND st.session_index = t.session_index
          WHERE t.federation_id = $1
            AND ($2::int IS NULL OR (t.session_index, t.item_index) < ($2::int, $3::int))
          ORDER BY t.session_index DESC, t.item_index DESC
          LIMIT $4 )
        UNION ALL
        ( SELECT ci.session_index::bigint, ci.item_index::bigint, 'ci', ci.kind, ci.peer_id,
                 NULL, NULL::double precision, NULL, NULL, NULL, ci.details,
                 ci.synced_at,
                 st.estimated_session_timestamp,
                 st.next_vote_time,
                 NULL::text AS role
          FROM consensus_items ci
          LEFT JOIN session_times st ON st.federation_id = ci.federation_id AND st.session_index = ci.session_index
          WHERE ci.federation_id = $1
            AND ($2::int IS NULL OR (ci.session_index, ci.item_index) < ($2::int, $3::int))
          ORDER BY ci.session_index DESC, ci.item_index DESC
          LIMIT $4 )
    ) u
    ORDER BY session_index DESC, item_index DESC
    LIMIT $4
"
    )
});

// language=postgresql
const KIND_QUERY: &str = "
    SELECT ci.session_index::bigint, ci.item_index::bigint, 'ci' AS item_type, ci.kind, ci.peer_id,
           NULL::text AS txid, NULL::double precision AS ecash_anon_bits,
           NULL::text AS user_tx_key, NULL::text AS user_tx_kind, NULL::text AS direction,
           ci.details,
           ci.synced_at AS synced_at,
           st.estimated_session_timestamp AS estimated_session_timestamp,
           st.next_vote_time AS next_vote_time,
           NULL::text AS role
    FROM consensus_items ci
    LEFT JOIN session_times st ON st.federation_id = ci.federation_id AND st.session_index = ci.session_index
    WHERE ci.federation_id = $1 AND ci.kind = $2
      AND ($3::int IS NULL OR (ci.session_index, ci.item_index) < ($3::int, $4::int))
    ORDER BY ci.session_index DESC, ci.item_index DESC
    LIMIT $5
";

impl FederationObserver {
    /// Federation-wide, keyset-paginated consensus item stream, newest
    /// first. `filter` is `"all"` (transactions ⊔ consensus items),
    /// `"transaction"` (transactions only), or a module `kind` (consensus
    /// items of that kind only). `before` is the `(session_index,
    /// item_index)` of the last item of the previous page (exclusive lower
    /// bound via row comparison); `None` starts from the newest item.
    pub async fn federation_consensus_page(
        &self,
        federation_id: FederationId,
        filter: &str,
        before: Option<(i64, i64)>,
        limit: i64,
    ) -> anyhow::Result<ConsensusPage> {
        self.get_federation(federation_id)
            .await
            .context("Federation doesn't exist")?;

        let fed_bytes = federation_id.consensus_encode_to_vec();
        let before_session = before.map(|(session, _)| session as i32);
        let before_item = before.map(|(_, item)| item as i32);

        let rows = match filter {
            "transaction" => {
                query::<ConsensusItemRow>(
                    &self.connection().await?,
                    TRANSACTION_ONLY_QUERY.as_str(),
                    &[&fed_bytes, &before_session, &before_item, &limit],
                )
                .await?
            }
            "all" => {
                query::<ConsensusItemRow>(
                    &self.connection().await?,
                    ALL_QUERY.as_str(),
                    &[&fed_bytes, &before_session, &before_item, &limit],
                )
                .await?
            }
            kind => {
                query::<ConsensusItemRow>(
                    &self.connection().await?,
                    KIND_QUERY,
                    &[&fed_bytes, &kind, &before_session, &before_item, &limit],
                )
                .await?
            }
        };

        let next = if (rows.len() as i64) < limit {
            None
        } else {
            rows.last().map(|row| (row.session_index, row.item_index))
        };

        Ok(ConsensusPage {
            items: rows.into_iter().map(SessionItem::from).collect(),
            next,
        })
    }
}

/// Builds the two composite indexes the consensus explorer needs. Uses
/// `CREATE INDEX CONCURRENTLY`, so the (one-time, potentially minutes-long on
/// the 127M-row `consensus_items` table) build does not block concurrent
/// writers — it is still `.await`ed by the caller before serving traffic, so
/// it *does* delay startup for its duration; `CONCURRENTLY` only buys
/// writer-non-blocking, not startup-non-blocking.
///
/// Note: `transactions_by_session` (2-column, no `item_index`) already
/// exists from the original core schema (`schema/core/v0.sql`), so the
/// desired 3-column composite index here is named
/// `transactions_by_session_item` to avoid colliding with it (`CREATE INDEX ...
/// IF NOT EXISTS` only checks the name, not the definition, and would otherwise
/// silently no-op).
pub async fn ensure_explorer_indexes(pool: &Pool) {
    const INDEXES: &[(&str, &str)] = &[
        (
            "transactions_by_session_item",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS transactions_by_session_item \
             ON transactions (federation_id, session_index, item_index)",
        ),
        (
            "consensus_items_stream",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS consensus_items_stream \
             ON consensus_items (federation_id, kind, session_index, item_index)",
        ),
    ];

    build_indexes_concurrently(pool, INDEXES).await;
}

/// Builds the partial `WHERE amount_msat IS NULL` indexes that keep
/// `amounts::infer_missing_amounts` off a full-table scan: without them it
/// seq-scans the multi-million-row `transaction_inputs`/`transaction_outputs`
/// tables every refresh cycle just to find its handful of NULL candidate rows,
/// which also made it hold a long `transactions` read-lock (via its subquery
/// join) that blocked deploy-time migrations. As a row's amount is filled --
/// by inference OR by a newly-added observer module decoding it exactly -- it
/// drops out of the partial index, which stays tiny. This changes NOTHING
/// about which rows are candidates: every NULL row stays eligible forever, so
/// a future module can still fill it with real data.
///
/// Unlike [`ensure_explorer_indexes`] this is spawned as a background task,
/// NOT awaited before serving: a `CREATE INDEX CONCURRENTLY` waits for
/// in-flight transactions to drain, so a slow `infer` cycle in progress could
/// otherwise stall startup — the exact bootstrap trap this fix removes. Built
/// in the background it lands whenever the DB allows, and `infer` is fast from
/// then on.
pub async fn ensure_infer_indexes(pool: &Pool) {
    const INDEXES: &[(&str, &str)] = &[
        (
            "transaction_inputs_null_amount",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS transaction_inputs_null_amount \
             ON transaction_inputs (federation_id, txid) WHERE amount_msat IS NULL",
        ),
        (
            "transaction_outputs_null_amount",
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS transaction_outputs_null_amount \
             ON transaction_outputs (federation_id, txid) WHERE amount_msat IS NULL",
        ),
    ];

    build_indexes_concurrently(pool, INDEXES).await;
}

/// Builds each `(name, sql)` index with `CREATE INDEX CONCURRENTLY` on its own
/// connection, outside any transaction. First drops a leftover INVALID index
/// of that name (a `CONCURRENTLY` build interrupted by a crash leaves one, and
/// `IF NOT EXISTS` only checks the name, not validity, so it would treat the
/// broken index as "present" forever). Idempotent and resilient: a per-index
/// failure is logged and does not stop the rest. `name` values are hardcoded
/// constants (not user input), so interpolating them via `format!` is safe;
/// the `DROP` is still identifier-quoted by Postgres's `format('DROP INDEX
/// %I', ...)`.
async fn build_indexes_concurrently(pool: &Pool, indexes: &[(&str, &str)]) {
    for (name, sql) in indexes {
        let conn = match pool.get().await {
            Ok(conn) => conn,
            Err(e) => {
                warn!("Could not get a connection to build index {name}: {e:?}");
                continue;
            }
        };

        let drop_if_invalid = format!(
            "DO $$ BEGIN
               IF EXISTS (
                 SELECT 1 FROM pg_class c
                 JOIN pg_index i ON i.indexrelid = c.oid
                 WHERE c.relname = '{name}' AND NOT i.indisvalid
               ) THEN
                 EXECUTE format('DROP INDEX %I', '{name}');
               END IF;
             END $$"
        );
        if let Err(e) = conn.execute(drop_if_invalid.as_str(), &[]).await {
            warn!("Could not check/drop invalid index {name} (continuing): {e:?}");
        }

        info!("Building index {name} (CONCURRENTLY, one-time)...");
        match conn.execute(*sql, &[]).await {
            Ok(_) => info!("Index {name} ready"),
            Err(e) => warn!(
                "Could not build index {name} (continuing; a concurrent build from a \
                 prior startup may already be in progress): {e:?}"
            ),
        }
    }
}
