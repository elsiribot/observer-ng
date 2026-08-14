use std::sync::LazyLock;

use anyhow::Context;
use axum::extract::{Path, Query, State};
use axum::Json;
use fedimint_core::config::FederationId;
use fedimint_core::encoding::Encodable;
use fmo_api_types::{SessionItem, SessionSummary};
use postgres_from_row::FromRow;
use serde::Deserialize;

use crate::api::sql_fragments::USER_TX_LATERAL;
use crate::api::AppState;
use crate::observer::FederationObserver;
use crate::query::{query, query_value};

#[derive(Debug, Deserialize)]
pub(super) struct SessionPageParams {
    before: Option<i64>,
    limit: Option<i64>,
}

const DEFAULT_SESSION_PAGE_LIMIT: i64 = 50;
const MIN_SESSION_PAGE_LIMIT: i64 = 1;
const MAX_SESSION_PAGE_LIMIT: i64 = 200;

/// Clamps a client-supplied `limit` to a sane range so a negative or
/// oversized value can never reach Postgres as e.g. `LIMIT -1` (which
/// Postgres treats as "no limit", not an error, but is still unbounded and
/// unintended).
fn clamp_limit(limit: Option<i64>) -> i64 {
    limit
        .unwrap_or(DEFAULT_SESSION_PAGE_LIMIT)
        .clamp(MIN_SESSION_PAGE_LIMIT, MAX_SESSION_PAGE_LIMIT)
}

/// Floors a client-supplied cursor at 0; a negative cursor has no valid
/// meaning and should not be forwarded to the query.
fn floor_non_negative(value: Option<i64>) -> Option<i64> {
    value.map(|v| v.max(0))
}

pub(super) async fn list_sessions(
    Path(federation_id): Path<FederationId>,
    Query(params): Query<SessionPageParams>,
    State(state): State<AppState>,
) -> crate::error::Result<Json<Vec<SessionSummary>>> {
    Ok(state
        .observer
        .federation_session_page(
            federation_id,
            floor_non_negative(params.before),
            clamp_limit(params.limit),
        )
        .await?
        .into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn negative_limit_is_clamped_not_passed_through() {
        assert_eq!(clamp_limit(Some(-1)), MIN_SESSION_PAGE_LIMIT);
        assert_eq!(clamp_limit(Some(-1_000_000)), MIN_SESSION_PAGE_LIMIT);
        assert_eq!(clamp_limit(Some(0)), MIN_SESSION_PAGE_LIMIT);
        assert_eq!(clamp_limit(Some(100_000)), MAX_SESSION_PAGE_LIMIT);
        assert_eq!(clamp_limit(None), DEFAULT_SESSION_PAGE_LIMIT);
    }

    #[test]
    fn negative_before_is_floored_at_zero() {
        assert_eq!(floor_non_negative(Some(-5)), Some(0));
        assert_eq!(floor_non_negative(Some(5)), Some(5));
        assert_eq!(floor_non_negative(None), None);
    }
}

pub(super) async fn count_sessions(
    Path(federation_id): Path<FederationId>,
    State(state): State<AppState>,
) -> crate::error::Result<Json<u64>> {
    Ok(state
        .observer
        .federation_session_count(federation_id)
        .await?
        .into())
}

pub(super) async fn session_items(
    Path((federation_id, session_index)): Path<(FederationId, i64)>,
    State(state): State<AppState>,
) -> crate::error::Result<Json<Vec<SessionItem>>> {
    Ok(state
        .observer
        .federation_session_items(federation_id, session_index)
        .await?
        .into())
}

#[derive(FromRow)]
struct SessionSummaryRow {
    session_index: i64,
    estimated_session_timestamp: Option<chrono::NaiveDateTime>,
    next_vote_time: Option<chrono::NaiveDateTime>,
    tx_count: i64,
    items_by_kind: serde_json::Value,
    /// Ascending peer ids that contributed >=1 CI; NULL (→ empty) for a
    /// session with no consensus items.
    guardians: Option<Vec<i32>>,
}

impl From<SessionSummaryRow> for SessionSummary {
    fn from(row: SessionSummaryRow) -> Self {
        // Session-level: no per-item `synced_at`, so the source is only ever
        // "voted"/"interpolated"/None.
        let time = crate::api::time_estimate::resolve_time(
            None,
            row.estimated_session_timestamp,
            row.next_vote_time,
        );
        SessionSummary {
            session_index: row.session_index,
            estimated_time: time.estimated_time,
            time_lower: time.time_lower,
            time_upper: time.time_upper,
            time_source: time.time_source.map(str::to_owned),
            tx_count: row.tx_count,
            items_by_kind: row.items_by_kind,
            guardians: row
                .guardians
                .unwrap_or_default()
                .into_iter()
                .map(|id| id as u16)
                .collect(),
        }
    }
}

#[derive(FromRow)]
struct SessionItemRow {
    item_index: i64,
    item_type: String,
    kind: Option<String>,
    peer_id: Option<i32>,
    txid: Option<String>,
    user_tx_key: Option<String>,
    user_tx_kind: Option<String>,
    direction: Option<String>,
    details: Option<serde_json::Value>,
    synced_at: Option<chrono::NaiveDateTime>,
    estimated_session_timestamp: Option<chrono::NaiveDateTime>,
    next_vote_time: Option<chrono::NaiveDateTime>,
    role: Option<String>,
}

impl FederationObserver {
    /// Keyset-paginated session list, newest first, backed by the
    /// precomputed `session_stats` table (no counting at request time).
    pub async fn federation_session_page(
        &self,
        federation_id: FederationId,
        before: Option<i64>,
        limit: i64,
    ) -> anyhow::Result<Vec<SessionSummary>> {
        self.get_federation(federation_id)
            .await
            .context("Federation doesn't exist")?;

        // language=postgresql
        // The `guardians` array is computed by a correlated subquery over
        // `consensus_items` (keyed on the PK prefix `(federation_id,
        // session_index)`) and evaluated only for the <=200 rows the LIMIT
        // emits, so it stays cheap despite scanning per session.
        const QUERY: &str = "
            SELECT ss.session_index::bigint,
                   st.estimated_session_timestamp AS estimated_session_timestamp,
                   st.next_vote_time AS next_vote_time,
                   ss.tx_count::bigint, ss.items_by_kind,
                   (SELECT array_agg(DISTINCT ci.peer_id ORDER BY ci.peer_id)
                    FROM consensus_items ci
                    WHERE ci.federation_id = ss.federation_id
                      AND ci.session_index = ss.session_index) AS guardians
            FROM session_stats ss
            LEFT JOIN session_times st ON st.federation_id = ss.federation_id AND st.session_index = ss.session_index
            WHERE ss.federation_id = $1 AND ($2::int IS NULL OR ss.session_index < $2)
            ORDER BY ss.session_index DESC
            LIMIT $3
        ";

        let before = before.map(|b| b as i32);
        let rows = query::<SessionSummaryRow>(
            &self.connection().await?,
            QUERY,
            &[&federation_id.consensus_encode_to_vec(), &before, &limit],
        )
        .await?;

        Ok(rows.into_iter().map(SessionSummary::from).collect())
    }

    pub async fn federation_session_count(
        &self,
        federation_id: FederationId,
    ) -> anyhow::Result<u64> {
        let session_count =
            query_value::<i64>(
                &self.connection().await?,
                "SELECT COALESCE(COUNT(session_index), 0) as max_session_index FROM sessions WHERE federation_id = $1",
                &[&federation_id.consensus_encode_to_vec()]
            ).await?;
        Ok(session_count as u64)
    }

    /// Full ordered item list (transactions ⊔ consensus items) of one
    /// session.
    pub async fn federation_session_items(
        &self,
        federation_id: FederationId,
        session_index: i64,
    ) -> anyhow::Result<Vec<SessionItem>> {
        self.get_federation(federation_id)
            .await
            .context("Federation doesn't exist")?;

        // language=postgresql
        static QUERY: LazyLock<String> = LazyLock::new(|| {
            format!(
                "
            SELECT t.item_index::bigint, 'transaction' AS item_type, NULL::text AS kind, NULL::int AS peer_id,
                   encode(t.txid,'hex') AS txid,
                   uxt.user_tx_key, uxt.user_tx_kind, uxt.direction,
                   NULL::jsonb AS details,
                   t.synced_at AS synced_at,
                   st.estimated_session_timestamp AS estimated_session_timestamp,
                   st.next_vote_time AS next_vote_time,
                   uxt.role
            FROM transactions t
            {USER_TX_LATERAL}
            LEFT JOIN session_times st ON st.federation_id = t.federation_id AND st.session_index = t.session_index
            WHERE t.federation_id=$1 AND t.session_index=$2
            UNION ALL
            SELECT ci.item_index::bigint, 'ci', ci.kind, ci.peer_id, NULL, NULL, NULL, NULL, ci.details,
                   ci.synced_at,
                   st.estimated_session_timestamp,
                   st.next_vote_time,
                   NULL::text AS role
            FROM consensus_items ci
            LEFT JOIN session_times st ON st.federation_id = ci.federation_id AND st.session_index = ci.session_index
            WHERE ci.federation_id=$1 AND ci.session_index=$2
            ORDER BY 1
        "
            )
        });

        let rows = query::<SessionItemRow>(
            &self.connection().await?,
            QUERY.as_str(),
            &[
                &federation_id.consensus_encode_to_vec(),
                &(session_index as i32),
            ],
        )
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| {
                let time = crate::api::time_estimate::resolve_time(
                    row.synced_at,
                    row.estimated_session_timestamp,
                    row.next_vote_time,
                );
                SessionItem {
                    session_index,
                    item_index: row.item_index,
                    item_type: row.item_type,
                    kind: row.kind,
                    peer_id: row.peer_id,
                    txid: row.txid,
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
            })
            .collect())
    }
}
