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

use crate::api::AppState;
use crate::observer::FederationObserver;
use crate::query::query;

const DEFAULT_CONSENSUS_PAGE_LIMIT: i64 = 50;

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
    let before = match (params.before_session, params.before_item) {
        (Some(session), Some(item)) => Some((session, item)),
        _ => None,
    };

    Ok(state
        .observer
        .federation_consensus_page(
            federation_id,
            &filter,
            before,
            params.limit.unwrap_or(DEFAULT_CONSENSUS_PAGE_LIMIT),
        )
        .await?
        .into())
}

#[derive(FromRow)]
struct ConsensusItemRow {
    session_index: i64,
    item_index: i64,
    item_type: String,
    kind: Option<String>,
    peer_id: Option<i32>,
    txid: Option<String>,
    user_tx_key: Option<String>,
    user_tx_kind: Option<String>,
    direction: Option<String>,
    details: Option<serde_json::Value>,
}

impl From<ConsensusItemRow> for SessionItem {
    fn from(row: ConsensusItemRow) -> Self {
        SessionItem {
            session_index: row.session_index,
            item_index: row.item_index,
            item_type: row.item_type,
            kind: row.kind,
            peer_id: row.peer_id,
            txid: row.txid,
            user_tx_key: row.user_tx_key,
            user_tx_kind: row.user_tx_kind,
            direction: row.direction,
            details: row.details,
        }
    }
}

// Each transaction-producing query below resolves `user_tx_key` +
// `user_tx_kind` + `direction` together via a single LATERAL join, avoiding
// row multiplication if a txid ever maps to more than one user_tx_key
// (LIMIT 1) and avoiding three separate correlated subqueries.
// language=postgresql
const TRANSACTION_ONLY_QUERY: &str = "
    SELECT t.session_index::bigint, t.item_index::bigint, 'transaction' AS item_type,
           NULL::text AS kind, NULL::int AS peer_id,
           encode(t.txid,'hex') AS txid,
           uxt.user_tx_key, uxt.user_tx_kind, uxt.direction,
           NULL::jsonb AS details
    FROM transactions t
    LEFT JOIN LATERAL (
        SELECT encode(utt.user_tx_key,'hex') AS user_tx_key, ut.kind AS user_tx_kind, ut.direction
        FROM user_transaction_txs utt
        JOIN user_transactions ut
          ON ut.federation_id = utt.federation_id AND ut.user_tx_key = utt.user_tx_key
        WHERE utt.federation_id = t.federation_id AND utt.txid = t.txid
        LIMIT 1
    ) uxt ON true
    WHERE t.federation_id = $1
      AND ($2::int IS NULL OR (t.session_index, t.item_index) < ($2::int, $3::int))
    ORDER BY t.session_index DESC, t.item_index DESC
    LIMIT $4
";

// language=postgresql
const ALL_QUERY: &str = "
    SELECT * FROM (
        SELECT t.session_index::bigint AS session_index, t.item_index::bigint AS item_index,
               'transaction' AS item_type, NULL::text AS kind, NULL::int AS peer_id,
               encode(t.txid,'hex') AS txid,
               uxt.user_tx_key, uxt.user_tx_kind, uxt.direction,
               NULL::jsonb AS details
        FROM transactions t
        LEFT JOIN LATERAL (
            SELECT encode(utt.user_tx_key,'hex') AS user_tx_key, ut.kind AS user_tx_kind, ut.direction
            FROM user_transaction_txs utt
            JOIN user_transactions ut
              ON ut.federation_id = utt.federation_id AND ut.user_tx_key = utt.user_tx_key
            WHERE utt.federation_id = t.federation_id AND utt.txid = t.txid
            LIMIT 1
        ) uxt ON true
        WHERE t.federation_id = $1
        UNION ALL
        SELECT ci.session_index::bigint, ci.item_index::bigint, 'ci', ci.kind, ci.peer_id,
               NULL, NULL, NULL, NULL, ci.details
        FROM consensus_items ci
        WHERE ci.federation_id = $1
    ) combined
    WHERE ($2::int IS NULL OR (session_index, item_index) < ($2::int, $3::int))
    ORDER BY session_index DESC, item_index DESC
    LIMIT $4
";

// language=postgresql
const KIND_QUERY: &str = "
    SELECT ci.session_index::bigint, ci.item_index::bigint, 'ci' AS item_type, ci.kind, ci.peer_id,
           NULL::text AS txid, NULL::text AS user_tx_key, NULL::text AS user_tx_kind, NULL::text AS direction,
           ci.details
    FROM consensus_items ci
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
                    TRANSACTION_ONLY_QUERY,
                    &[&fed_bytes, &before_session, &before_item, &limit],
                )
                .await?
            }
            "all" => {
                query::<ConsensusItemRow>(
                    &self.connection().await?,
                    ALL_QUERY,
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

/// Builds the two composite indexes the consensus explorer needs, via
/// `CREATE INDEX CONCURRENTLY` so the (one-time, minutes-long on the 127M-row
/// `consensus_items` table) build doesn't block writers. Must run outside any
/// transaction — each statement is issued individually on its own
/// connection. Idempotent (`IF NOT EXISTS`) and resilient: a failure (e.g. a
/// build already in progress from a prior crashed startup) is logged and
/// does not stop the caller.
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

    for (name, sql) in INDEXES {
        let conn = match pool.get().await {
            Ok(conn) => conn,
            Err(e) => {
                warn!("Could not get a connection to build explorer index {name}: {e:?}");
                continue;
            }
        };

        info!("Building explorer index {name} (CONCURRENTLY, one-time, non-blocking)...");
        match conn.execute(*sql, &[]).await {
            Ok(_) => info!("Explorer index {name} ready"),
            Err(e) => warn!(
                "Could not build explorer index {name} (continuing; a concurrent build from a \
                 prior startup may already be in progress): {e:?}"
            ),
        }
    }
}
