//! SSE endpoint (`GET /federations/:id/live`) streaming enriched consensus
//! items of the federation's live (in-progress) session to the browser.
//!
//! The SSE channel itself carries only the
//! [`Watermark`](crate::live::Watermark) tick published by the live poller
//! (Task 4, `FederationObserver::live_watch`); this handler reacts to each tick
//! by re-querying the DB for the delta of items since the last cursor it sent,
//! via [`FederationObserver::federation_live_items`] below. This keeps the SSE
//! payload itself gold-classified (`user_tx_kind`/ `direction`) for free, since
//! it reuses SP-1's enriched consensus-item query (`ConsensusItemRow` +
//! `USER_TX_LATERAL`) rather than re-deriving classification from the raw item.

use std::convert::Infallible;
use std::sync::LazyLock;

use anyhow::anyhow;
use async_stream::stream;
use axum::extract::{Path, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use fedimint_core::config::FederationId;
use fedimint_core::encoding::Encodable;
use fmo_api_types::SessionItem;
use futures::Stream;
use tracing::warn;

use crate::api::consensus::ConsensusItemRow;
use crate::api::sql_fragments::USER_TX_LATERAL;
use crate::api::AppState;
use crate::live::Watermark;
use crate::observer::FederationObserver;
use crate::query::query;

// Mirrors `consensus::ALL_QUERY`'s two-branch tx ⊔ ci union (same columns,
// same `USER_TX_LATERAL` enrichment for the tx branch) but ASCENDING and
// bounded on BOTH ends: `after < (session,item) <= up_to`, rather than
// `ALL_QUERY`'s descending, single-sided (`< before`) keyset page. `after`
// is nullable (no lower bound) so the client's very first read -- from the
// start of the current live session -- can pass `None`.
//
// The per-branch `LIMIT` is a defensive cap, not a real pagination limit: a
// live (in-progress, unsigned) session's item count is always small, and the
// handler below scopes `after`/`up_to` to at most the current session, so in
// practice each branch returns far fewer rows than this.
// language=postgresql
static LIVE_QUERY: LazyLock<String> = LazyLock::new(|| {
    format!(
        "
    SELECT session_index, item_index, item_type, kind, peer_id, txid, ecash_anon_bits, ecash_issuance_bits, user_tx_key, user_tx_kind, direction, details,
           synced_at, estimated_session_timestamp, next_vote_time, role
    FROM (
        ( SELECT t.session_index::bigint AS session_index, t.item_index::bigint AS item_index,
                 'transaction' AS item_type, NULL::text AS kind, NULL::int AS peer_id,
                 encode(t.txid,'hex') AS txid,
                 tp.ecash_anon_bits AS ecash_anon_bits,
                 tp.ecash_issuance_bits AS ecash_issuance_bits,
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
            AND ($2::int IS NULL OR (t.session_index, t.item_index) > ($2::int, $3::int))
            AND (t.session_index, t.item_index) <= ($4::int, $5::int)
          ORDER BY t.session_index ASC, t.item_index ASC
          LIMIT 10000 )
        UNION ALL
        ( SELECT ci.session_index::bigint, ci.item_index::bigint, 'ci', ci.kind, ci.peer_id,
                 NULL, NULL::double precision, NULL::double precision, NULL, NULL, NULL, ci.details,
                 ci.synced_at,
                 st.estimated_session_timestamp,
                 st.next_vote_time,
                 NULL::text AS role
          FROM consensus_items ci
          LEFT JOIN session_times st ON st.federation_id = ci.federation_id AND st.session_index = ci.session_index
          WHERE ci.federation_id = $1
            AND ($2::int IS NULL OR (ci.session_index, ci.item_index) > ($2::int, $3::int))
            AND (ci.session_index, ci.item_index) <= ($4::int, $5::int)
          ORDER BY ci.session_index ASC, ci.item_index ASC
          LIMIT 10000 )
    ) u
    ORDER BY session_index ASC, item_index ASC
"
    )
});

impl FederationObserver {
    /// Ascending, bounded keyset delta over the federation-wide tx ⊔ ci
    /// union: rows with `after < (session_index, item_index) <= up_to`.
    /// `after = None` means no lower bound at all (from the very first
    /// session); callers that only want the current live session pass
    /// `Some((session_index, -1))` as the lower bound instead, as the `/live`
    /// SSE handler below does. Reuses
    /// SP-1's enriched item shape (`ConsensusItemRow`, `USER_TX_LATERAL`) so
    /// live items carry the same `user_tx_key`/`user_tx_kind`/`direction`
    /// gold classification as the historical consensus explorer.
    ///
    /// Used by the `/live` SSE handler below to tail only what's new since
    /// the client's own cursor on each watermark tick; not itself
    /// paginated (no client-facing `limit`) since the caller always bounds
    /// the query to at most one live session's worth of items.
    pub async fn federation_live_items(
        &self,
        federation_id: FederationId,
        after: Option<(i64, i64)>,
        up_to: (i64, i64),
    ) -> anyhow::Result<Vec<SessionItem>> {
        let fed_bytes = federation_id.consensus_encode_to_vec();
        let after_session = after.map(|(session, _)| session as i32);
        let after_item = after.map(|(_, item)| item as i32);
        let up_to_session = up_to.0 as i32;
        let up_to_item = up_to.1 as i32;

        let rows = query::<ConsensusItemRow>(
            &self.connection().await?,
            LIVE_QUERY.as_str(),
            &[
                &fed_bytes,
                &after_session,
                &after_item,
                &up_to_session,
                &up_to_item,
            ],
        )
        .await?;

        Ok(rows.into_iter().map(SessionItem::from).collect())
    }
}

/// Streams the live session's enriched consensus items to the browser as
/// they're accepted, via Server-Sent Events.
///
/// Unauthenticated at the app layer, matching its `/federations/*` siblings
/// (e.g. `/consensus`, `/sessions`): this route serves read-only public
/// data, same as the rest of the federation-monitoring API. The private
/// instance's bearer-auth gate is a deploy-time proxy concern (Task 6), not
/// applied here -- `EventSource` cannot send custom headers, so an
/// app-level bearer check would not be usable by the browser client anyway.
pub(super) async fn federation_live(
    Path(federation_id): Path<FederationId>,
    State(state): State<AppState>,
) -> crate::error::Result<Sse<impl Stream<Item = Result<Event, Infallible>>>> {
    let mut rx = state
        .observer
        .live_watch(federation_id)
        .ok_or_else(|| anyhow!("No live state for federation {federation_id}"))?;
    let observer = state.observer;

    let stream = stream! {
        // Start of the current live session (exclusive lower bound at
        // item -1, i.e. "before item 0"), upper bound at the watermark's
        // current position. Scoping the lower bound to the *current*
        // session -- not an open `None` lower bound -- is deliberate: this
        // is a live tail, not a history replay, so a client that connects
        // mid-session only backfills that session's items so far, not the
        // federation's entire history.
        let wm = rx.borrow_and_update().clone();
        let mut cursor = (wm.session_index, -1i64);

        // Skip the initial read while the watermark is still `Watermark::default()`
        // (session 0, item 0, not rolled over). That value means no *live* poll
        // has ticked yet -- the fetcher is still in `catch_up`, or the open
        // session is empty/`Initial` -- NOT that session 0 item 0 is genuinely
        // live. Reading with it would emit that stale historical item as if it
        // were live (rendering a phantom "Session 0" in the client). The loop
        // below serves everything once the first real live poll ticks.
        if wm != Watermark::default() {
            match observer
                .federation_live_items(federation_id, Some(cursor), (wm.session_index, wm.item_index))
                .await
            {
                Ok(items) => {
                    for item in items {
                        cursor = (item.session_index, item.item_index);
                        match Event::default().json_data(&item) {
                            Ok(event) => yield Ok(event),
                            Err(e) => warn!("Failed to encode live session item as SSE event: {e:?}"),
                        }
                    }
                }
                Err(e) => {
                    warn!(%federation_id, "federation_live_items failed on initial live read: {e:?}")
                }
            }
        }

        loop {
            // Sender dropped (fetcher task gone) -- end the stream.
            if rx.changed().await.is_err() {
                break;
            }
            let wm = rx.borrow_and_update().clone();

            // Clamp the lower bound only when `cursor` is MORE THAN one session
            // behind `wm.session_index` -- i.e. a genuine restart gap (a fresh
            // connection's watch starts at `Watermark::default()` and, after a
            // process restart, the first tick can jump straight from session 0
            // to whatever session is live now). Reading from a far-behind
            // `cursor` there would span the whole gap as an ascending
            // full-history scan, so we jump to `(wm.session_index, -1)`.
            //
            // A cursor exactly ONE session behind is the NORMAL rollover case:
            // session N just signed and N+1 is live. We must NOT clamp then --
            // `finalize_live_session` commits session N's tail before publishing
            // the final watermark, and if that watermark coalesces (watch keeps
            // only the latest) with N+1's first, clamping to `(N+1, -1)` would
            // skip N's un-served tail. Keeping the fine cursor lets the delta
            // span `(N, cursor..last] ∪ (N+1, 0..]` with no gap or overlap.
            let lower = if cursor.0 < wm.session_index - 1 {
                (wm.session_index, -1i64)
            } else {
                cursor
            };

            match observer
                .federation_live_items(federation_id, Some(lower), (wm.session_index, wm.item_index))
                .await
            {
                Ok(items) => {
                    for item in items {
                        cursor = (item.session_index, item.item_index);
                        match Event::default().json_data(&item) {
                            Ok(event) => yield Ok(event),
                            Err(e) => {
                                warn!("Failed to encode live session item as SSE event: {e:?}");
                            }
                        }
                    }
                }
                Err(e) => warn!(%federation_id, "federation_live_items failed on live delta read: {e:?}"),
            }

            // Yielded AFTER the delta items so the client appends the
            // completed session's tail before clearing for the next one.
            // No cursor reset needed: `cursor` now sits at the completed
            // session's last item `(s, last_item)`, and the next session's
            // items `(s+1, j)` satisfy `(s+1, j) > (s, last_item)` for any
            // `j >= 0` -- the same exclusive-lower-bound comparison handles
            // the session boundary uniformly.
            if wm.rolled_over {
                yield Ok(Event::default()
                    .event("rollover")
                    .data(wm.session_index.to_string()));
            }
        }
    };

    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}
