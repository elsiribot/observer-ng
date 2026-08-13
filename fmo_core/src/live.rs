//! Live processing: the pure per-poll step (Task 3) that the live fetch loop
//! (Task 4) calls as guardians accept new items into an in-progress session,
//! plus the finalize step run once the session signs, plus (Task 4) the
//! actual network-facing poll loop and the watermark type used to publish
//! live-poll progress to the SSE handler (Task 5).
//!
//! `live_process`/`finalize_live_session` are the pure per-poll/finalize DB
//! steps; `run_live` is the only network-facing piece in this module.

use std::sync::Arc;
use std::time::Duration;

use deadpool_postgres::Pool;
use fedimint_api_client::api::DynGlobalApi;
use fedimint_core::config::{ClientConfig, FederationId};
use fedimint_core::encoding::Encodable;
use fedimint_core::module::ApiVersion;
use fedimint_core::session_outcome::{AcceptedItem, SessionStatus};
use tokio::sync::watch;
use tracing::warn;

use crate::dispatch::dispatch_items_to_module;
use crate::gold::fold_sessions;
use crate::ingest::ingest_items;
use crate::registry::ModuleRegistry;
use crate::services::CoreServices;

/// Live-poll progress for one federation, published on a `watch` channel so
/// the SSE handler (Task 5) can stream updates without polling the DB.
///
/// `Default` (all zero, `rolled_over: false`) represents "no live poll has
/// reported anything yet" -- it is intentionally indistinguishable from
/// "session 0, item 0, not yet rolled over" since a fresh federation starts
/// there anyway.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Watermark {
    pub session_index: i64,
    pub item_index: i64,
    pub rolled_over: bool,
}

/// How long `run_live` sleeps between polls of a not-yet-complete session,
/// overridable via `FO_LIVE_POLL_SECS` (default 1s).
fn live_poll_interval() -> Duration {
    let secs = std::env::var("FO_LIVE_POLL_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(1);
    Duration::from_secs(secs)
}

/// Live-polls a single session (`session_index`) until it completes, calling
/// [`live_process`] on each newly observed batch of items and
/// [`finalize_live_session`] once the session signs.
///
/// Returns `Ok(())` only once the session is `Complete` -- the caller (the
/// fetcher's catch-up/live transition loop) is responsible for advancing to
/// the next session index; this function never loops across sessions.
#[allow(clippy::too_many_arguments)]
pub async fn run_live(
    pool: &Pool,
    registry: &ModuleRegistry,
    services: &Arc<CoreServices>,
    federation_id: FederationId,
    config: &ClientConfig,
    api: &DynGlobalApi,
    wm: &watch::Sender<Watermark>,
    session_index: u64,
) -> anyhow::Result<()> {
    // Decode live session items with the federation's REAL module decoders so
    // dispatch sees typed items. Derived here from (registry, config) rather
    // than passed in, so the empty fallback registry can never leak into the
    // live path — that bug made every live item `DynUnknown`, every module
    // downcast fail, and amounts/details store NULL for the whole live tail.
    // `.decoders()` appends a fallback, so genuinely-unknown module kinds still
    // degrade gracefully.
    let decoders = registry.decoders(config);
    let mut next_item_index = 0usize;

    loop {
        // V1 (core_api_version below the V2 threshold, no broadcast keys):
        // routes to the plain SESSION_STATUS_ENDPOINT consensus request, no
        // signature verification needed for the live/pending path.
        match api
            .get_session_status(session_index, &decoders, ApiVersion::new(0, 0), None)
            .await
        {
            Ok(SessionStatus::Initial) => {
                tokio::time::sleep(live_poll_interval()).await;
            }
            Ok(SessionStatus::Pending(items)) => {
                if items.len() > next_item_index {
                    live_process(
                        pool,
                        registry,
                        services,
                        federation_id,
                        config,
                        session_index,
                        &items,
                        next_item_index,
                    )
                    .await?;
                    next_item_index = items.len();
                    let _ = wm.send(Watermark {
                        session_index: session_index as i64,
                        item_index: items.len() as i64 - 1,
                        rolled_over: false,
                    });
                }
                tokio::time::sleep(live_poll_interval()).await;
            }
            Ok(SessionStatus::Complete(outcome)) => {
                let data = outcome.consensus_encode_to_vec();
                finalize_live_session(
                    pool,
                    registry,
                    services,
                    federation_id,
                    config,
                    session_index,
                    &outcome.items,
                    next_item_index,
                    &data,
                    None,
                )
                .await?;
                let _ = wm.send(Watermark {
                    session_index: session_index as i64,
                    item_index: outcome.items.len().saturating_sub(1) as i64,
                    rolled_over: true,
                });
                return Ok(());
            }
            Err(e) => {
                warn!(session_index, "get_session_status failed: {e:#}");
                tokio::time::sleep(live_poll_interval()).await;
            }
        }
    }
}

/// Processes `items[start..]` of an in-progress (possibly still-open)
/// session: structural ingest, then every installed module's dispatch hooks,
/// then a best-effort gold fold of this session -- all in one transaction.
///
/// This is the same ingest/dispatch code historical replay uses
/// ([`ingest::ingest_items`] / [`dispatch::dispatch_items_to_module`]), just
/// called with `start = next_item_index` as the live loop polls a session
/// that is still filling in. It does **not** touch `module_progress`: the
/// module cursor only advances once a session is finalized (signed) via
/// [`finalize_live_session`], so a crash mid-poll simply means the next poll
/// re-derives the same (idempotent) rows from `start` again.
///
/// The gold fold here is best-effort *classification* only (kind/direction).
/// Folding a session before `session_times` and amount-inference have run
/// for it is an expected race -- `gold::heal_gold` repairs `first_timestamp`
/// and inferred amounts later on the normal refresh cycle. Do not try to
/// backfill those here.
///
/// Returns immediately (before opening a transaction) if `start >=
/// items.len()`: nothing new to process. This also guards against an
/// `items[start..]` panic for the common "no new items this poll" case,
/// where `start == items.len()`.
#[allow(clippy::too_many_arguments)]
pub async fn live_process(
    pool: &Pool,
    registry: &ModuleRegistry,
    services: &Arc<CoreServices>,
    federation_id: FederationId,
    config: &ClientConfig,
    session_index: u64,
    items: &[AcceptedItem],
    start: usize,
) -> anyhow::Result<()> {
    if start >= items.len() {
        return Ok(());
    }

    let federation_id_bytes = federation_id.consensus_encode_to_vec();

    let mut conn = pool.get().await?;
    let dbtx = conn.transaction().await?;

    // First-seen wall-clock stamp for the items this live poll newly observes.
    // Computed once per call so all items in this batch share one stamp;
    // `ingest_items`' `ON CONFLICT DO NOTHING` keeps the earliest one if a
    // later poll or historical replay re-touches the same row.
    let synced_at = chrono::Utc::now().naive_utc();
    ingest_items(
        &dbtx,
        config,
        federation_id,
        session_index,
        items,
        start,
        Some(synced_at),
    )
    .await?;

    for (kind, module) in registry.iter() {
        // Matches `process_module_batch`'s convention: a module's dispatch
        // hooks write to its own schema with unqualified table names, so
        // `search_path` must point there (falling back to `public` for the
        // core tables `dispatch_items_to_module` writes amounts/details
        // back into).
        dbtx.batch_execute(&format!(
            "SET LOCAL search_path TO {}, public",
            crate::db::migrations::schema_name(kind.as_str())
        ))
        .await?;
        dispatch_items_to_module(
            &dbtx,
            module.as_ref(),
            services,
            federation_id,
            config,
            session_index,
            items,
            start,
        )
        .await?;
    }

    // Reset back to the default search_path before the gold fold, which
    // (like `run_gold_processor`) assumes unqualified names resolve in
    // `public`.
    dbtx.batch_execute("SET LOCAL search_path TO public")
        .await?;

    fold_sessions(
        &dbtx,
        &federation_id_bytes,
        session_index as i32,
        session_index as i32 + 1,
    )
    .await?;

    dbtx.commit().await?;
    Ok(())
}

/// Reconciles and finalizes a session that just signed: backfills any tail
/// the live poller hadn't caught up to yet, stores the real (signed)
/// session `data`/`signature`, and -- only for modules whose cursor has
/// already reached this session -- advances `module_progress` past it.
///
/// `final_items`/`data`/`signature` come from the authoritative signed
/// `SessionOutcome`; `processed_count` is how many of its items the live
/// poller believes it already processed (should always be a prefix of
/// `final_items`, i.e. `<= final_items.len()`).
///
/// The `module_progress` advance is conditional
/// (`next_session_index = session_index` in the `WHERE` clause): the
/// separate `run_processor`/`run_gold_processor` background tasks replay
/// historical sessions independently and may still be behind this session
/// index when it finalizes live. An unconditional advance to
/// `session_index + 1` would then skip the un-dispatched sessions between
/// the module's real cursor and here. When the module is behind, this
/// `UPDATE` simply matches 0 rows and `run_processor` dispatches the now
/// data-complete session and advances the cursor itself once it catches up,
/// idempotently. `gold_progress` is not touched here for the same reason --
/// it follows `module_progress` via the normal `run_gold_processor` cycle.
#[allow(clippy::too_many_arguments)]
pub async fn finalize_live_session(
    pool: &Pool,
    registry: &ModuleRegistry,
    services: &Arc<CoreServices>,
    federation_id: FederationId,
    config: &ClientConfig,
    session_index: u64,
    final_items: &[AcceptedItem],
    processed_count: usize,
    data: &[u8],
    signature: Option<&[u8]>,
) -> anyhow::Result<()> {
    // Reconcile: process whatever tail the live poller hasn't seen yet.
    // `final_items` is authoritative -- live-observed counts should only
    // ever have been a prefix of it, but reconcile defensively either way.
    live_process(
        pool,
        registry,
        services,
        federation_id,
        config,
        session_index,
        final_items,
        processed_count,
    )
    .await?;

    if final_items.len() != processed_count {
        tracing::warn!(
            session_index,
            final_items_len = final_items.len(),
            processed_count,
            "finalize_live_session: final item count did not match live-processed count; \
             backfilled the tail from the authoritative final_items"
        );
    }

    let federation_id_bytes = federation_id.consensus_encode_to_vec();
    let session_index_i32 = session_index as i32;

    let mut conn = pool.get().await?;
    let dbtx = conn.transaction().await?;

    dbtx.execute(
        "UPDATE sessions SET data = $3, signature = $4
         WHERE federation_id = $1 AND session_index = $2",
        &[&federation_id_bytes, &session_index_i32, &data, &signature],
    )
    .await?;

    for (kind, _module) in registry.iter() {
        dbtx.execute(
            "UPDATE module_progress SET next_session_index = $3
             WHERE federation_id = $1 AND module_kind = $2 AND next_session_index = $4",
            &[
                &federation_id_bytes,
                &kind.as_str(),
                &(session_index_i32 + 1),
                &session_index_i32,
            ],
        )
        .await?;
    }

    dbtx.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::Watermark;

    #[test]
    fn watermark_default_is_zeroed_and_not_rolled_over() {
        let wm = Watermark::default();
        assert_eq!(wm.session_index, 0);
        assert_eq!(wm.item_index, 0);
        assert!(!wm.rolled_over);
    }

    #[test]
    fn watermark_equality_is_field_wise() {
        let a = Watermark {
            session_index: 3,
            item_index: 7,
            rolled_over: false,
        };
        let b = a.clone();
        assert_eq!(a, b);

        let c = Watermark {
            rolled_over: true,
            ..a.clone()
        };
        assert_ne!(a, c);
    }
}
