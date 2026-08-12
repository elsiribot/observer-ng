//! Live processing: the pure per-poll step (Task 3) that the live fetch loop
//! (a later task) calls as guardians accept new items into an in-progress
//! session, plus the finalize step run once the session signs.
//!
//! No network/watch/SSE code lives here -- just DB-testable functions that
//! run ingest -> module dispatch -> gold fold for a slice of items, in one
//! transaction, so a poll is atomic and safe to retry.

use std::sync::Arc;

use deadpool_postgres::Pool;
use fedimint_core::config::{ClientConfig, FederationId};
use fedimint_core::encoding::Encodable;
use fedimint_core::session_outcome::AcceptedItem;

use crate::dispatch::dispatch_items_to_module;
use crate::gold::fold_sessions;
use crate::ingest::ingest_items;
use crate::registry::ModuleRegistry;
use crate::services::CoreServices;

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

    ingest_items(&dbtx, config, federation_id, session_index, items, start).await?;

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
