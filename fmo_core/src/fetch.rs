use std::sync::Arc;
use std::time::SystemTime;

use deadpool_postgres::Pool;
use fedimint_api_client::api::DynGlobalApi;
use fedimint_connectors::ConnectorRegistry;
use fedimint_core::config::{ClientConfig, FederationId};
use fedimint_core::encoding::Encodable;
use fedimint_core::module::registry::ModuleDecoderRegistry;
use fedimint_core::util::backoff_util::background_backoff;
use fedimint_core::util::retry;
use futures::StreamExt;
use tokio::sync::watch;
use tracing::{debug, info};

use crate::ingest::ingest_session;
use crate::live::{run_live, Watermark};
use crate::query::query_value;
use crate::registry::ModuleRegistry;
use crate::services::CoreServices;

/// The session index a (re)starting fetcher should resume from: one past the
/// highest COMPLETE (`data IS NOT NULL`) session, or 0 if none exist yet.
///
/// The `data IS NOT NULL` filter is load-bearing: the live path leaves the
/// currently-open session as a `data = NULL` row for its whole lifetime, so an
/// unfiltered `MAX(session_index)` would resume PAST the open session on every
/// restart — permanently orphaning it (never finalized, dispatch/gold skip it).
/// Mirrors the same filter on `dispatch.rs`'s pending-range query.
pub async fn next_session_to_fetch(
    pool: &Pool,
    federation_id: FederationId,
) -> anyhow::Result<u64> {
    let next_session = query_value::<Option<i32>>(
        &pool.get().await?,
        "SELECT MAX(session_index) FROM sessions WHERE federation_id = $1 AND data IS NOT NULL",
        &[&federation_id.consensus_encode_to_vec()],
    )
    .await?
    .map(|max_session_index| max_session_index as u64 + 1)
    .unwrap_or(0);
    Ok(next_session)
}

/// Fetches sessions from the federation and writes raw session data plus
/// structural facts into the core tables. Does NOT do any module-specific
/// decoding — that is the dispatch engine's job.
///
/// Alternates between bounded catch-up (bulk `await_block` fetch of every
/// signed session not yet ingested) and live polling (Task 4's [`run_live`])
/// of the currently-open session once caught up. Never returns unless an
/// error occurs.
#[allow(clippy::too_many_arguments)]
pub async fn run_fetcher(
    pool: Pool,
    connectors: ConnectorRegistry,
    federation_id: FederationId,
    config: ClientConfig,
    registry: Arc<ModuleRegistry>,
    services: Arc<CoreServices>,
    watermark_tx: &watch::Sender<Watermark>,
) -> anyhow::Result<()> {
    let peers = config
        .global
        .api_endpoints
        .iter()
        .map(|(&peer_id, peer_url)| (peer_id, peer_url.url.clone()))
        .collect();
    let api = DynGlobalApi::new(connectors, peers, None)?;
    let decoders = ModuleRegistry::fallback_decoders();

    info!("Starting session fetcher for {federation_id}");
    let mut next_session = next_session_to_fetch(&pool, federation_id).await?;
    debug!("Next session {next_session}");

    loop {
        let count = api.session_count().await?;

        if next_session < count {
            // Bounded catch-up: bulk-fetch every signed session not yet
            // ingested. Bounded to `next_session..count` (not
            // `next_session..`) so the stream drains and we re-check
            // `session_count` -- otherwise we would never reach the live
            // phase below.
            next_session = catch_up(
                &pool,
                &api,
                &decoders,
                federation_id,
                &config,
                next_session,
                count,
            )
            .await?;
            continue;
        }

        // Caught up: session `next_session` (== count) is the currently
        // open one. Go live on it until it completes, then advance.
        run_live(
            &pool,
            &registry,
            &services,
            federation_id,
            &config,
            &api,
            &decoders,
            watermark_tx,
            next_session,
        )
        .await?;
        next_session += 1;
    }
}

/// Bulk-fetches and ingests every signed session in `next_session..count`
/// via `await_block`, buffered 32-wide. Returns the next session index to
/// fetch (== `count`) once the stream drains.
async fn catch_up(
    pool: &Pool,
    api: &DynGlobalApi,
    decoders: &ModuleDecoderRegistry,
    federation_id: FederationId,
    config: &ClientConfig,
    next_session: u64,
    count: u64,
) -> anyhow::Result<u64> {
    let mut session_stream = futures::stream::iter(next_session..count)
        .map(move |session_index| {
            debug!("Starting fetch job for session {session_index}");
            let api_fetch_single = api.clone();
            let decoders_single = decoders.clone();
            async move {
                let session_outcome = retry(
                    format!("Waiting for session {session_index}"),
                    background_backoff(),
                    || async {
                        api_fetch_single
                            .await_block(session_index, &decoders_single)
                            .await
                    },
                )
                .await
                .expect("Will fail after 136 years");
                debug!("Finished fetch job for session {session_index}");
                (session_index, session_outcome)
            }
        })
        .buffered(32);

    let mut timer = SystemTime::now();
    let mut last_session = next_session;
    while let Some((session_index, session_outcome)) = session_stream.next().await {
        let mut connection = pool.get().await?;
        let dbtx = connection.transaction().await?;
        ingest_session(
            &dbtx,
            config,
            federation_id,
            session_index,
            &session_outcome,
        )
        .await?;
        dbtx.commit().await?;

        let elapsed = timer.elapsed().unwrap_or_default();
        if elapsed >= std::time::Duration::from_secs(5) {
            let sessions_synced = session_index - last_session;
            let rate = (sessions_synced as f64) / elapsed.as_secs_f64();
            info!("Synced up to session {session_index}, processed {sessions_synced} sessions at a rate of {rate:.2} sessions/s");
            timer = SystemTime::now();
            last_session = session_index;
        }
    }

    // The stream drained `next_session..count` fully and in order (`buffered`
    // preserves input order), so every session up to `count - 1` is now
    // ingested; the caller re-reads `session_count` from here.
    Ok(count)
}
