use std::time::SystemTime;

use deadpool_postgres::Pool;
use fedimint_api_client::api::DynGlobalApi;
use fedimint_connectors::ConnectorRegistry;
use fedimint_core::config::{ClientConfig, FederationId};
use fedimint_core::encoding::Encodable;
use fedimint_core::util::backoff_util::background_backoff;
use fedimint_core::util::retry;
use futures::StreamExt;
use tracing::{debug, info};

use crate::ingest::ingest_session;
use crate::query::query_value;
use crate::registry::ModuleRegistry;

/// Fetches sessions from the federation and writes raw session data plus
/// structural facts into the core tables. Does NOT do any module-specific
/// decoding — that is the dispatch engine's job.
///
/// Never returns unless an error occurs.
pub async fn run_fetcher(
    pool: Pool,
    connectors: ConnectorRegistry,
    federation_id: FederationId,
    config: ClientConfig,
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
    let next_session = query_value::<Option<i32>>(
        &pool.get().await?,
        "SELECT MAX(session_index) FROM sessions WHERE federation_id = $1",
        &[&federation_id.consensus_encode_to_vec()],
    )
    .await?
    .map(|max_session_index| max_session_index as u64 + 1)
    .unwrap_or(0);
    debug!("Next session {next_session}");

    let mut session_stream = futures::stream::iter(next_session..)
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
            &config,
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

    unreachable!("Session stream should never end")
}
