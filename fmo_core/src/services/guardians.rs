use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context};
use fedimint_api_client::api::{DynGlobalApi, FederationApiExt, StatusResponse};
use fedimint_core::config::{ClientConfig, FederationId};
use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::endpoint_constants::STATUS_ENDPOINT;
use fedimint_core::module::ApiRequestErased;
use fedimint_core::{NumPeers, PeerId};
use fmo_api_types::{FederationHealth, GuardianHealth, GuardianHealthLatest};
use futures::future::join_all;
use postgres_from_row::FromRow;

use crate::observer::FederationObserver;
use crate::query::query;

/// Wallet module endpoint used to measure guardian API latency and block
/// height. Inlined from `fedimint-wallet-common` so core doesn't depend on a
/// module crate; the endpoint name is part of the wallet module's stable API.
const BLOCK_COUNT_LOCAL_ENDPOINT: &str = "block_count_local";

impl FederationObserver {
    pub async fn monitor_health(
        &self,
        federation_id: FederationId,
        config: ClientConfig,
    ) -> anyhow::Result<()> {
        const REQUEST_INTERVAL: Duration = Duration::from_secs(60);

        let mut interval = tokio::time::interval(REQUEST_INTERVAL);
        let peers = config
            .global
            .api_endpoints
            .iter()
            .map(|(&peer_id, peer_url)| (peer_id, peer_url.url.clone()))
            .collect();
        let api = DynGlobalApi::new(self.connectors().clone(), peers, None)?;

        // The v1 wallet module's `block_count_local` endpoint doubles as a warm
        // per-peer latency probe and a bitcoin block-height source. Federations
        // without a v1 wallet module (e.g. walletv2-only ones like pure-v2
        // federations) simply don't report a block height — we still measure
        // latency via a second status request rather than failing the whole
        // monitor, which would leave those federations with no health data at all.
        let wallet_module = config
            .modules
            .iter()
            .find_map(|(&module_instance_id, module)| {
                (module.kind.as_str() == "wallet").then_some(module_instance_id)
            });

        loop {
            interval.tick().await;

            let peer_status_responses =
                join_all(config.global.api_endpoints.keys().map(|&peer_id| {
                    let api = api.clone();
                    async move {
                        // We don't time the first request, there might be a reconnect happening in
                        // the background
                        let status = api
                            .request_single_peer(
                                STATUS_ENDPOINT.to_owned(),
                                ApiRequestErased::default(),
                                peer_id,
                            )
                            .await
                            .ok()
                            .and_then(|json| serde_json::from_value::<StatusResponse>(json).ok());

                        // Second request is used to determine ping (warm).
                        // TODO: how much time does bitcoind take to answer if at all (caching?)?
                        let start_time = Instant::now();
                        let block_height = if let Some(wallet_module) = wallet_module {
                            api.with_module(wallet_module)
                                .request_single_peer(
                                    BLOCK_COUNT_LOCAL_ENDPOINT.to_owned(),
                                    ApiRequestErased::default(),
                                    peer_id,
                                )
                                .await
                                .ok()
                                .and_then(|json| {
                                    serde_json::from_value::<Option<u32>>(json).ok().flatten()
                                })
                                .map(|block_count| {
                                    // Fedimint uses 1-based block heights, while bitcoind uses
                                    // 0-based heights. saturating_sub guards the (never-observed
                                    // but possible) block_count == 0 case against u32 underflow.
                                    block_count.saturating_sub(1)
                                })
                        } else {
                            // No v1 wallet module (e.g. walletv2-only federation): time a second
                            // status request for latency; block height is unavailable here.
                            let _ = api
                                .request_single_peer::<serde_json::Value>(
                                    STATUS_ENDPOINT.to_owned(),
                                    ApiRequestErased::default(),
                                    peer_id,
                                )
                                .await;
                            None
                        };
                        let api_latency = start_time.elapsed();

                        (peer_id, status, block_height, api_latency)
                    }
                }))
                .await;

            let mut conn = self.connection().await?;
            let dbtx = conn.transaction().await?;
            let timestamp = chrono::Utc::now().naive_utc();
            for (peer_id, status, block_height, api_latency) in peer_status_responses {
                dbtx.execute(
                    "INSERT INTO guardian_health VALUES ($1, $2, $3, $4, $5, $6)",
                    &[
                        &federation_id.consensus_encode_to_vec(),
                        &timestamp,
                        &(peer_id.to_usize() as i32),
                        &status.map(|s| serde_json::to_value(s).expect("Can be serialized")),
                        &block_height.map(|bh| bh as i32),
                        &(api_latency.as_millis() as i32),
                    ],
                )
                .await?;
            }
            dbtx.commit().await?;
        }
    }

    pub async fn get_guardian_health(
        &self,
        federation_id: FederationId,
    ) -> anyhow::Result<BTreeMap<PeerId, GuardianHealth>> {
        let _federation = self
            .get_federation(federation_id)
            .await
            .context("Unknown federation")?;

        let health_rows = query::<GuardianHealthRow>(
            &self.connection().await?,
            // language=postgresql
            "SELECT
                latest.guardian_id,
                latest.block_height,
                (latest.status -> 'federation' ->> 'session_count')::integer AS session_count,
                last30d.uptime,
                last30d.latency_ms
             FROM guardian_health latest
             INNER JOIN (
                 SELECT guardian_id, MAX(time) as latest_time
                 FROM guardian_health
                 WHERE federation_id = $1
                 GROUP BY guardian_id
             ) max_times ON latest.guardian_id = max_times.guardian_id
                           AND latest.time = max_times.latest_time
             INNER JOIN (
                 SELECT
                     guardian_id,
                     (COUNT(status)::decimal / COUNT(*)::decimal * 100)::real as uptime,
                     AVG(latency_ms)::real as latency_ms
                 FROM guardian_health
                 WHERE federation_id = $1
                   AND time > NOW() - INTERVAL '30 days'
                 GROUP BY guardian_id
             ) last30d ON latest.guardian_id = last30d.guardian_id
             WHERE latest.federation_id = $1",
            &[&federation_id.consensus_encode_to_vec()],
        )
        .await?;

        let our_block_height = self.get_block_height().await?;
        let max_session = health_rows
            .iter()
            .filter_map(|row| row.session_count)
            .max()
            .unwrap_or_default() as u32;

        Ok(health_rows
            .into_iter()
            .map(|row| {
                // A guardian is "reporting" as soon as it returns a session
                // count; the bitcoin block height is a separate, optional
                // signal that walletv2-only federations never provide. Gating
                // `latest` on `session_count` alone (not also on block height)
                // is what lets those federations show their guardians online.
                let latest = row.session_count.map(|session_count| {
                    let session_count = session_count as u32;
                    let block_height = row.block_height.map(|bh| bh as u32);
                    GuardianHealthLatest {
                        block_height,
                        block_outdated: block_height
                            .map(|bh| our_block_height.saturating_sub(bh) > 6),
                        session_count,
                        session_outdated: max_session.saturating_sub(session_count) > 1,
                    }
                });

                let health = GuardianHealth {
                    avg_uptime: row.uptime,
                    avg_latency: row.latency_ms,
                    latest,
                };

                (PeerId::new(row.guardian_id as u16), health)
            })
            .collect())
    }

    /// Returns the cached fleet-wide guardian health, refreshed on the matview
    /// refresh cycle (see `refresh_views_inner`). Before the first refresh
    /// cycle completes, computes and caches it on demand so a freshly started
    /// process still serves health immediately. This is read on every home-page
    /// and `/summary` load, so it must not hit the (full-scan) query each time.
    pub async fn get_guardian_health_summary(
        &self,
    ) -> anyhow::Result<BTreeMap<FederationId, FederationHealth>> {
        if let Some(health) = self.cached_health_summary().read().await.clone() {
            return Ok(health);
        }

        let health = self.compute_guardian_health_summary().await?;
        *self.cached_health_summary().write().await = Some(health.clone());
        Ok(health)
    }

    /// Computes the fleet-wide guardian health from scratch. Expensive: scans
    /// the append-only `guardian_health` table in full to find each guardian's
    /// latest sample. Called on the matview refresh cycle
    /// (`refresh_views_inner`) and, as a fallback, by
    /// [`Self::get_guardian_health_summary`] before the cache is warm.
    pub async fn compute_guardian_health_summary(
        &self,
    ) -> anyhow::Result<BTreeMap<FederationId, FederationHealth>> {
        #[derive(FromRow)]
        struct FederationHealthRow {
            federation_id: Vec<u8>,
            guardians: i32,
            online_guardians: i32,
        }

        let federations = query::<FederationHealthRow>(
            &self.connection().await?,
            // language=postgresql
            "SELECT
                gh.federation_id,
                COUNT(DISTINCT gh.guardian_id)::int as guardians,
                COUNT(DISTINCT CASE WHEN gh.status -> 'federation' ->> 'session_count' IS NOT NULL
                                   THEN gh.guardian_id END)::int as online_guardians
             FROM guardian_health gh
             INNER JOIN (
                 SELECT federation_id, guardian_id, MAX(time) as latest_time
                 FROM guardian_health
                 GROUP BY federation_id, guardian_id
             ) latest ON gh.federation_id = latest.federation_id
                        AND gh.guardian_id = latest.guardian_id
                        AND gh.time = latest.latest_time
             GROUP BY gh.federation_id",
            &[],
        )
        .await?;

        federations
            .into_iter()
            .map(|federation| {
                let federation_id = FederationId::consensus_decode_whole(
                    &federation.federation_id,
                    &Default::default(),
                )
                .map_err(|_| anyhow!("Invalid federation id in DB"))?;

                // Special case single guardian federations to not show them as degraded
                if federation.guardians == 1 {
                    return Ok((federation_id, FederationHealth::Online));
                }

                let threshold = NumPeers::from(federation.guardians as usize).threshold();
                let online = federation.online_guardians as usize;

                #[allow(clippy::comparison_chain)]
                if online > threshold {
                    Ok((federation_id, FederationHealth::Online))
                } else if online == threshold {
                    Ok((federation_id, FederationHealth::Degraded))
                } else {
                    Ok((federation_id, FederationHealth::Offline))
                }
            })
            .collect()
    }
}

#[derive(FromRow)]
struct GuardianHealthRow {
    guardian_id: i32,
    block_height: Option<i32>,
    session_count: Option<i32>,
    uptime: f32,
    latency_ms: f32,
}
