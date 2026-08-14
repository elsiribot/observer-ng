use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context};
use fedimint_api_client::api::{DynGlobalApi, FederationApiExt, StatusResponse};
use fedimint_core::config::{ClientConfig, FederationId};
use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::endpoint_constants::STATUS_ENDPOINT;
use fedimint_core::module::ApiRequestErased;
use fedimint_core::{NumPeers, PeerId};
use fmo_api_types::{
    FederationHealth, GuardianHealth, GuardianHealthLatest, GuardianLane, GuardianTimeline,
    TimeInterval,
};
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
            "WITH last2 AS (
                 SELECT guardian_id,
                        block_height,
                        (status -> 'federation' ->> 'session_count')::integer AS sc,
                        ROW_NUMBER() OVER (PARTITION BY guardian_id ORDER BY time DESC) AS rn
                 FROM guardian_health
                 WHERE federation_id = $1
             ),
             -- Debounce a single most-recent missed poll: report the newest
             -- non-null value among each guardian's two most recent samples.
             -- Both null => the guardian is (still) offline (session_count stays
             -- NULL). This keeps a lone transient timeout from flipping a
             -- guardian to offline in the panel / fleet health.
             latest AS (
                 SELECT guardian_id,
                        (array_remove(array_agg(block_height ORDER BY rn), NULL))[1] AS block_height,
                        (array_remove(array_agg(sc ORDER BY rn), NULL))[1] AS session_count
                 FROM last2
                 WHERE rn <= 2
                 GROUP BY guardian_id
             )
             SELECT
                latest.guardian_id,
                latest.block_height,
                latest.session_count,
                last30d.uptime,
                last30d.latency_ms
             FROM latest
             INNER JOIN (
                 SELECT
                     guardian_id,
                     (COUNT(status)::decimal / COUNT(*)::decimal * 100)::real as uptime,
                     AVG(latency_ms)::real as latency_ms
                 FROM guardian_health
                 WHERE federation_id = $1
                   AND time > NOW() - INTERVAL '30 days'
                 GROUP BY guardian_id
             ) last30d ON latest.guardian_id = last30d.guardian_id",
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
            // A guardian counts as online here only if it is *participating*:
            // its newest reported session_count (among its last 2 samples, to
            // debounce a single transient miss) is present AND caught up to the
            // federation tip (trails it by <=1). A deeply lagging guardian is
            // thus treated as degraded, matching the timeline's participating
            // rule, while a lone missed poll still doesn't flip it.
            "WITH last2 AS (
                 SELECT federation_id, guardian_id,
                        (status -> 'federation' ->> 'session_count')::bigint AS sc,
                        ROW_NUMBER() OVER (PARTITION BY federation_id, guardian_id ORDER BY time DESC) AS rn
                 FROM guardian_health
             ),
             latest AS (
                 SELECT federation_id, guardian_id,
                        (array_remove(array_agg(sc ORDER BY rn), NULL))[1] AS sc
                 FROM last2
                 WHERE rn <= 2
                 GROUP BY federation_id, guardian_id
             ),
             tip AS (
                 SELECT federation_id, MAX(sc) AS tip_sc FROM latest GROUP BY federation_id
             )
             SELECT l.federation_id,
                    COUNT(*)::int AS guardians,
                    COUNT(*) FILTER (
                        WHERE l.sc IS NOT NULL AND (t.tip_sc IS NULL OR t.tip_sc - l.sc <= 1)
                    )::int AS online_guardians
             FROM latest l
             JOIN tip t ON t.federation_id = l.federation_id
             GROUP BY l.federation_id",
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

    /// Builds the guardian outage timeline for a federation over the last
    /// `window`: one lane per guardian listing the maximal runs it was offline
    /// or lagging, plus the windows where the federation was inoperable
    /// (participating guardian count below the consensus threshold).
    ///
    /// # Lagging rule
    /// A guardian that responds but reports a consensus `session_count`
    /// trailing the highest among its peers (the tip) by more than one is
    /// *lagging*: online but stuck behind and not effectively participating
    /// in consensus. Lagging runs are reported separately from offline runs
    /// (they are disjoint — an offline guardian reports no session count)
    /// and, like offline time, count a guardian as non-participating for
    /// the inoperable threshold.
    ///
    /// # Offline rule
    /// The guardian poller writes one `guardian_health` row per guardian per
    /// ~60s poll; a NULL `status` means the guardian did not respond (offline),
    /// a non-NULL `status` means it responded (online). A guardian is
    /// considered offline from its first NULL sample until the next sample that
    /// shows it back online (non-NULL `status`), or until `window_end` if it is
    /// still offline at the end of the window (an ongoing outage). We carry the
    /// last observed state forward across sampling gaps: if the observer itself
    /// was down for a while, whatever state we last saw is assumed to hold
    /// until the next sample proves otherwise (so an outage is shown as
    /// continuous until an online sample proves recovery). Monitoring gaps are
    /// deliberately NOT independently inferred as offline — with no data we do
    /// not fabricate an outage, we only extend the last known state. A guardian
    /// with no samples at all in the window therefore has zero intervals.
    ///
    /// # Inoperable rule
    /// Each poll timestamp is shared by all guardians (the poller stamps one
    /// time per poll), so we count, per timestamp, how many guardians were
    /// *participating* (online AND caught up to the tip). The federation is
    /// inoperable across `[poll, next poll)` whenever that count is
    /// `< threshold`, coalesced into maximal runs, with an ongoing
    /// sub-threshold state extended to `window_end`.
    ///
    /// Both interval sets are computed with gap-and-islands SQL over
    /// `guardian_health` rather than shipping raw samples to Rust; guardians
    /// are almost always online so the resulting interval set is small.
    ///
    /// When `despike` is false, the single-poll false-positive filtering is
    /// disabled and every raw missed/lagging poll is shown as an interval — the
    /// opt-out for users who want to see unfiltered samples.
    pub async fn get_guardian_timeline(
        &self,
        federation_id: FederationId,
        window: chrono::Duration,
        despike: bool,
    ) -> anyhow::Result<GuardianTimeline> {
        let federation = self
            .get_federation(federation_id)
            .await
            .context("Unknown federation")?
            .context("Unknown federation")?;

        // Guardian display names + count come from the federation config, the
        // same source as the guardian panel (`api_endpoints[peer].name`).
        let names: BTreeMap<u16, String> = federation
            .config
            .global
            .api_endpoints
            .iter()
            .map(|(peer, peer_url)| (peer.to_usize() as u16, peer_url.name.clone()))
            .collect();
        let num_guardians = names.len();
        let threshold = NumPeers::from(num_guardians).threshold();

        let window_end = chrono::Utc::now().naive_utc();
        let window_start = window_end - window;
        let fed = federation_id.consensus_encode_to_vec();

        let conn = self.connection().await?;

        // --- per-guardian offline + lagging intervals (gap-and-islands) ---
        // One combined query classifies each poll into a per-guardian state
        // (offline / lagging / online) and emits maximal runs of the two
        // abnormal states, tagged by `kind`.
        #[derive(FromRow)]
        struct StateIntervalRow {
            guardian_id: i32,
            kind: String,
            start_time: chrono::NaiveDateTime,
            end_time: chrono::NaiveDateTime,
        }

        let state_rows = query::<StateIntervalRow>(
            &conn,
            // language=postgresql
            "WITH base AS (
                 SELECT guardian_id, time,
                        (status IS NOT NULL) AS raw_online,
                        (status -> 'federation' ->> 'session_count')::bigint AS sc
                 FROM guardian_health
                 WHERE federation_id = $1 AND time >= $2 AND time <= $3
             ),
             -- The highest session_count reported by any online guardian at a
             -- poll is the consensus tip the federation had reached then.
             tipped AS (
                 SELECT guardian_id, time, raw_online, sc,
                        MAX(sc) OVER (PARTITION BY time) AS tip_sc
                 FROM base
             ),
             -- Per-poll state: offline (no response), lagging (online but the
             -- reported session_count trails the tip by >1, i.e. stuck behind
             -- and not effectively participating), else online.
             stated AS (
                 SELECT guardian_id, time,
                        CASE
                            WHEN NOT raw_online THEN 'offline'
                            WHEN tip_sc IS NOT NULL AND tip_sc - sc > 1 THEN 'lagging'
                            ELSE 'online'
                        END AS raw_state
                 FROM tipped
             ),
             -- Despike single-poll false positives per channel (when $4): a lone
             -- poll in an abnormal state whose immediate neighbours were both
             -- NOT in that state is a transient blip, reclassified to normal.
             -- Runs of >=2 consecutive abnormal polls are untouched. With $4
             -- false the despike is disabled and raw states pass through.
             despiked AS (
                 SELECT guardian_id, time,
                        CASE WHEN $4 AND raw_state = 'offline'
                                  AND LAG(raw_state = 'offline') OVER w = false
                                  AND LEAD(raw_state = 'offline') OVER w = false
                             THEN false ELSE raw_state = 'offline' END AS off,
                        CASE WHEN $4 AND raw_state = 'lagging'
                                  AND LAG(raw_state = 'lagging') OVER w = false
                                  AND LEAD(raw_state = 'lagging') OVER w = false
                             THEN false ELSE raw_state = 'lagging' END AS lag
                 FROM stated
                 WINDOW w AS (PARTITION BY guardian_id ORDER BY time)
             ),
             -- Unpivot the two despiked channels so one gap-and-islands pass,
             -- partitioned by (guardian_id, kind), covers both.
             long AS (
                 SELECT guardian_id, time, 'offline' AS kind, off AS flag FROM despiked
                 UNION ALL
                 SELECT guardian_id, time, 'lagging' AS kind, lag AS flag FROM despiked
             ),
             flagged AS (
                 SELECT guardian_id, kind, time, flag,
                        LAG(flag) OVER w AS prev_flag,
                        LAG(time) OVER w AS prev_time
                 FROM long
                 WINDOW w AS (PARTITION BY guardian_id, kind ORDER BY time)
             ),
             -- A segment spans [prev_time, time) whenever the previous sample
             -- was in the abnormal state; a trailing segment extends a still-
             -- abnormal last sample to window_end.
             segments AS (
                 SELECT guardian_id, kind, prev_time AS seg_start, time AS seg_end
                 FROM flagged
                 WHERE prev_time IS NOT NULL AND prev_flag = true
                 UNION ALL
                 SELECT guardian_id, kind, time AS seg_start, $3::timestamp AS seg_end
                 FROM (
                     SELECT DISTINCT ON (guardian_id, kind) guardian_id, kind, time, flag
                     FROM long
                     ORDER BY guardian_id, kind, time DESC
                 ) last_sample
                 WHERE flag = true
             ),
             grouped AS (
                 SELECT guardian_id, kind, seg_start, seg_end,
                        SUM(new_grp) OVER (PARTITION BY guardian_id, kind ORDER BY seg_start) AS grp
                 FROM (
                     SELECT guardian_id, kind, seg_start, seg_end,
                            CASE WHEN LAG(seg_end) OVER (PARTITION BY guardian_id, kind ORDER BY seg_start)
                                      >= seg_start
                                 THEN 0 ELSE 1 END AS new_grp
                     FROM segments
                 ) s
             )
             SELECT guardian_id, kind,
                    MIN(seg_start) AS start_time,
                    MAX(seg_end) AS end_time
             FROM grouped
             GROUP BY guardian_id, kind, grp
             ORDER BY guardian_id, kind, start_time",
            &[&fed, &window_start, &window_end, &despike],
        )
        .await?;

        // Split intervals by guardian id and kind.
        let mut offline_by_guardian: BTreeMap<u16, Vec<TimeInterval>> = BTreeMap::new();
        let mut lagging_by_guardian: BTreeMap<u16, Vec<TimeInterval>> = BTreeMap::new();
        for row in state_rows {
            let interval = TimeInterval {
                start: row.start_time.and_utc().timestamp(),
                end: row.end_time.and_utc().timestamp(),
            };
            let bucket = if row.kind == "lagging" {
                &mut lagging_by_guardian
            } else {
                &mut offline_by_guardian
            };
            bucket
                .entry(row.guardian_id as u16)
                .or_default()
                .push(interval);
        }

        // One lane per configured guardian, in peer-id order, even if it has no
        // samples/outages (so the timeline always shows every guardian).
        let guardians = names
            .into_iter()
            .map(|(guardian_id, name)| GuardianLane {
                guardian_id,
                name,
                offline_intervals: offline_by_guardian.remove(&guardian_id).unwrap_or_default(),
                lagging_intervals: lagging_by_guardian.remove(&guardian_id).unwrap_or_default(),
            })
            .collect();

        // --- federation-wide inoperable intervals (gap-and-islands) ---
        #[derive(FromRow)]
        struct InoperableIntervalRow {
            start_time: chrono::NaiveDateTime,
            end_time: chrono::NaiveDateTime,
        }

        let inoperable_rows = query::<InoperableIntervalRow>(
            &conn,
            // language=postgresql
            // Count *participating* guardians per poll — online AND caught up to
            // the consensus tip — so a deeply lagging guardian counts against
            // the threshold just like an offline one. Despike per guardian first
            // (same rule as the state query), so an observer-side blip dropping
            // several guardians for a single poll doesn't fabricate an
            // inoperable (sub-threshold) window.
            "WITH base AS (
                 SELECT guardian_id, time,
                        (status IS NOT NULL) AS raw_online,
                        (status -> 'federation' ->> 'session_count')::bigint AS sc
                 FROM guardian_health
                 WHERE federation_id = $1 AND time >= $2 AND time <= $3
             ),
             tipped AS (
                 SELECT guardian_id, time, raw_online, sc,
                        MAX(sc) OVER (PARTITION BY time) AS tip_sc
                 FROM base
             ),
             stated AS (
                 SELECT guardian_id, time,
                        (raw_online AND (tip_sc IS NULL OR tip_sc - sc <= 1)) AS participating
                 FROM tipped
             ),
             despiked AS (
                 SELECT time,
                        CASE
                            WHEN $5
                                 AND NOT participating
                                 AND LAG(participating) OVER w = true
                                 AND LEAD(participating) OVER w = true
                            THEN true
                            ELSE participating
                        END AS online
                 FROM stated
                 WINDOW w AS (PARTITION BY guardian_id ORDER BY time)
             ),
             poll_counts AS (
                 SELECT time,
                        COUNT(*) FILTER (WHERE online) AS online_count
                 FROM despiked
                 GROUP BY time
             ),
             flagged AS (
                 SELECT time,
                        LAG(online_count < $4) OVER (ORDER BY time) AS prev_inoperable,
                        LAG(time) OVER (ORDER BY time) AS prev_time
                 FROM poll_counts
             ),
             segments AS (
                 SELECT prev_time AS seg_start, time AS seg_end
                 FROM flagged
                 WHERE prev_time IS NOT NULL AND prev_inoperable = true
                 UNION ALL
                 SELECT time AS seg_start, $3::timestamp AS seg_end
                 FROM (
                     SELECT time, online_count FROM poll_counts ORDER BY time DESC LIMIT 1
                 ) last_poll
                 WHERE online_count < $4
             ),
             grouped AS (
                 SELECT seg_start, seg_end,
                        SUM(new_grp) OVER (ORDER BY seg_start) AS grp
                 FROM (
                     SELECT seg_start, seg_end,
                            CASE WHEN LAG(seg_end) OVER (ORDER BY seg_start) >= seg_start
                                 THEN 0 ELSE 1 END AS new_grp
                     FROM segments
                 ) s
             )
             SELECT MIN(seg_start) AS start_time, MAX(seg_end) AS end_time
             FROM grouped
             GROUP BY grp
             ORDER BY start_time",
            &[
                &fed,
                &window_start,
                &window_end,
                &(threshold as i64),
                &despike,
            ],
        )
        .await?;

        let inoperable_intervals = inoperable_rows
            .into_iter()
            .map(|row| TimeInterval {
                start: row.start_time.and_utc().timestamp(),
                end: row.end_time.and_utc().timestamp(),
            })
            .collect();

        Ok(GuardianTimeline {
            window_start: window_start.and_utc().timestamp(),
            window_end: window_end.and_utc().timestamp(),
            num_guardians,
            threshold,
            guardians,
            inoperable_intervals,
        })
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
