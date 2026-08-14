use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context};
use fedimint_api_client::api::{DynGlobalApi, FederationApiExt, StatusResponse};
use fedimint_core::config::{ClientConfig, FederationId};
use fedimint_core::encoding::{Decodable, Encodable};
use fedimint_core::endpoint_constants::STATUS_ENDPOINT;
use fedimint_core::module::ApiRequestErased;
use fedimint_core::net::api_announcement::SignedApiAnnouncement;
use fedimint_core::secp256k1::PublicKey;
use fedimint_core::util::SafeUrl;
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
        // API-URL announcements change rarely, so refresh them only every ~10
        // polls (~10 min) rather than on the 60s health cadence.
        const ANNOUNCEMENT_REFRESH_POLLS: u64 = 10;

        let mut interval = tokio::time::interval(REQUEST_INTERVAL);

        // Base (consensus config) API URLs, plus the guardian identity keys that
        // sign API-URL announcements. Without the keys (very old 0.3.x configs)
        // we can't verify announcements, so override tracking is skipped and we
        // poll the config URLs directly.
        let cfg_urls: BTreeMap<PeerId, SafeUrl> = config
            .global
            .api_endpoints
            .iter()
            .map(|(&peer_id, peer_url)| (peer_id, peer_url.url.clone()))
            .collect();
        let broadcast_pub_keys = config.global.broadcast_public_keys.clone();

        // Effective URLs = config URLs overridden by any tracked announcements.
        // Rebuilt below whenever a guardian rotates its endpoint.
        let mut effective_urls = self.effective_api_urls(federation_id, &cfg_urls).await?;
        let mut api = DynGlobalApi::new(self.connectors().clone(), effective_urls.clone(), None)?;
        let mut poll_count: u64 = 0;

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

            // Periodically refresh signed API-URL announcements and, if a
            // guardian rotated its endpoint, rebuild `api` so we poll the
            // current URL instead of a stale (dead) config URL. Best-effort:
            // any failure just leaves the previous effective URLs in place.
            if poll_count.is_multiple_of(ANNOUNCEMENT_REFRESH_POLLS) {
                if let Some(pub_keys) = &broadcast_pub_keys {
                    if let Err(e) = self
                        .refresh_api_announcements(federation_id, &api, &cfg_urls, pub_keys)
                        .await
                    {
                        tracing::debug!(
                            "API announcement refresh failed for {federation_id}: {e:?}"
                        );
                    }
                    match self.effective_api_urls(federation_id, &cfg_urls).await {
                        Ok(new_urls) if new_urls != effective_urls => {
                            tracing::info!(
                                "guardian API URLs changed for {federation_id}, rebuilding api client"
                            );
                            effective_urls = new_urls;
                            api = DynGlobalApi::new(
                                self.connectors().clone(),
                                effective_urls.clone(),
                                None,
                            )?;
                        }
                        Ok(_) => {}
                        Err(e) => {
                            tracing::debug!(
                                "reading tracked API URLs failed for {federation_id}: {e:?}"
                            )
                        }
                    }
                }
            }
            poll_count = poll_count.wrapping_add(1);

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

    /// The effective API URL per guardian: the config URL, overridden by the
    /// highest-nonce tracked announcement (`guardian_api_announcements`) where
    /// one exists. Public for tests; the health monitor is the real caller.
    pub async fn effective_api_urls(
        &self,
        federation_id: FederationId,
        cfg_urls: &BTreeMap<PeerId, SafeUrl>,
    ) -> anyhow::Result<BTreeMap<PeerId, SafeUrl>> {
        #[derive(FromRow)]
        struct AnnouncementRow {
            guardian_id: i32,
            api_url: String,
        }

        let rows = query::<AnnouncementRow>(
            &self.connection().await?,
            "SELECT guardian_id, api_url FROM guardian_api_announcements WHERE federation_id = $1",
            &[&federation_id.consensus_encode_to_vec()],
        )
        .await?;

        let mut urls = cfg_urls.clone();
        for row in rows {
            match SafeUrl::parse(&row.api_url) {
                // Only override peers that exist in the config; ignore a stored
                // URL that no longer parses.
                Ok(url) => {
                    let peer = PeerId::new(row.guardian_id as u16);
                    if urls.contains_key(&peer) {
                        urls.insert(peer, url);
                    }
                }
                Err(e) => tracing::debug!("stored API URL '{}' is invalid: {e:?}", row.api_url),
            }
        }
        Ok(urls)
    }

    /// Fetches signed API-URL announcements from any reachable guardian (the
    /// returned map covers all peers, so one responsive guardian suffices to
    /// learn another's rotated URL), verifies each against the announcing
    /// guardian's identity key, and records the highest-nonce URL per guardian
    /// in `guardian_api_announcements`.
    async fn refresh_api_announcements(
        &self,
        federation_id: FederationId,
        api: &DynGlobalApi,
        cfg_urls: &BTreeMap<PeerId, SafeUrl>,
        broadcast_pub_keys: &BTreeMap<PeerId, PublicKey>,
    ) -> anyhow::Result<()> {
        // Try guardians in turn until one returns the announcement map.
        let mut announcements: Option<BTreeMap<PeerId, SignedApiAnnouncement>> = None;
        for &peer_id in cfg_urls.keys() {
            match api.api_announcements(peer_id).await {
                Ok(map) => {
                    announcements = Some(map);
                    break;
                }
                Err(e) => {
                    tracing::debug!("api_announcements from peer {peer_id} failed: {e:?}")
                }
            }
        }
        let announcements = announcements.context("no guardian returned API announcements")?;

        let secp = fedimint_core::secp256k1::Secp256k1::verification_only();
        let conn = self.connection().await?;
        let now = chrono::Utc::now().naive_utc();
        let fed = federation_id.consensus_encode_to_vec();

        for (peer_id, signed) in announcements {
            // Reject announcements not signed by the announcing guardian's own
            // identity key — a peer must not be able to redirect another's URL.
            let Some(pub_key) = broadcast_pub_keys.get(&peer_id) else {
                continue;
            };
            if !signed.verify(&secp, pub_key) {
                tracing::warn!(
                    "invalid API announcement for peer {peer_id} of {federation_id}, ignoring"
                );
                continue;
            }

            let url = signed.api_announcement.api_url.to_string();
            let nonce = signed.api_announcement.nonce as i64;
            // Upsert only when this announcement is newer (higher nonce) than
            // what we have tracked, so a replayed older announcement can't
            // downgrade the URL.
            conn.execute(
                "INSERT INTO guardian_api_announcements
                     (federation_id, guardian_id, api_url, nonce, updated_at)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT (federation_id, guardian_id) DO UPDATE SET
                     api_url = EXCLUDED.api_url,
                     nonce = EXCLUDED.nonce,
                     updated_at = EXCLUDED.updated_at
                 WHERE EXCLUDED.nonce > guardian_api_announcements.nonce",
                &[&fed, &(peer_id.to_usize() as i32), &url, &nonce, &now],
            )
            .await?;
        }
        Ok(())
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

    /// Threshold-aware federation uptime over the last `window`: the fraction
    /// of health polls at which at least `threshold` guardians were
    /// participating (online AND caught up to the consensus tip). Uses the
    /// same per-poll participating rule and single-poll despiking as the
    /// timeline's inoperable bands, so the two agree.
    pub async fn federation_uptime(
        &self,
        federation_id: FederationId,
        window: chrono::Duration,
    ) -> anyhow::Result<fmo_api_types::FederationUptime> {
        let federation = self
            .get_federation(federation_id)
            .await
            .context("Unknown federation")?
            .context("Unknown federation")?;

        let num_guardians = federation.config.global.api_endpoints.len();
        let threshold = NumPeers::from(num_guardians).threshold();

        let window_end = chrono::Utc::now().naive_utc();
        let window_start = window_end - window;
        let fed = federation_id.consensus_encode_to_vec();

        #[derive(FromRow)]
        struct UptimeRow {
            total_polls: i64,
            operable_polls: i64,
        }

        let row = query::<UptimeRow>(
            &self.connection().await?,
            // language=postgresql
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
                            WHEN NOT participating
                                 AND LAG(participating) OVER w = true
                                 AND LEAD(participating) OVER w = true
                            THEN true
                            ELSE participating
                        END AS participating
                 FROM stated
                 WINDOW w AS (PARTITION BY guardian_id ORDER BY time)
             ),
             poll_counts AS (
                 SELECT time, COUNT(*) FILTER (WHERE participating) AS online_count
                 FROM despiked
                 GROUP BY time
             )
             SELECT COUNT(*)::bigint AS total_polls,
                    COUNT(*) FILTER (WHERE online_count >= $4)::bigint AS operable_polls
             FROM poll_counts",
            &[&fed, &window_start, &window_end, &(threshold as i64)],
        )
        .await?
        .into_iter()
        .next()
        .unwrap_or(UptimeRow {
            total_polls: 0,
            operable_polls: 0,
        });

        let uptime_pct = (row.total_polls > 0)
            .then(|| row.operable_polls as f64 / row.total_polls as f64 * 100.0);

        Ok(fmo_api_types::FederationUptime {
            window_start: window_start.and_utc().timestamp(),
            window_end: window_end.and_utc().timestamp(),
            num_guardians,
            threshold,
            total_polls: row.total_polls,
            operable_polls: row.operable_polls,
            uptime_pct,
        })
    }

    /// Builds the guardian API-latency time series for a federation over the
    /// last `window`: one bucketed line per guardian plus a derived
    /// quorum-latency line.
    ///
    /// # Quorum latency
    /// At each ~60s poll we rank the *responding* guardians by latency and take
    /// the `threshold`-th fastest — the slowest latency among the fastest
    /// quorum (e.g. the 5th-fastest of 7 in a 5/7). That is the latency at
    /// which the federation could actually reach consensus at that instant.
    /// Polls with fewer than `threshold` responders contribute no quorum
    /// sample. The per-poll quorum latencies are then averaged per bucket.
    ///
    /// Raw samples (60s × window × guardians) are far too many to plot, so both
    /// the per-guardian lines and the quorum line are averaged into ~a few
    /// hundred time buckets (`bucket_seconds`, derived from the window) in SQL.
    pub async fn get_guardian_latency(
        &self,
        federation_id: FederationId,
        window: chrono::Duration,
    ) -> anyhow::Result<fmo_api_types::GuardianLatencySeries> {
        use fmo_api_types::{GuardianLatencySeries, GuardianRef, LatencyBucket};

        let federation = self
            .get_federation(federation_id)
            .await
            .context("Unknown federation")?
            .context("Unknown federation")?;

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

        // Aim for ~240 buckets across the window, floored at one minute (the
        // poll interval) so buckets never subdivide a single poll.
        let bucket_seconds = (window.num_seconds() / 240).max(60);

        // Guardian peer-id order, and a lookup from peer id to series index.
        let guardian_ids: Vec<u16> = names.keys().copied().collect();
        let index_of: BTreeMap<u16, usize> = guardian_ids
            .iter()
            .enumerate()
            .map(|(i, &id)| (id, i))
            .collect();

        #[derive(FromRow)]
        struct LatencyRow {
            bucket: chrono::NaiveDateTime,
            // -1 marks the derived quorum line; otherwise a guardian peer id.
            guardian_id: i32,
            avg_ms: f64,
        }

        let rows = query::<LatencyRow>(
            &self.connection().await?,
            // language=postgresql
            "WITH resp AS (
                 SELECT guardian_id, time, latency_ms
                 FROM guardian_health
                 WHERE federation_id = $1 AND time >= $2 AND time <= $3
                   AND status IS NOT NULL
             ),
             -- Rank responders by latency within each poll; the threshold-th
             -- fastest is the quorum latency for that poll.
             ranked AS (
                 SELECT time, latency_ms,
                        ROW_NUMBER() OVER (PARTITION BY time ORDER BY latency_ms) AS rnk,
                        COUNT(*) OVER (PARTITION BY time) AS n_resp
                 FROM resp
             ),
             quorum_per_poll AS (
                 SELECT time, latency_ms AS q
                 FROM ranked
                 WHERE rnk = $5 AND n_resp >= $5
             )
             SELECT date_bin(make_interval(secs => $4), time, $2) AS bucket,
                    guardian_id,
                    AVG(latency_ms)::float8 AS avg_ms
             FROM resp
             GROUP BY 1, 2
             UNION ALL
             SELECT date_bin(make_interval(secs => $4), time, $2) AS bucket,
                    -1 AS guardian_id,
                    AVG(q)::float8 AS avg_ms
             FROM quorum_per_poll
             GROUP BY 1
             ORDER BY bucket, guardian_id",
            &[
                &fed,
                &window_start,
                &window_end,
                &(bucket_seconds as f64),
                &(threshold as i64),
            ],
        )
        .await?;

        // Assemble long-form rows into per-bucket records. Rows are ordered by
        // bucket, so we accumulate into an ordered map keyed by bucket time.
        let mut buckets: BTreeMap<i64, LatencyBucket> = BTreeMap::new();
        for row in rows {
            let time = row.bucket.and_utc().timestamp();
            let entry = buckets.entry(time).or_insert_with(|| LatencyBucket {
                time,
                latencies: vec![None; num_guardians],
                quorum_ms: None,
            });
            if row.guardian_id == -1 {
                entry.quorum_ms = Some(row.avg_ms);
            } else if let Some(&idx) = index_of.get(&(row.guardian_id as u16)) {
                entry.latencies[idx] = Some(row.avg_ms);
            }
        }

        let guardians = guardian_ids
            .into_iter()
            .map(|guardian_id| GuardianRef {
                guardian_id,
                name: names.get(&guardian_id).cloned().unwrap_or_default(),
            })
            .collect();

        Ok(GuardianLatencySeries {
            window_start: window_start.and_utc().timestamp(),
            window_end: window_end.and_utc().timestamp(),
            num_guardians,
            threshold,
            bucket_seconds,
            guardians,
            buckets: buckets.into_values().collect(),
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
