//! Generic gateway-polling harness shared across LN observer modules
//! (LNv1, LNv2, ...). A `GatewaySource` implementation knows how to fetch a
//! module's gateway registry and upsert it into that module's own `gateways`
//! table; this harness owns the poll loop, the reachability ping, the
//! `gateway_poll_snapshots` bookkeeping and pruning, generalized over the
//! module's schema name.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use fedimint_api_client::api::DynGlobalApi;
use fedimint_core::encoding::Encodable;
use futures::future::join_all;
use tokio_postgres::Transaction;
use tracing::{info, warn};

use crate::module::ModuleTaskCtx;

/// Default poll cadence (minutes); overridable via `FO_GATEWAY_POLL_SECS`.
/// Exported so modules computing uptime-display math from
/// `gateway_poll_snapshots` sample counts can share this instead of keeping
/// their own copy.
pub const POLL_INTERVAL_MINUTES: u64 = 5;
const SNAPSHOT_RETENTION_DAYS: i64 = 90;
const PRUNE_INTERVAL_HOURS: i64 = 6;
const PING_TIMEOUT: Duration = Duration::from_secs(5);

/// A gateway as returned by a `GatewaySource` after it fetches the module's
/// registry: just enough to snapshot uptime and ping reachability.
pub struct PolledGateway {
    pub gateway_id: String,
    pub api_endpoint: Option<String>,
}

/// Implemented once per module kind that has a gateway registry (LNv1, LNv2,
/// ...). Owns the module-specific registry fetch + `gateways` table upsert;
/// the harness owns everything else (loop cadence, snapshotting, pinging,
/// pruning). Split into a network-only `fetch` and a DB-only `upsert` so the
/// harness can do all network I/O (registry fetch + pings) *before* taking a
/// pooled DB connection, rather than holding one for the whole cycle.
#[async_trait::async_trait]
pub trait GatewaySource: Send + Sync {
    /// Module-specific parsed registry data (carries whatever `upsert`
    /// needs).
    type Fetched: Send + Sync;

    /// The module's Postgres schema, e.g. `"fmo_ln"`. Used to qualify the
    /// shared `gateway_poll_snapshots` and `gateways` tables.
    fn schema(&self) -> &'static str;

    /// Network only — query the federation's gateway registry (from
    /// guardians and/or peers, module-specific). MUST NOT touch the DB.
    async fn fetch(&self, ctx: &ModuleTaskCtx, api: &DynGlobalApi)
        -> anyhow::Result<Self::Fetched>;

    /// The currently-registered gateways to snapshot + ping (id + api
    /// endpoint), derived from the fetched data.
    fn polled_gateways(&self, fetched: &Self::Fetched) -> Vec<PolledGateway>;

    /// DB only — upsert the module's `gateways` table from the fetched data.
    async fn upsert(
        &self,
        dbtx: &Transaction<'_>,
        ctx: &ModuleTaskCtx,
        now: DateTime<Utc>,
        fetched: &Self::Fetched,
    ) -> anyhow::Result<()>;
}

/// GET the gateway's API root with a bounded timeout using a shared client.
/// `reachable` = any HTTP response received in time; `latency_ms` =
/// round-trip. Never errors — a dead gateway just reports `(false, None)` and
/// cannot stall the caller.
async fn ping_with(client: &reqwest::Client, api_endpoint: &str) -> (bool, Option<i32>) {
    let start = Instant::now();
    match client.get(api_endpoint).send().await {
        Ok(_resp) => (
            true,
            Some(start.elapsed().as_millis().min(i32::MAX as u128) as i32),
        ),
        Err(_) => (false, None),
    }
}

/// GET the gateway's API root with a bounded timeout. Thin one-off-client
/// wrapper around [`ping_with`], kept for callers (and tests) that just want
/// to ping a single endpoint without building a client themselves.
pub async fn ping_gateway(api_endpoint: &str, timeout: Duration) -> (bool, Option<i32>) {
    let client = match reqwest::Client::builder().timeout(timeout).build() {
        Ok(c) => c,
        Err(_) => return (false, None),
    };
    ping_with(&client, api_endpoint).await
}

/// Ping every polled gateway in parallel with a single reused client, bounded
/// by `PING_TIMEOUT` in aggregate (not `N * PING_TIMEOUT`).
async fn ping_all(polled: &[PolledGateway]) -> HashMap<String, (bool, Option<i32>)> {
    let client = reqwest::Client::builder()
        .timeout(PING_TIMEOUT)
        .build()
        .ok();
    let futs = polled.iter().filter_map(|g| {
        let id = g.gateway_id.clone();
        let ep = g.api_endpoint.clone()?;
        let client = client.clone();
        Some(async move {
            let res = match &client {
                Some(c) => ping_with(c, &ep).await,
                None => (false, None),
            };
            (id, res)
        })
    });
    join_all(futs).await.into_iter().collect()
}

/// The shared gateway-polling background task: periodically fetches the
/// registry via `source`, pings each currently-registered gateway and
/// persists an uptime/reachability snapshot, pruning old snapshots on a
/// coarse schedule.
pub async fn run_gateway_poller(
    ctx: ModuleTaskCtx,
    source: impl GatewaySource + 'static,
) -> anyhow::Result<()> {
    let poll_secs = std::env::var("FO_GATEWAY_POLL_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(POLL_INTERVAL_MINUTES * 60);
    let peers = ctx
        .config
        .global
        .api_endpoints
        .iter()
        .map(|(&id, url)| (id, url.url.clone()))
        .collect();
    let api = DynGlobalApi::new(ctx.connectors.clone(), peers, None)?;
    let mut interval = tokio::time::interval(Duration::from_secs(poll_secs));
    loop {
        interval.tick().await;
        if let Err(e) = poll_once(&ctx, &api, &source).await {
            warn!("gateway poll for {} failed: {e:?}", ctx.federation_id);
        }
    }
}

async fn poll_once(
    ctx: &ModuleTaskCtx,
    api: &DynGlobalApi,
    source: &impl GatewaySource,
) -> anyhow::Result<()> {
    let now = Utc::now();
    let fed = ctx.federation_id.consensus_encode_to_vec();

    // ---- network phase: no pool connection held ----
    let fetched = source.fetch(ctx, api).await?;
    let polled = source.polled_gateways(&fetched);
    let ping = ping_all(&polled).await;

    // ---- DB phase: short-lived connection + transaction ----
    let mut conn = ctx.pool.get().await?;
    let dbtx = conn.transaction().await?;

    source.upsert(&dbtx, ctx, now, &fetched).await?;

    // Snapshot: seen gateways (is_seen=true, with ping result) + previously-known
    // but currently-absent ones (is_seen=false, unreachable).
    let schema = source.schema();
    for gw in &polled {
        let (reachable, latency) = ping.get(&gw.gateway_id).copied().unwrap_or((false, None));
        dbtx.execute(
            &format!(
                "INSERT INTO {schema}.gateway_poll_snapshots
                      (federation_id, gateway_id, poll_time, is_seen, reachable, latency_ms)
                      VALUES ($1,$2,$3,true,$4,$5) ON CONFLICT DO NOTHING"
            ),
            &[&fed, &gw.gateway_id, &now, &reachable, &latency],
        )
        .await?;
    }
    dbtx.execute(
        &format!(
            "INSERT INTO {schema}.gateway_poll_snapshots
                  (federation_id, gateway_id, poll_time, is_seen, reachable, latency_ms)
                  SELECT $1, g.gateway_id, $2, false, false, NULL
                  FROM {schema}.gateways g
                  WHERE g.federation_id = $1
                    AND g.gateway_id <> ALL($3::text[])
                  ON CONFLICT DO NOTHING"
        ),
        &[
            &fed,
            &now,
            &polled
                .iter()
                .map(|g| g.gateway_id.clone())
                .collect::<Vec<_>>(),
        ],
    )
    .await?;

    // Prune old snapshots on a coarse schedule.
    let prune_interval = PRUNE_INTERVAL_HOURS * 3600;
    if now.timestamp().rem_euclid(prune_interval) < (POLL_INTERVAL_MINUTES as i64 * 60) {
        let cutoff = now - chrono::Duration::days(SNAPSHOT_RETENTION_DAYS);
        dbtx.execute(
            &format!(
                "DELETE FROM {schema}.gateway_poll_snapshots WHERE federation_id=$1 AND poll_time<$2"
            ),
            &[&fed, &cutoff],
        )
        .await?;
    }
    dbtx.commit().await?;
    info!(
        "{}: polled {} gateway(s) for {}",
        schema,
        polled.len(),
        ctx.federation_id
    );
    Ok(())
}
