//! LNv2 gateway monitoring on the shared `fmo_core::gateway_poll` harness.
//! LNv2's registry is thinner than LNv1's: it returns bare gateway API URLs
//! (no vetting/node-key/fees), so the URL string doubles as both
//! `gateway_id` and `api_endpoint`.

use std::collections::BTreeSet;

use axum::extract::{Path, State};
use axum::Json;
use chrono::{DateTime, Utc};
use fedimint_api_client::api::{DynGlobalApi, FederationApiExt};
use fedimint_core::config::FederationId;
use fedimint_core::encoding::Encodable;
use fedimint_core::module::ApiRequestErased;
use fedimint_core::util::SafeUrl;
use fedimint_lnv2_common::endpoint_constants::GATEWAYS_ENDPOINT;
use fmo_core::api::ModuleApiState;
use fmo_core::gateway_poll::{GatewaySource, PolledGateway};
use fmo_core::module::ModuleTaskCtx;
use fmo_core::query::query;
use futures::future::join_all;
use tokio_postgres::Transaction;
use tracing::warn;

/// The LNv2 `GatewaySource`: queries every peer's `GATEWAYS_ENDPOINT` on the
/// lnv2 module instance and merges the union of gateway API URLs. The poll
/// loop, ping and snapshot/prune bookkeeping live in
/// `fmo_core::gateway_poll`.
pub(crate) struct LnV2GatewaySource;

#[async_trait::async_trait]
impl GatewaySource for LnV2GatewaySource {
    type Fetched = Vec<String>;

    fn schema(&self) -> &'static str {
        "fmo_lnv2"
    }

    async fn fetch(
        &self,
        ctx: &ModuleTaskCtx,
        api: &DynGlobalApi,
    ) -> anyhow::Result<Self::Fetched> {
        let instance_id = ctx
            .config
            .modules
            .iter()
            .find_map(|(&id, module)| (module.kind.as_str() == "lnv2").then_some(id))
            .ok_or_else(|| anyhow::anyhow!("no lnv2 module in config"))?;
        let peer_ids: Vec<_> = ctx.config.global.api_endpoints.keys().copied().collect();

        let results = join_all(peer_ids.into_iter().map(|peer| async move {
            let result = api
                .with_module(instance_id)
                .request_single_peer::<Vec<SafeUrl>>(
                    GATEWAYS_ENDPOINT.to_owned(),
                    ApiRequestErased::default(),
                    peer,
                )
                .await;
            (peer, result)
        }))
        .await;

        let mut urls = BTreeSet::new();
        let mut any = false;
        for (peer, result) in results {
            match result {
                Ok(r) => {
                    any = true;
                    for u in r {
                        urls.insert(u.to_string());
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to fetch lnv2 gateways from peer {} for {}: {:?}",
                        peer, ctx.federation_id, e
                    );
                }
            }
        }
        if !any {
            anyhow::bail!(
                "no lnv2 gateway responses from any peer for {}",
                ctx.federation_id
            );
        }

        Ok(urls.into_iter().collect())
    }

    fn polled_gateways(&self, fetched: &Self::Fetched) -> Vec<PolledGateway> {
        fetched
            .iter()
            .map(|u| PolledGateway {
                gateway_id: u.clone(),
                api_endpoint: Some(u.clone()),
            })
            .collect()
    }

    async fn upsert(
        &self,
        dbtx: &Transaction<'_>,
        ctx: &ModuleTaskCtx,
        now: DateTime<Utc>,
        fetched: &Self::Fetched,
    ) -> anyhow::Result<()> {
        let fed = ctx.federation_id.consensus_encode_to_vec();
        if !fetched.is_empty() {
            dbtx.execute(
                "INSERT INTO gateways (federation_id, gateway_id, api_endpoint, first_seen, last_seen)
                 SELECT $1, u, u, $2, $2 FROM UNNEST($3::text[]) AS u
                 ON CONFLICT (federation_id, gateway_id) DO UPDATE SET last_seen = EXCLUDED.last_seen",
                &[&fed, &now, fetched],
            )
            .await?;
        }
        Ok(())
    }
}

/// LNv2's thin gateway listing: unlike LNv1 it computes no contract-derived
/// activity metrics (uptime/reachability from `gateway_poll_snapshots` could
/// be folded in later if wanted, but isn't required for parity).
pub(crate) async fn get_federation_gateways(
    Path(federation_id): Path<FederationId>,
    State(state): State<ModuleApiState>,
) -> fmo_core::error::Result<Json<Vec<serde_json::Value>>> {
    #[derive(postgres_from_row::FromRow)]
    struct Row {
        gateway_id: String,
        api_endpoint: String,
        first_seen: DateTime<Utc>,
        last_seen: DateTime<Utc>,
    }

    let conn = state.pool.get().await?;
    let rows = query::<Row>(
        &conn,
        "SELECT gateway_id, api_endpoint, first_seen, last_seen FROM fmo_lnv2.gateways
         WHERE federation_id=$1 ORDER BY last_seen DESC",
        &[&federation_id.consensus_encode_to_vec()],
    )
    .await?;

    Ok(Json(
        rows.into_iter()
            .map(|r| {
                serde_json::json!({
                    "gateway_id": r.gateway_id,
                    "api_endpoint": r.api_endpoint,
                    "first_seen": r.first_seen,
                    "last_seen": r.last_seen,
                })
            })
            .collect(),
    ))
}
