use anyhow::Context;
use axum::extract::{Path, State};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use axum_auth::AuthBearer;
use chrono::NaiveDate;
use fedimint_core::config::{FederationId, JsonClientConfig};
use fedimint_core::encoding::Encodable;
use fedimint_core::invite_code::InviteCode;
use fedimint_core::Amount;
use fmo_api_types::{FederationActivity, FederationHealth, FederationSummary, FedimintTotals};
use futures::future::join_all;
use postgres_from_row::FromRow;
use serde::Deserialize;
use serde_json::json;

use crate::api::AppState;
use crate::observer::FederationObserver;
use crate::query::{query, query_one, query_value};
use crate::services::meta::{config_to_json, MetaFieldsExt};

pub fn get_federations_routes() -> Router<AppState> {
    Router::new()
        .route("/", get(list_observed_federations))
        .route("/", put(add_observed_federation))
        .route("/totals", get(get_federation_totals))
        // TODO: move to nostr module
        .route("/nostr/rating", put(publish_rating_event))
        .route("/:federation_id", get(get_federation_overview))
        .route("/:federation_id/config", get(get_federation_config))
        .route("/:federation_id/meta", get(get_federation_meta))
        .route("/:federation_id/health", get(get_federation_health))
        .route(
            "/:federation_id/transactions",
            get(super::transactions::list_transactions),
        )
        .route(
            "/:federation_id/transactions/:transaction_id",
            get(super::transactions::transaction),
        )
        .route(
            "/:federation_id/transactions/count",
            get(super::transactions::count_transactions),
        )
        .route(
            "/:federation_id/transactions/histogram",
            get(super::transactions::transaction_histogram),
        )
        .route(
            "/:federation_id/sessions",
            get(super::sessions::list_sessions),
        )
        .route(
            "/:federation_id/sessions/count",
            get(super::sessions::count_sessions),
        )
        .route("/:federation_id/backfill", post(backfill_federation))
}

async fn list_observed_federations(
    State(state): State<AppState>,
) -> crate::error::Result<Json<Vec<FederationSummary>>> {
    Ok(state.observer.list_federation_summaries().await?.into())
}

async fn add_observed_federation(
    AuthBearer(auth): AuthBearer,
    State(state): State<AppState>,
    Json(body): Json<serde_json::Value>,
) -> crate::error::Result<Json<FederationId>> {
    state.observer.check_auth(&auth)?;

    let invite: InviteCode = serde_json::from_value(
        body.get("invite")
            .context("Request did not contain invite field")?
            .clone(),
    )
    .context("Invalid invite code")?;
    Ok(state.observer.add_federation(&invite).await?.into())
}

async fn get_federation_config(
    Path(federation_id): Path<FederationId>,
    State(state): State<AppState>,
) -> crate::error::Result<Json<JsonClientConfig>> {
    let config = state
        .observer
        .get_federation(federation_id)
        .await?
        .context("Federation not observed, you might want to try /config/:federation_invite")?
        .config;
    Ok(config_to_json(config, state.observer.registry())?.into())
}

async fn get_federation_meta(
    Path(federation_id): Path<FederationId>,
    State(state): State<AppState>,
) -> crate::error::Result<Json<crate::services::meta::MetaFields>> {
    let config = state
        .observer
        .get_federation(federation_id)
        .await?
        .context("Federation not observed, you might want to try /config/:federation_invite")?
        .config;

    super::config::federation_meta(&config_to_json(config, state.observer.registry())?, &state)
        .await
}

async fn get_federation_health(
    Path(federation_id): Path<FederationId>,
    State(state): State<AppState>,
) -> crate::error::Result<
    Json<std::collections::BTreeMap<fedimint_core::PeerId, fmo_api_types::GuardianHealth>>,
> {
    Ok(state
        .observer
        .get_guardian_health(federation_id)
        .await?
        .into())
}

async fn get_federation_overview(
    Path(federation_id): Path<FederationId>,
    State(state): State<AppState>,
) -> crate::error::Result<Json<serde_json::Value>> {
    let session_count = state
        .observer
        .federation_session_count(federation_id)
        .await?;
    let total_assets_msat = state.observer.get_federation_assets(federation_id).await?;

    Ok(json!({
        "session_count": session_count,
        "total_assets_msat": total_assets_msat
    })
    .into())
}

async fn get_federation_totals(
    State(state): State<AppState>,
) -> crate::error::Result<Json<FedimintTotals>> {
    Ok(state.observer.totals().await?.into())
}

async fn publish_rating_event(
    State(state): State<AppState>,
    Json(event): Json<nostr_sdk::Event>,
) -> crate::error::Result<()> {
    Ok(state.observer.submit_rating(event).await?)
}

#[derive(Deserialize, Debug)]
struct BackfillParams {
    session_start: Option<i32>,
    // Kept for API compatibility; replay always runs to the current tip.
    #[allow(dead_code)]
    session_end: Option<i32>,
}

/// Resets all module cursors of the federation to `session_start` (default 0).
/// The regular dispatch engine then replays the sessions; all writes are
/// idempotent so re-processing existing data is safe.
async fn backfill_federation(
    Path(federation_id): Path<FederationId>,
    AuthBearer(auth): AuthBearer,
    State(state): State<AppState>,
    Json(params): Json<BackfillParams>,
) -> crate::error::Result<()> {
    state.observer.check_auth(&auth)?;

    let session_start = params.session_start.unwrap_or(0);
    state
        .observer
        .connection()
        .await?
        .execute(
            "UPDATE module_progress SET next_session_index = LEAST(next_session_index, $2)
             WHERE federation_id = $1",
            &[&federation_id.consensus_encode_to_vec(), &session_start],
        )
        .await
        .map_err(anyhow::Error::from)?;

    Ok(())
}

impl FederationObserver {
    pub async fn list_federation_summaries(&self) -> anyhow::Result<Vec<FederationSummary>> {
        // TODO: possibly combine list and health query
        let federations = self.list_federations().await?;

        let federation_health = self.get_guardian_health_summary().await?;

        join_all(federations.into_iter().map(|federation| {
            let federation_health_ref = &federation_health;
            async move {
                let deposits = self.get_federation_assets(federation.federation_id).await?;

                let name = self
                    .consensus_meta_cache()
                    .fetch_meta_cached(&config_to_json(federation.config.clone(), self.registry())?)
                    .await
                    .and_then(|meta| meta.get_as::<String>("federation_name"))
                    .or_else(|| {
                        federation
                            .config
                            .global
                            .meta
                            .get("federation_name")
                            .cloned()
                    });

                let health = federation_health_ref
                    .get(&federation.federation_id)
                    .copied()
                    .unwrap_or(FederationHealth::Offline);

                let last_7d_activity = self
                    .federation_activity(federation.federation_id, 7)
                    .await?;

                let (first_peer_id, first_peer_url) = federation
                    .config
                    .global
                    .api_endpoints
                    .first_key_value()
                    .expect("At least one peer");
                let invite = InviteCode::new(
                    first_peer_url.url.clone(),
                    *first_peer_id,
                    federation.federation_id,
                    None,
                )
                .to_string();

                Ok(FederationSummary {
                    id: federation.federation_id,
                    name,
                    last_7d_activity,
                    deposits,
                    invite,
                    nostr_votes: self.federation_rating(federation.federation_id).await?,
                    health,
                })
            }
        }))
        .await
        .into_iter()
        .collect()
    }

    async fn federation_activity(
        &self,
        federation_id: FederationId,
        days: u32,
    ) -> anyhow::Result<Vec<FederationActivity>> {
        #[derive(Debug, FromRow)]
        struct FederationActivityRow {
            date: NaiveDate,
            tx_count: i64,
            total_amount: i64,
        }

        let now = chrono::offset::Utc::now();

        // language=postgresql
        let activity = query::<FederationActivityRow>(&self.connection().await?, "
            SELECT DATE(st.estimated_session_timestamp) AS date,
                   COUNT(DISTINCT t.txid)::bigint       AS tx_count,
                   COALESCE(SUM((SELECT SUM(amount_msat)
                        FROM transaction_inputs
                        WHERE transaction_inputs.txid = t.txid AND transaction_inputs.federation_id = t.federation_id))::bigint, 0)   AS total_amount
            FROM transactions t
                     JOIN
                 session_times st ON t.session_index = st.session_index AND t.federation_id = st.federation_id
            WHERE t.federation_id = $1  AND st.estimated_session_timestamp >= $2
            GROUP BY date
            ORDER BY date;
        ", &[&federation_id.consensus_encode_to_vec(), &(now - chrono::Duration::days(8)).naive_utc()]).await?;

        Ok(last_n_day_iter(now.date_naive(), days)
            .map(|date| {
                let (tx_count, total_amt) = activity
                    .iter()
                    .find(|row| row.date == date)
                    .map(|row| (row.tx_count, row.total_amount))
                    .unwrap_or((0, 0));
                FederationActivity {
                    num_transactions: tx_count as u64,
                    amount_transferred: Amount::from_msats(total_amt as u64),
                }
            })
            .collect())
    }

    pub async fn get_federation_assets(
        &self,
        federation_id: FederationId,
    ) -> anyhow::Result<Amount> {
        let total_assets_msat = query_value::<i64>(
            &self.connection().await?,
            "
        SELECT
            CAST((SELECT COALESCE(SUM(amount_msat), 0)
             FROM transaction_inputs
             WHERE kind = 'wallet' AND federation_id = $1) -
            (SELECT COALESCE(SUM(amount_msat), 0)
             FROM transaction_outputs
             WHERE kind = 'wallet' AND federation_id = $1) AS BIGINT) AS net_amount_msat
        ",
            &[&federation_id.consensus_encode_to_vec()],
        )
        .await?;

        Ok(Amount::from_msats(total_assets_msat as u64))
    }

    pub async fn totals(&self) -> anyhow::Result<FedimintTotals> {
        #[derive(Debug, FromRow)]
        struct FedimintTotalsResult {
            federations: i64,
            tx_count: i64,
            tx_volume: i64,
        }

        let offline_federations = self
            .get_guardian_health_summary()
            .await?
            .values()
            .filter(|&health| *health == FederationHealth::Offline)
            .count() as u64;

        let totals = query_one::<FedimintTotalsResult>(
            &self.connection().await?,
            // language=postgresql
            "
                SELECT (SELECT count(*) from federations)::bigint               as federations,
                       (SELECT count(*) from transactions)::bigint               as tx_count,
                       (SELECT COALESCE(sum(amount_msat), 0) from transaction_inputs)::bigint as tx_volume
            ",
            &[],
        )
        .await?;

        Ok(FedimintTotals {
            federations: (totals.federations as u64) - offline_federations,
            tx_count: totals.tx_count as u64,
            tx_volume: Amount::from_msats(totals.tx_volume as u64),
        })
    }
}

fn last_n_day_iter(now: NaiveDate, days: u32) -> impl Iterator<Item = NaiveDate> {
    (0..days)
        .rev()
        .map(move |day| now - chrono::Duration::days(day as i64))
}

#[cfg(test)]
mod tests {
    use super::last_n_day_iter;

    #[test]
    fn test_day_iter() {
        let now = chrono::offset::Utc::now().date_naive();
        let days = 7;
        let last_7_days = last_n_day_iter(now, days).collect::<Vec<_>>();
        assert_eq!(last_7_days.len(), days as usize);
        assert_eq!(last_7_days[6], now);
        assert_eq!(last_7_days[0], now - chrono::Duration::days(6));
    }
}
