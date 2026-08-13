use anyhow::Context;
use axum::extract::{Path, State};
use axum::http::header::CACHE_CONTROL;
use axum::response::IntoResponse;
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

/// Header applied to the fleet/totals/histogram/summary endpoints, which are
/// hammered by the two hottest pages (home + federation detail) but only
/// need up-to-`FO_REFRESH_INTERVAL_SECS`-fresh data.
const HOT_CACHE_CONTROL: &str = "public, max-age=30";

pub fn get_federations_routes() -> Router<AppState> {
    Router::new()
        .route("/", get(list_observed_federations))
        .route("/", put(add_observed_federation))
        .route("/totals", get(get_federation_totals))
        // TODO: move to nostr module
        .route("/nostr/rating", put(publish_rating_event))
        .route("/:federation_id", get(get_federation_overview))
        .route("/:federation_id/summary", get(get_federation_summary))
        .route("/:federation_id/config", get(get_federation_config))
        .route("/:federation_id/meta", get(get_federation_meta))
        .route("/:federation_id/health", get(get_federation_health))
        .route(
            "/:federation_id/transactions",
            get(super::transactions::list_transactions),
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
            "/:federation_id/tx/:txid",
            get(super::transactions::transaction_detail),
        )
        .route(
            "/:federation_id/user-transactions/:user_tx_key",
            get(super::user_transactions::user_transaction_detail),
        )
        .route(
            "/:federation_id/sessions",
            get(super::sessions::list_sessions),
        )
        .route(
            "/:federation_id/sessions/count",
            get(super::sessions::count_sessions),
        )
        .route(
            "/:federation_id/sessions/:session_index",
            get(super::sessions::session_items),
        )
        .route(
            "/:federation_id/consensus",
            get(super::consensus::consensus_stream),
        )
        .route("/:federation_id/live", get(super::live::federation_live))
        .route("/:federation_id/backfill", post(backfill_federation))
}

async fn list_observed_federations(
    State(state): State<AppState>,
) -> crate::error::Result<impl IntoResponse> {
    let summaries = state.observer.list_federation_summaries().await?;
    Ok(([(CACHE_CONTROL, HOT_CACHE_CONTROL)], Json(summaries)))
}

async fn get_federation_summary(
    Path(federation_id): Path<FederationId>,
    State(state): State<AppState>,
) -> crate::error::Result<impl IntoResponse> {
    let summary = state.observer.federation_summary(federation_id).await?;
    Ok(([(CACHE_CONTROL, HOT_CACHE_CONTROL)], Json(summary)))
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
) -> crate::error::Result<impl IntoResponse> {
    let totals = state.observer.totals().await?;
    Ok(([(CACHE_CONTROL, HOT_CACHE_CONTROL)], Json(totals)))
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
        let federations = self.list_federations().await?;

        // Fetch the fleet-wide guardian health ONCE and share it across every
        // per-federation summary, rather than recomputing it per federation.
        let health = self.get_guardian_health_summary().await?;

        join_all(federations.into_iter().map(|federation| {
            let health = &health;
            async move {
                self.federation_summary_with_health(federation.federation_id, health)
                    .await
            }
        }))
        .await
        .into_iter()
        .collect()
    }

    pub async fn federation_summary(
        &self,
        federation_id: FederationId,
    ) -> anyhow::Result<FederationSummary> {
        let health = self.get_guardian_health_summary().await?;
        self.federation_summary_with_health(federation_id, &health)
            .await
    }

    /// Summary for a single federation: name, health, assets, recent
    /// activity, invite code and nostr rating. Used both by
    /// `list_federation_summaries` (fleet overview) and the single-federation
    /// `/federations/:federation_id/summary` endpoint.
    /// Builds one federation's summary using an already-fetched fleet health
    /// map, so callers folding over many federations pay the (fleet-wide)
    /// guardian-health query once instead of once per federation.
    async fn federation_summary_with_health(
        &self,
        federation_id: FederationId,
        health_summary: &std::collections::BTreeMap<FederationId, FederationHealth>,
    ) -> anyhow::Result<FederationSummary> {
        let federation = self
            .get_federation(federation_id)
            .await?
            .context("Federation doesn't exist")?;

        let deposits = self.get_federation_assets(federation.federation_id).await?;

        let (total_tx_count, total_volume) = self
            .get_federation_all_time_totals(federation.federation_id)
            .await?;

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

        let health = health_summary
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
            total_volume,
            total_tx_count,
        })
    }

    /// All-time (`tx_count`, `volume`) for a federation, summed from the
    /// `federation_tx_daily` matview (schema/core/v5.sql). That matview already
    /// precomputes per-federation daily rollups keyed by `(federation_id,
    /// day)`, so this is a cheap index range scan rather than a full
    /// `transactions`/`transaction_inputs` aggregate. `volume` mirrors
    /// `federation_tx_daily.volume_msat` (summed transaction input amounts),
    /// the same grain the fleet-wide `/federations/totals` reports. Federations
    /// with no rows yet return `(0, Amount::ZERO)`.
    async fn get_federation_all_time_totals(
        &self,
        federation_id: FederationId,
    ) -> anyhow::Result<(u64, Amount)> {
        #[derive(Debug, FromRow)]
        struct AllTimeTotalsRow {
            tx_count: i64,
            volume_msat: i64,
        }

        // language=postgresql
        let row = query_one::<AllTimeTotalsRow>(
            &self.connection().await?,
            "
            SELECT COALESCE(SUM(tx_count), 0)::bigint    AS tx_count,
                   COALESCE(SUM(volume_msat), 0)::bigint AS volume_msat
            FROM federation_tx_daily
            WHERE federation_id = $1
        ",
            &[&federation_id.consensus_encode_to_vec()],
        )
        .await?;

        Ok((
            row.tx_count as u64,
            Amount::from_msats(row.volume_msat as u64),
        ))
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

        // Served from the `federation_tx_daily` matview (schema/core/v5.sql),
        // which precomputes this per-day aggregate for every federation. This
        // used to run a correlated per-tx input-sum aggregate live, once per
        // federation on the home page (73x). We fetch a slightly wider window
        // than requested and `last_n_day_iter` below selects the exact days.
        // language=postgresql
        let activity = query::<FederationActivityRow>(
            &self.connection().await?,
            "
            SELECT day         AS date,
                   tx_count    AS tx_count,
                   volume_msat AS total_amount
            FROM federation_tx_daily
            WHERE federation_id = $1 AND day >= $2
            ORDER BY day;
        ",
            &[
                &federation_id.consensus_encode_to_vec(),
                &(now - chrono::Duration::days(8)).date_naive(),
            ],
        )
        .await?;

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

    /// On-chain assets held by the federation, summing the v1 `wallet` and v2
    /// `walletv2` modules (walletv2-only federations would otherwise net to 0).
    ///
    /// - v1 `wallet`: peg-in deposits (inputs) minus peg-out withdrawals
    ///   (outputs), the exact consensus values.
    /// - v2 `walletv2`: the EXACT current on-chain balance, read as the value
    ///   of the federation's single consolidated UTXO (the latest resolved
    ///   `fmo_walletv2.wallet_utxos` row, derived from the wallet-tx txids
    ///   announced in consensus and looked up on an explorer). If no such row
    ///   is resolved yet (e.g. the version-bump replay + explorer backfill
    ///   hasn't caught up), we fall back to the old input(+)/output(-) netting,
    ///   which is fee-approximate (provably low by the mandatory per-item
    ///   walletv2 fees) but never worse than before this change.
    pub async fn get_federation_assets(
        &self,
        federation_id: FederationId,
    ) -> anyhow::Result<Amount> {
        let fed = federation_id.consensus_encode_to_vec();
        let conn = self.connection().await?;

        #[derive(Debug, FromRow)]
        struct AssetNettingRow {
            wallet_net_msat: i64,
            walletv2_net_msat: i64,
        }

        // Baseline: per-module netting of exact input/output amounts. Always
        // available (reads core structural tables).
        let netting = query_one::<AssetNettingRow>(
            &conn,
            "
        SELECT
            CAST((SELECT COALESCE(SUM(amount_msat), 0) FROM transaction_inputs
                  WHERE kind = 'wallet' AND federation_id = $1) -
                 (SELECT COALESCE(SUM(amount_msat), 0) FROM transaction_outputs
                  WHERE kind = 'wallet' AND federation_id = $1) AS BIGINT) AS wallet_net_msat,
            CAST((SELECT COALESCE(SUM(amount_msat), 0) FROM transaction_inputs
                  WHERE kind = 'walletv2' AND federation_id = $1) -
                 (SELECT COALESCE(SUM(amount_msat), 0) FROM transaction_outputs
                  WHERE kind = 'walletv2' AND federation_id = $1) AS BIGINT) AS walletv2_net_msat
        ",
            &[&fed],
        )
        .await?;

        // Prefer the exact walletv2 UTXO value when available. Guard the
        // cross-schema reference so core stays decoupled from whether the
        // walletv2 module (and its schema) is registered at all — Postgres
        // validates all table references at parse time, so we can't reference
        // `fmo_walletv2.wallet_utxos` in a query unless it exists.
        let walletv2_exact_msat = if self.walletv2_utxos_table_exists(&conn).await? {
            query_value::<Option<i64>>(
                &conn,
                "SELECT (SELECT utxo_value_msat FROM fmo_walletv2.wallet_utxos
                         WHERE federation_id = $1 AND utxo_value_msat IS NOT NULL
                         ORDER BY session_index DESC, item_index DESC
                         LIMIT 1)",
                &[&fed],
            )
            .await?
        } else {
            None
        };

        let walletv2_msat = walletv2_exact_msat.unwrap_or(netting.walletv2_net_msat);
        let total_msat = (netting.wallet_net_msat + walletv2_msat).max(0);

        Ok(Amount::from_msats(total_msat as u64))
    }

    /// Whether the `fmo_walletv2.wallet_utxos` table exists (i.e. the walletv2
    /// module is registered and its schema migrated). Uses `to_regclass`,
    /// which returns NULL for an absent relation instead of erroring.
    async fn walletv2_utxos_table_exists(
        &self,
        conn: &deadpool_postgres::Object,
    ) -> anyhow::Result<bool> {
        Ok(query_value::<Option<String>>(
            conn,
            "SELECT to_regclass('fmo_walletv2.wallet_utxos')::text",
            &[],
        )
        .await?
        .is_some())
    }

    /// Returns the cached totals (refreshed on the matview refresh cycle,
    /// see `refresh_views_inner`). Before the first refresh cycle has
    /// completed, computes and caches them on demand so a freshly started
    /// process still serves totals immediately.
    pub async fn totals(&self) -> anyhow::Result<FedimintTotals> {
        if let Some(totals) = self.cached_totals().read().await.clone() {
            return Ok(totals);
        }

        let totals = self.compute_totals().await?;
        *self.cached_totals().write().await = Some(totals.clone());
        Ok(totals)
    }

    /// Computes the fleet-wide totals from scratch. Expensive: scans
    /// `transactions`/`transaction_inputs` in full. Called on the matview
    /// refresh cycle (`refresh_views_inner`) and, as a fallback, once by
    /// [`Self::totals`] before the cache is warm.
    pub async fn compute_totals(&self) -> anyhow::Result<FedimintTotals> {
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
