use std::collections::BTreeMap;

use anyhow::Context;
use axum::extract::{Path, State};
use axum::Json;
use chrono::NaiveDate;
use fedimint_core::config::FederationId;
use fedimint_core::encoding::Encodable;
use fedimint_core::{Amount, TransactionId};
use fmo_api_types::{FederationActivity, TxDetail, TxItemPart};
use postgres_from_row::FromRow;

use crate::api::AppState;
use crate::federation;
use crate::observer::FederationObserver;
use crate::query::{query, query_opt, query_value};

pub(super) async fn list_transactions(
    Path(federation_id): Path<FederationId>,
    State(state): State<AppState>,
) -> crate::error::Result<Json<Vec<TransactionId>>> {
    Ok(state
        .observer
        .federation_transaction_list(federation_id)
        .await?
        .into_iter()
        .map(|tx| tx.txid)
        .collect::<Vec<_>>()
        .into())
}

pub(super) async fn count_transactions(
    Path(federation_id): Path<FederationId>,
    State(state): State<AppState>,
) -> crate::error::Result<Json<u64>> {
    Ok(state
        .observer
        .federation_transaction_count(federation_id)
        .await?
        .into())
}

/// Structured (rich) transaction detail: inputs/outputs read straight from
/// `transaction_inputs`/`transaction_outputs` (kind + amount + details).
pub(super) async fn transaction_detail(
    Path((federation_id, txid_hex)): Path<(FederationId, String)>,
    State(state): State<AppState>,
) -> crate::error::Result<Json<TxDetail>> {
    let txid = hex::decode(&txid_hex).context("Invalid txid hex")?;
    Ok(state
        .observer
        .federation_transaction_detail(federation_id, &txid)
        .await?
        .into())
}

pub(super) async fn transaction_histogram(
    Path(federation_id): Path<FederationId>,
    State(state): State<AppState>,
) -> crate::error::Result<impl axum::response::IntoResponse> {
    let histogram = state
        .observer
        .transaction_histogram(federation_id)
        .await?
        .into_iter()
        .map(|histogram_entry| {
            (
                histogram_entry.date,
                FederationActivity {
                    num_transactions: histogram_entry.count as u64,
                    amount_transferred: Amount::from_msats(histogram_entry.amount as u64),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();

    Ok((
        [(axum::http::header::CACHE_CONTROL, "public, max-age=30")],
        Json(histogram),
    ))
}

impl FederationObserver {
    pub async fn federation_transaction_list(
        &self,
        federation_id: FederationId,
    ) -> anyhow::Result<Vec<federation::Transaction>> {
        self.get_federation(federation_id)
            .await
            .context("Federation doesn't exist")?;

        query::<federation::Transaction>(
            &self.connection().await?,
            "SELECT txid, session_index, item_index, data FROM transactions WHERE federation_id = $1",
            &[&federation_id.consensus_encode_to_vec()]
        ).await
    }

    pub async fn federation_transaction_count(
        &self,
        federation_id: FederationId,
    ) -> anyhow::Result<u64> {
        self.get_federation(federation_id)
            .await
            .context("Federation doesn't exist")?;

        Ok(query_value::<i64>(
            &self.connection().await?,
            "SELECT COALESCE(COUNT(txid), 0) FROM transactions WHERE federation_id = $1",
            &[&federation_id.consensus_encode_to_vec()],
        )
        .await? as u64)
    }

    /// Structured transaction detail: `transactions` row (session/item
    /// index) + `transaction_inputs`/`transaction_outputs` (index, kind,
    /// amount, details) ordered by index, + the tx's gold-layer
    /// `user_tx_key` (if it's a member leg of one), resolved via
    /// `user_transaction_txs`.
    pub async fn federation_transaction_detail(
        &self,
        federation_id: FederationId,
        txid: &[u8],
    ) -> anyhow::Result<TxDetail> {
        self.get_federation(federation_id)
            .await
            .context("Federation doesn't exist")?;

        let fed = federation_id.consensus_encode_to_vec();

        #[derive(FromRow)]
        struct TxRow {
            session_index: i64,
            item_index: i64,
        }

        let tx = query_opt::<TxRow>(
            &self.connection().await?,
            "SELECT session_index::bigint, item_index::bigint FROM transactions
             WHERE federation_id = $1 AND txid = $2",
            &[&fed, &txid],
        )
        .await?
        .context("Transaction not found")?;

        #[derive(FromRow)]
        struct PartRow {
            index: i32,
            kind: String,
            amount_msat: Option<i64>,
            details: Option<serde_json::Value>,
        }

        let inputs = query::<PartRow>(
            &self.connection().await?,
            "SELECT in_index::int AS index, kind, amount_msat, details FROM transaction_inputs
             WHERE federation_id = $1 AND txid = $2 ORDER BY in_index",
            &[&fed, &txid],
        )
        .await?;

        let outputs = query::<PartRow>(
            &self.connection().await?,
            "SELECT out_index::int AS index, kind, amount_msat, details FROM transaction_outputs
             WHERE federation_id = $1 AND txid = $2 ORDER BY out_index",
            &[&fed, &txid],
        )
        .await?;

        #[derive(FromRow)]
        struct UserTxKeyRow {
            user_tx_key: String,
        }

        let user_tx_key = query_opt::<UserTxKeyRow>(
            &self.connection().await?,
            "SELECT encode(user_tx_key, 'hex') AS user_tx_key FROM user_transaction_txs
             WHERE federation_id = $1 AND txid = $2 LIMIT 1",
            &[&fed, &txid],
        )
        .await?
        .map(|row| row.user_tx_key);

        #[derive(FromRow)]
        struct EcashPrivacyRow {
            ecash_anon_bits: Option<f64>,
            ecash_issuance_bits: Option<f64>,
        }

        let ecash_privacy = query_opt::<EcashPrivacyRow>(
            &self.connection().await?,
            "SELECT ecash_anon_bits, ecash_issuance_bits FROM transaction_privacy
             WHERE federation_id = $1 AND txid = $2",
            &[&fed, &txid],
        )
        .await?;
        let ecash_anon_bits = ecash_privacy.as_ref().and_then(|row| row.ecash_anon_bits);
        let ecash_issuance_bits = ecash_privacy.and_then(|row| row.ecash_issuance_bits);

        let to_part = |row: PartRow| TxItemPart {
            index: row.index,
            kind: row.kind,
            amount_msat: row.amount_msat,
            details: row.details,
        };

        Ok(TxDetail {
            txid: hex::encode(txid),
            session_index: tx.session_index,
            item_index: tx.item_index,
            inputs: inputs.into_iter().map(to_part).collect(),
            outputs: outputs.into_iter().map(to_part).collect(),
            user_tx_key,
            ecash_anon_bits,
            ecash_issuance_bits,
        })
    }

    pub async fn transaction_histogram(
        &self,
        federation_id: FederationId,
    ) -> anyhow::Result<Vec<HistogramEntry>> {
        // Served from the `federation_tx_daily` matview (see schema/core/v5.sql),
        // which precomputes this exact per-day aggregate for every federation on
        // the refresh cycle. Previously this recomputed the whole-history
        // aggregate live on each request (10-18s on busy federations).
        // language=postgresql
        const QUERY: &str = "
            SELECT day          AS date,
                   tx_count     AS count,
                   volume_msat  AS amount
            FROM federation_tx_daily
            WHERE federation_id = $1
            ORDER BY day;
        ";

        // Check federation exists
        let _federation = self
            .get_federation(federation_id)
            .await?
            .context("Federation doesn't exist")?;

        let histogram = query::<HistogramEntry>(
            &self.connection().await?,
            QUERY,
            &[&federation_id.consensus_encode_to_vec()],
        )
        .await?;

        Ok(histogram)
    }
}

#[derive(Debug, Clone, FromRow)]
pub struct HistogramEntry {
    pub date: NaiveDate,
    pub count: i64,
    pub amount: i64,
}
