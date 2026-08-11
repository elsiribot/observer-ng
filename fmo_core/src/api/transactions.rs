use std::collections::BTreeMap;
use std::io::Cursor;

use anyhow::Context;
use axum::extract::{Path, State};
use axum::Json;
use chrono::NaiveDate;
use fedimint_core::config::FederationId;
use fedimint_core::core::{DynInput, DynOutput, DynUnknown};
use fedimint_core::encoding::Encodable;
use fedimint_core::{Amount, TransactionId};
use fmo_api_types::{FederationActivity, TxDetail, TxItemPart};
use postgres_from_row::FromRow;
use serde::Serialize;

use crate::api::AppState;
use crate::federation;
use crate::observer::FederationObserver;
use crate::query::{query, query_one, query_opt, query_value};

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

pub(super) async fn transaction(
    Path((federation_id, transaction_id)): Path<(FederationId, TransactionId)>,
    State(state): State<AppState>,
) -> crate::error::Result<Json<DebugTransaction>> {
    Ok(state
        .observer
        .transaction_details(federation_id, transaction_id)
        .await?
        .into())
}

/// Structured (rich) transaction detail: inputs/outputs read straight from
/// `transaction_inputs`/`transaction_outputs` (kind + amount + details), not
/// the Debug-string decode `transaction()` above produces.
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
) -> crate::error::Result<Json<BTreeMap<NaiveDate, FederationActivity>>> {
    Ok(state
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
        .collect::<BTreeMap<_, _>>()
        .into())
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

    pub async fn transaction_details(
        &self,
        federation_id: FederationId,
        transaction_id: TransactionId,
    ) -> anyhow::Result<DebugTransaction> {
        let cfg = self
            .get_federation(federation_id)
            .await?
            .context("Federation doesn't exist")?
            .config;

        let tx = query_one::<federation::Transaction>(&self.connection().await?, "SELECT txid, session_index, item_index, data FROM transactions WHERE federation_id = $1 AND txid = $2", &[&federation_id.consensus_encode_to_vec(), &transaction_id.consensus_encode_to_vec()]).await?;

        let decoders = self.registry().decoders(&cfg);

        let inputs = tx
            .data
            .inputs
            .into_iter()
            .map(|input| {
                let module_instance_id = input.module_instance_id();
                match input.as_any().downcast_ref::<DynUnknown>() {
                    Some(undecoded) => decoders
                        .get(module_instance_id)
                        .map(|decoder| {
                            decoder
                                .decode_complete::<DynInput>(
                                    &mut Cursor::new(&undecoded.0),
                                    undecoded.0.len() as u64,
                                    module_instance_id,
                                    &Default::default(),
                                )
                                .map(|input| format!("{input:?}"))
                                .unwrap_or_else(|e| format!("Decoding failed: {e}"))
                        })
                        .unwrap_or_else(|| {
                            format!("Unknown module, instance id={module_instance_id}")
                        }),
                    None => format!("{input:?}"),
                }
            })
            .collect::<Vec<_>>();

        let outputs = tx
            .data
            .outputs
            .into_iter()
            .map(|output| {
                let module_instance_id = output.module_instance_id();
                match output.as_any().downcast_ref::<DynUnknown>() {
                    Some(undecoded) => decoders
                        .get(module_instance_id)
                        .map(|decoder| {
                            decoder
                                .decode_complete::<DynOutput>(
                                    &mut Cursor::new(&undecoded.0),
                                    undecoded.0.len() as u64,
                                    module_instance_id,
                                    &Default::default(),
                                )
                                .map(|output| format!("{output:?}"))
                                .unwrap_or_else(|e| format!("Decoding failed: {e}"))
                        })
                        .unwrap_or_else(|| {
                            format!("Unknown module, instance id={module_instance_id}")
                        }),
                    None => format!("{output:?}"),
                }
            })
            .collect::<Vec<_>>();

        Ok(DebugTransaction { inputs, outputs })
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
        })
    }

    pub async fn transaction_histogram(
        &self,
        federation_id: FederationId,
    ) -> anyhow::Result<Vec<HistogramEntry>> {
        // language=postgresql
        const QUERY: &str = "
            SELECT DATE(st.estimated_session_timestamp)            AS date,
                   COUNT(DISTINCT t.txid)::bigint                  AS count,
                   COALESCE(SUM(ti.total_input_amount), 0)::bigint AS amount
            FROM transactions t
                     JOIN
                 session_times st ON t.session_index = st.session_index AND t.federation_id = st.federation_id
                     JOIN
                 (SELECT federation_id,
                         txid,
                         SUM(amount_msat) AS total_input_amount
                  FROM transaction_inputs
                  GROUP BY txid, federation_id) ti ON t.txid = ti.txid AND t.federation_id = ti.federation_id
            WHERE t.federation_id = $1
            GROUP BY date
            ORDER BY date;
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

#[derive(Debug, Clone, Serialize)]
pub struct DebugTransaction {
    inputs: Vec<String>,
    outputs: Vec<String>,
}

#[derive(Debug, Clone, FromRow)]
pub struct HistogramEntry {
    date: NaiveDate,
    count: i64,
    amount: i64,
}
