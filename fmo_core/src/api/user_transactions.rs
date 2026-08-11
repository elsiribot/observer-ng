use anyhow::Context;
use axum::extract::{Path, State};
use axum::Json;
use fedimint_core::config::FederationId;
use fedimint_core::encoding::Encodable;
use fmo_api_types::{MemberTx, UserTransaction};
use postgres_from_row::FromRow;

use crate::api::AppState;
use crate::observer::FederationObserver;
use crate::query::{query, query_opt};

/// Gold-layer user transaction: the deduplicated summary row plus every
/// underlying fedimint transaction (leg) and its role.
pub(super) async fn user_transaction_detail(
    Path((federation_id, user_tx_key_hex)): Path<(FederationId, String)>,
    State(state): State<AppState>,
) -> crate::error::Result<Json<UserTransaction>> {
    let user_tx_key = hex::decode(&user_tx_key_hex).context("Invalid user_tx_key hex")?;
    Ok(state
        .observer
        .federation_user_transaction(federation_id, &user_tx_key)
        .await?
        .into())
}

impl FederationObserver {
    /// Reads the `user_transactions` row by `(federation_id, user_tx_key)`
    /// plus its `user_transaction_txs` member legs, ordered by
    /// `session_index, role`.
    pub async fn federation_user_transaction(
        &self,
        federation_id: FederationId,
        user_tx_key: &[u8],
    ) -> anyhow::Result<UserTransaction> {
        self.get_federation(federation_id)
            .await
            .context("Federation doesn't exist")?;

        let fed = federation_id.consensus_encode_to_vec();

        #[derive(FromRow)]
        struct UserTxRow {
            kind: String,
            direction: String,
            amount_msat: Option<i64>,
            fedimint_fee_msat: Option<i64>,
            gateway_fee_estimate_msat: Option<i64>,
            num_fedimint_txs: i64,
            first_timestamp: Option<i64>,
            last_timestamp: Option<i64>,
        }

        let row = query_opt::<UserTxRow>(
            &self.connection().await?,
            "SELECT kind, direction, amount_msat, fedimint_fee_msat, gateway_fee_estimate_msat,
                    num_fedimint_txs::bigint,
                    EXTRACT(EPOCH FROM first_timestamp)::bigint AS first_timestamp,
                    EXTRACT(EPOCH FROM last_timestamp)::bigint AS last_timestamp
             FROM user_transactions WHERE federation_id = $1 AND user_tx_key = $2",
            &[&fed, &user_tx_key],
        )
        .await?
        .context("User transaction not found")?;

        #[derive(FromRow)]
        struct MemberTxRow {
            txid: String,
            role: String,
            session_index: i64,
        }

        let member_txs = query::<MemberTxRow>(
            &self.connection().await?,
            "SELECT encode(txid, 'hex') AS txid, role, session_index::bigint FROM user_transaction_txs
             WHERE federation_id = $1 AND user_tx_key = $2
             ORDER BY session_index, role",
            &[&fed, &user_tx_key],
        )
        .await?;

        Ok(UserTransaction {
            kind: row.kind,
            direction: row.direction,
            amount_msat: row.amount_msat,
            fedimint_fee_msat: row.fedimint_fee_msat,
            gateway_fee_estimate_msat: row.gateway_fee_estimate_msat,
            num_fedimint_txs: row.num_fedimint_txs,
            first_timestamp: row.first_timestamp,
            last_timestamp: row.last_timestamp,
            member_txs: member_txs
                .into_iter()
                .map(|row| MemberTx {
                    txid: row.txid,
                    role: row.role,
                    session_index: row.session_index,
                })
                .collect(),
        })
    }
}
