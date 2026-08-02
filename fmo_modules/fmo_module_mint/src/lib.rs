use std::collections::HashMap;

use axum::extract::{Path, State};
use axum::routing::post;
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use fedimint_core::config::FederationId;
use fedimint_core::core::{Decoder, DynInput, DynModuleConsensusItem, DynOutput, ModuleKind};
use fedimint_core::encoding::Encodable;
use fedimint_core::module::CommonModuleInit;
use fedimint_mint_common::{MintCommonInit, MintConsensusItem, MintInput, MintOutput};
use fmo_api_types::{NonceSpendInfo, NoncesRequest};
use fmo_core::api::ModuleApiState;
use fmo_core::module::{
    CiMeta, ItemMeta, Migration, ObserverModule, ProcessCtx, ProcessedItem,
};
use fmo_core::query::query;
use postgres_from_row::FromRow;
use tracing::warn;

/// Observer module for the fedimint `mint` (e-cash) module.
///
/// The mint module has no tables of its own: amounts and the JSON
/// representation of inputs/outputs/consensus items live in the core
/// structural tables, which is enough for e-cash analytics like nonce spend
/// lookups.
pub struct MintObserver;

#[async_trait::async_trait]
impl ObserverModule for MintObserver {
    fn kind(&self) -> ModuleKind {
        ModuleKind::from_static_str("mint")
    }

    fn decoder(&self) -> Decoder {
        MintCommonInit::decoder()
    }

    fn version(&self) -> u32 {
        1
    }

    fn migrations(&self) -> &'static [Migration] {
        &[]
    }

    async fn process_input(
        &self,
        _ctx: &mut ProcessCtx<'_>,
        input: &DynInput,
        _meta: &ItemMeta,
    ) -> anyhow::Result<ProcessedItem> {
        let Some(input) = input.as_any().downcast_ref::<MintInput>() else {
            warn!("could not downcast mint input (check decoders registry). {input:?}");
            return Ok(ProcessedItem::default());
        };

        let amount = input.maybe_v0_ref().map(|input_v0| input_v0.amount);
        if amount.is_none() {
            warn!("Unknown mint input version, storing JSON only: {input:?}");
        }

        Ok(ProcessedItem {
            amount,
            details: serde_json::to_value(input).ok(),
        })
    }

    async fn process_output(
        &self,
        _ctx: &mut ProcessCtx<'_>,
        output: &DynOutput,
        _meta: &ItemMeta,
    ) -> anyhow::Result<ProcessedItem> {
        let Some(output) = output.as_any().downcast_ref::<MintOutput>() else {
            warn!("could not downcast mint output (check decoders registry). {output:?}");
            return Ok(ProcessedItem::default());
        };

        let amount = output.maybe_v0_ref().map(|output_v0| output_v0.amount);
        if amount.is_none() {
            warn!("Unknown mint output version, storing JSON only: {output:?}");
        }

        Ok(ProcessedItem {
            amount,
            details: serde_json::to_value(output).ok(),
        })
    }

    async fn process_ci(
        &self,
        _ctx: &mut ProcessCtx<'_>,
        ci: &DynModuleConsensusItem,
        _meta: &CiMeta,
    ) -> anyhow::Result<Option<serde_json::Value>> {
        let Some(ci) = ci.as_any().downcast_ref::<MintConsensusItem>() else {
            warn!("could not downcast mint CI (check decoders registry). {ci:?}");
            return Ok(None);
        };

        Ok(serde_json::to_value(ci).ok())
    }

    fn api_router(&self) -> Option<Router<ModuleApiState>> {
        Some(Router::new().route("/nonces/spend", post(get_nonces_spend_info)))
    }
}

async fn get_nonces_spend_info(
    Path(federation_id): Path<FederationId>,
    State(state): State<ModuleApiState>,
    Json(request): Json<NoncesRequest>,
) -> fmo_core::error::Result<Json<HashMap<String, NonceSpendInfo>>> {
    Ok(Json(
        nonces_spend_info(&state, federation_id, &request.nonces).await?,
    ))
}

async fn nonces_spend_info(
    state: &ModuleApiState,
    federation_id: FederationId,
    nonces: &[String],
) -> anyhow::Result<HashMap<String, NonceSpendInfo>> {
    if nonces.is_empty() {
        return Ok(HashMap::new());
    }

    #[derive(Debug, FromRow)]
    struct NonceSpendRow {
        nonce: String,
        session_index: i32,
        estimated_session_timestamp: Option<chrono::NaiveDateTime>,
    }

    // Extract nonce from JSONB: {"V0": {"note": {"nonce": "..."}}}
    // language=postgresql
    let sql = "
        SELECT
            ti.details->'V0'->'note'->>'nonce' AS nonce,
            t.session_index,
            st.estimated_session_timestamp
        FROM transaction_inputs ti
        JOIN transactions t ON ti.federation_id = t.federation_id AND ti.txid = t.txid
        LEFT JOIN session_times st ON t.federation_id = st.federation_id AND t.session_index = st.session_index
        WHERE ti.federation_id = $1
          AND ti.kind = 'mint'
          AND ti.details->'V0'->'note'->>'nonce' = ANY($2)
    ";

    let rows = query::<NonceSpendRow>(
        &state.pool.get().await?,
        sql,
        &[&federation_id.consensus_encode_to_vec(), &nonces],
    )
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| {
            (
                row.nonce,
                NonceSpendInfo {
                    session_index: row.session_index as u64,
                    estimated_timestamp: row
                        .estimated_session_timestamp
                        .map(|ts| DateTime::<Utc>::from_naive_utc_and_offset(ts, Utc)),
                },
            )
        })
        .collect())
}
