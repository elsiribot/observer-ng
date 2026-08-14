use std::collections::HashMap;

use axum::extract::{Path, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use fedimint_core::config::FederationId;
use fedimint_core::core::{Decoder, DynInput, DynModuleConsensusItem, DynOutput, ModuleKind};
use fedimint_core::encoding::Encodable;
use fedimint_core::module::CommonModuleInit;
use fedimint_mint_common::{MintCommonInit, MintConsensusItem, MintInput, MintOutput};
use fmo_api_types::{MintDenomination, NonceSpendInfo, NoncesRequest};
use fmo_core::api::ModuleApiState;
use fmo_core::module::{CiMeta, ItemMeta, Migration, ObserverModule, ProcessCtx, ProcessedItem};
use fmo_core::query::query;
use postgres_from_row::FromRow;
use tracing::warn;

/// Observer module for the fedimint `mint` (ecash) module.
///
/// It owns one table, `fmo_mint.note_denominations`, a per-federation cumulative
/// count of notes issued/spent per denomination (maintained incrementally by
/// process_output/process_input; see `schema/v0.sql`). Everything else —
/// amounts and the JSON representation of inputs/outputs/consensus items — lives
/// in the core structural tables, which is enough for ecash analytics like
/// nonce spend lookups.
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
        // NOTE: intentionally left at 1 even though v0.sql (the note_denominations
        // table) is new. The table is seeded by a one-time backfill inside the
        // migration and maintained incrementally thereafter, so it needs no
        // schema drop + replay. A version bump would force an unnecessary
        // full mint replay across every federation.
        1
    }

    fn migrations(&self) -> &'static [Migration] {
        &[Migration {
            sql: include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/schema/v0.sql")),
        }]
    }

    async fn process_input(
        &self,
        ctx: &mut ProcessCtx<'_>,
        input: &DynInput,
        meta: &ItemMeta,
    ) -> anyhow::Result<ProcessedItem> {
        let Some(input) = input.as_any().downcast_ref::<MintInput>() else {
            warn!("could not downcast mint input (check decoders registry). {input:?}");
            return Ok(ProcessedItem::default());
        };

        let amount = input.maybe_v0_ref().map(|input_v0| input_v0.amount);
        match amount {
            // Each mint input spends exactly one note of `amount`.
            Some(amount) => count_note(ctx, meta.federation_id, amount, NoteDirection::Spent).await?,
            None => warn!("Unknown mint input version, storing JSON only: {input:?}"),
        }

        Ok(ProcessedItem {
            amount,
            details: serde_json::to_value(input).ok(),
        })
    }

    async fn process_output(
        &self,
        ctx: &mut ProcessCtx<'_>,
        output: &DynOutput,
        meta: &ItemMeta,
    ) -> anyhow::Result<ProcessedItem> {
        let Some(output) = output.as_any().downcast_ref::<MintOutput>() else {
            warn!("could not downcast mint output (check decoders registry). {output:?}");
            return Ok(ProcessedItem::default());
        };

        let amount = output.maybe_v0_ref().map(|output_v0| output_v0.amount);
        match amount {
            // Each mint output mints exactly one note of `amount`.
            Some(amount) => {
                count_note(ctx, meta.federation_id, amount, NoteDirection::Issued).await?
            }
            None => warn!("Unknown mint output version, storing JSON only: {output:?}"),
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
        Some(
            Router::new()
                .route("/nonces/spend", post(get_nonces_spend_info))
                .route("/denominations", get(get_denominations)),
        )
    }
}

/// Which counter a processed note increments.
#[derive(Clone, Copy)]
enum NoteDirection {
    Issued,
    Spent,
}

/// Increment the per-denomination note counter for one processed note. Runs on
/// `ctx.dbtx`, so it commits atomically with the module cursor (exactly-once
/// per note; see `dispatch::process_module_batch`). The `search_path` is already
/// set to the `fmo_mint` schema for the duration of the batch transaction.
async fn count_note(
    ctx: &mut ProcessCtx<'_>,
    federation_id: FederationId,
    amount: fedimint_core::Amount,
    direction: NoteDirection,
) -> anyhow::Result<()> {
    // Two fixed statements rather than string interpolation of the column name.
    let sql = match direction {
        NoteDirection::Issued => {
            "INSERT INTO note_denominations (federation_id, denomination_msat, issued, spent)
             VALUES ($1, $2, 1, 0)
             ON CONFLICT (federation_id, denomination_msat)
             DO UPDATE SET issued = note_denominations.issued + 1"
        }
        NoteDirection::Spent => {
            "INSERT INTO note_denominations (federation_id, denomination_msat, issued, spent)
             VALUES ($1, $2, 0, 1)
             ON CONFLICT (federation_id, denomination_msat)
             DO UPDATE SET spent = note_denominations.spent + 1"
        }
    };
    ctx.dbtx
        .execute(
            sql,
            &[
                &federation_id.consensus_encode_to_vec(),
                &(amount.msats as i64),
            ],
        )
        .await?;
    Ok(())
}

async fn get_denominations(
    Path(federation_id): Path<FederationId>,
    State(state): State<ModuleApiState>,
) -> fmo_core::error::Result<Json<Vec<MintDenomination>>> {
    Ok(Json(denominations(&state, federation_id).await?))
}

async fn denominations(
    state: &ModuleApiState,
    federation_id: FederationId,
) -> anyhow::Result<Vec<MintDenomination>> {
    #[derive(Debug, FromRow)]
    struct DenominationRow {
        denomination_msat: i64,
        issued: i64,
        in_circulation: i64,
    }

    // Pad to the GLOBAL denomination set -- every denomination used by any
    // observed federation -- zero-filling the ones this federation never used,
    // so denomination histograms line up across federations and are directly
    // comparable. The `EXISTS` guard keeps a federation with no mint notes of
    // its own returning an empty list (frontend shows an empty state) rather
    // than a chart of all-zero bars.
    // language=postgresql
    let sql = "
        SELECT d.denomination_msat,
               COALESCE(n.issued, 0) AS issued,
               GREATEST(COALESCE(n.issued, 0) - COALESCE(n.spent, 0), 0) AS in_circulation
        FROM (SELECT DISTINCT denomination_msat FROM fmo_mint.note_denominations) d
        LEFT JOIN fmo_mint.note_denominations n
               ON n.denomination_msat = d.denomination_msat
              AND n.federation_id = $1
        WHERE EXISTS (SELECT 1 FROM fmo_mint.note_denominations f
                      WHERE f.federation_id = $1)
        ORDER BY d.denomination_msat
    ";

    let rows = query::<DenominationRow>(
        &state.pool.get().await?,
        sql,
        &[&federation_id.consensus_encode_to_vec()],
    )
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| MintDenomination {
            denomination_msat: row.denomination_msat as u64,
            issued: row.issued as u64,
            in_circulation: row.in_circulation as u64,
        })
        .collect())
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
