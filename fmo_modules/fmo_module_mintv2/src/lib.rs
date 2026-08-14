use axum::extract::{Path, State};
use axum::routing::get;
use axum::{Json, Router};
use fedimint_core::config::FederationId;
use fedimint_core::core::{Decoder, DynInput, DynModuleConsensusItem, DynOutput, ModuleKind};
use fedimint_core::encoding::Encodable;
use fedimint_core::module::CommonModuleInit;
use fedimint_mintv2_common::{MintCommonInit, MintConsensusItem, MintInput, MintOutput};
use fmo_api_types::MintDenomination;
use fmo_core::api::ModuleApiState;
use fmo_core::module::{CiMeta, ItemMeta, Migration, ObserverModule, ProcessCtx, ProcessedItem};
use fmo_core::query::query;
use postgres_from_row::FromRow;
use tracing::warn;

/// Observer module for the next-generation fedimint `mintv2` (e-cash) module:
/// records amounts of issued and spent notes and tracks spent note nonces for
/// e-cash analytics such as spend lookups.
pub struct MintV2Observer;

const KIND: ModuleKind = ModuleKind::from_static_str("mintv2");

#[async_trait::async_trait]
impl ObserverModule for MintV2Observer {
    fn kind(&self) -> ModuleKind {
        KIND
    }

    fn decoder(&self) -> Decoder {
        MintCommonInit::decoder()
    }

    fn version(&self) -> u32 {
        // NOTE: intentionally left at 1 even though v1.sql (the
        // note_denominations table) is new. The table is seeded by a one-time
        // backfill inside the migration and maintained incrementally
        // thereafter, so it needs no schema drop + replay: the per-migration
        // `schema_version` cursor applies v1.sql on top of the existing schema
        // (see `setup_module_schema`). A version bump would force an
        // unnecessary full mintv2 replay across every federation.
        1
    }

    fn migrations(&self) -> &'static [Migration] {
        &[
            Migration {
                sql: include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/schema/v0.sql")),
            },
            Migration {
                sql: include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/schema/v1.sql")),
            },
        ]
    }

    async fn process_input(
        &self,
        ctx: &mut ProcessCtx<'_>,
        input: &DynInput,
        meta: &ItemMeta,
    ) -> anyhow::Result<ProcessedItem> {
        let Some(mint_input) = input.as_any().downcast_ref::<MintInput>() else {
            warn!("could not downcast mintv2 input (check decoders registry). {input:?}");
            return Ok(ProcessedItem::default());
        };

        let Some(input_v0) = mint_input.maybe_v0_ref() else {
            warn!("Unknown mintv2 input version, storing JSON only: {mint_input:?}");
            return Ok(ProcessedItem {
                amount: None,
                details: serde_json::to_value(mint_input).ok(),
            });
        };

        let note = &input_v0.note;
        let amount = note.amount();

        ctx.dbtx
            .execute(
                "INSERT INTO spent_nonces VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING",
                &[
                    &meta.federation_id.consensus_encode_to_vec(),
                    &note.nonce.to_string(),
                    &(note.denomination.0 as i16),
                    &(amount.msats as i64),
                    &meta.txid.consensus_encode_to_vec(),
                    &(meta.index as i32),
                ],
            )
            .await?;

        // Each mint input spends exactly one note of `amount`.
        count_note(ctx, meta.federation_id, amount, NoteDirection::Spent).await?;

        Ok(ProcessedItem {
            amount: Some(amount),
            details: serde_json::to_value(mint_input).ok(),
        })
    }

    async fn process_output(
        &self,
        ctx: &mut ProcessCtx<'_>,
        output: &DynOutput,
        meta: &ItemMeta,
    ) -> anyhow::Result<ProcessedItem> {
        let Some(mint_output) = output.as_any().downcast_ref::<MintOutput>() else {
            warn!("could not downcast mintv2 output (check decoders registry). {output:?}");
            return Ok(ProcessedItem::default());
        };

        let amount = mint_output
            .maybe_v0_ref()
            .map(|output_v0| output_v0.amount());
        match amount {
            // Each mint output mints exactly one note of `amount`.
            Some(amount) => {
                count_note(ctx, meta.federation_id, amount, NoteDirection::Issued).await?
            }
            None => warn!("Unknown mintv2 output version, storing JSON only: {mint_output:?}"),
        }

        Ok(ProcessedItem {
            amount,
            details: serde_json::to_value(mint_output).ok(),
        })
    }

    async fn process_ci(
        &self,
        _ctx: &mut ProcessCtx<'_>,
        ci: &DynModuleConsensusItem,
        _meta: &CiMeta,
    ) -> anyhow::Result<Option<serde_json::Value>> {
        let Some(mint_ci) = ci.as_any().downcast_ref::<MintConsensusItem>() else {
            warn!("could not downcast mintv2 CI (check decoders registry). {ci:?}");
            return Ok(None);
        };

        // mintv2 defines no consensus items today; any observed item is an
        // unknown future variant and only stored as JSON.
        Ok(serde_json::to_value(mint_ci).ok())
    }

    fn api_router(&self) -> Option<Router<ModuleApiState>> {
        Some(Router::new().route("/denominations", get(get_denominations)))
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
/// set to the `fmo_mintv2` schema for the duration of the batch transaction.
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
        FROM (SELECT DISTINCT denomination_msat FROM fmo_mintv2.note_denominations) d
        LEFT JOIN fmo_mintv2.note_denominations n
               ON n.denomination_msat = d.denomination_msat
              AND n.federation_id = $1
        WHERE EXISTS (SELECT 1 FROM fmo_mintv2.note_denominations f
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
