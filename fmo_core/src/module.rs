use std::sync::Arc;

use deadpool_postgres::Transaction;
use fedimint_core::config::{ClientConfig, FederationId};
use fedimint_core::core::{Decoder, DynInput, DynModuleConsensusItem, DynOutput, ModuleKind};
use fedimint_core::encoding::Encodable;
use fedimint_core::{Amount, PeerId, TransactionId};

pub use crate::db::migrations::Migration;
use crate::services::CoreServices;

/// Metadata passed to [`ObserverModule::process_input`] and
/// [`ObserverModule::process_output`].
#[derive(Debug, Clone)]
pub struct ItemMeta {
    pub federation_id: FederationId,
    pub txid: TransactionId,
    pub session_index: u64,
    pub item_index: u64,
    /// Input or output index within the transaction
    pub index: u64,
    pub peer_count: usize,
}

/// Metadata passed to [`ObserverModule::process_ci`].
#[derive(Debug, Clone)]
pub struct CiMeta {
    pub federation_id: FederationId,
    pub session_index: u64,
    pub item_index: u64,
    pub peer: PeerId,
    pub peer_count: usize,
}

/// Result of processing a transaction input or output. Core writes `amount`
/// and `details` back into the global
/// `transaction_inputs`/`transaction_outputs` tables; modules never write core
/// tables directly.
#[derive(Debug, Default)]
pub struct ProcessedItem {
    pub amount: Option<Amount>,
    pub details: Option<serde_json::Value>,
}

/// Context handed to a module while processing a session. `dbtx` has
/// `search_path` set to the module's own schema (`fmo_<kind>`) followed by
/// `public`, so unqualified table names refer to module-owned tables.
pub struct ProcessCtx<'a> {
    pub dbtx: &'a Transaction<'a>,
    pub federation_id: FederationId,
    pub config: ClientConfig,
    pub services: Arc<CoreServices>,
}

impl ProcessCtx<'_> {
    /// Contribute a session timestamp estimate. Core aggregates these votes
    /// into the `session_times` materialized view.
    pub async fn record_session_time_vote(
        &self,
        kind: &ModuleKind,
        session_index: u64,
        peer: PeerId,
        timestamp: chrono::NaiveDateTime,
    ) -> anyhow::Result<()> {
        self.dbtx
            .execute(
                "INSERT INTO public.session_time_votes VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT DO NOTHING",
                &[
                    &self.federation_id.consensus_encode_to_vec(),
                    &(session_index as i32),
                    &kind.as_str(),
                    &(peer.to_usize() as i32),
                    &timestamp,
                ],
            )
            .await?;
        Ok(())
    }
}

/// Context for module-owned per-federation background tasks.
#[derive(Clone)]
pub struct ModuleTaskCtx {
    pub federation_id: FederationId,
    pub config: ClientConfig,
    pub pool: deadpool_postgres::Pool,
    pub services: Arc<CoreServices>,
    pub connectors: fedimint_connectors::ConnectorRegistry,
}

/// An observer module understands the consensus items of one fedimint module
/// kind, normalizes them into its own Postgres schema and can expose extra
/// API routes and background tasks.
#[async_trait::async_trait]
pub trait ObserverModule: Send + Sync + 'static {
    fn kind(&self) -> ModuleKind;

    fn decoder(&self) -> Decoder;

    /// Bump to force a replay: the module's schema is dropped, its cursors
    /// reset and all sessions are re-processed from raw session data.
    fn version(&self) -> u32;

    /// SQL migrations run inside the module's own schema
    /// (`search_path = fmo_<kind>, public`).
    fn migrations(&self) -> &'static [Migration];

    /// Materialized views owned by this module (schema-qualified names) that
    /// core should refresh periodically.
    fn matviews(&self) -> &'static [&'static str] {
        &[]
    }

    async fn process_input(
        &self,
        ctx: &mut ProcessCtx<'_>,
        input: &DynInput,
        meta: &ItemMeta,
    ) -> anyhow::Result<ProcessedItem>;

    async fn process_output(
        &self,
        ctx: &mut ProcessCtx<'_>,
        output: &DynOutput,
        meta: &ItemMeta,
    ) -> anyhow::Result<ProcessedItem>;

    async fn process_ci(
        &self,
        ctx: &mut ProcessCtx<'_>,
        ci: &DynModuleConsensusItem,
        meta: &CiMeta,
    ) -> anyhow::Result<Option<serde_json::Value>>;

    /// Spawned once per (module, federation); loop internally if the task is
    /// recurring. Default: no background task.
    async fn run_federation_task(self: Arc<Self>, _ctx: ModuleTaskCtx) {}

    /// Extra API routes, mounted at
    /// `/federations/:federation_id/modules/<kind>`.
    fn api_router(&self) -> Option<axum::Router<crate::api::ModuleApiState>> {
        None
    }
}
