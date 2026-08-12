use bitcoin::address::NetworkUnchecked;
use fedimint_core::config::FederationId;
use fedimint_core::Amount;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FedimintTotals {
    pub federations: u64,
    pub tx_volume: Amount,
    pub tx_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederationSummary {
    pub id: FederationId,
    pub name: Option<String>,
    pub last_7d_activity: Vec<FederationActivity>,
    pub deposits: Amount,
    pub invite: String,
    pub nostr_votes: FederationRating,
    pub health: FederationHealth,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct FederationRating {
    pub count: u64,
    pub avg: Option<f64>,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct FederationActivity {
    pub num_transactions: u64,
    pub amount_transferred: Amount,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederationUtxo {
    pub address: bitcoin::Address<NetworkUnchecked>,
    pub out_point: bitcoin::OutPoint,
    pub amount: Amount,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GuardianHealth {
    pub avg_uptime: f32,
    pub avg_latency: f32,
    pub latest: Option<GuardianHealthLatest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GuardianHealthLatest {
    pub block_height: u32,
    pub block_outdated: bool,
    pub session_count: u32,
    pub session_outdated: bool,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FederationHealth {
    Online,
    Degraded,
    Offline,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoncesRequest {
    pub nonces: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NonceSpendInfo {
    pub session_index: u64,
    pub estimated_timestamp: Option<chrono::DateTime<chrono::Utc>>,
}

/// Subset of a gateway's registration info suitable for public API responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayInfo {
    /// Gateway's public key (hex-encoded)
    pub gateway_id: String,
    /// LN node public key (hex-encoded)
    pub node_pub_key: String,
    pub lightning_alias: String,
    /// URL of the gateway's public API
    pub api_endpoint: String,
    /// Whether the federation has vetted this gateway
    pub vetted: bool,
    /// Full raw announcement, useful for forwards-compatible client usage
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw: Option<serde_json::Value>,
    /// First time this gateway was seen by the observer
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_seen: Option<chrono::DateTime<chrono::Utc>>,
    /// Most recent time this gateway was seen by the observer
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_seen: Option<chrono::DateTime<chrono::Utc>>,
    /// Real LN activity metrics over the last 7 days
    #[serde(skip_serializing_if = "Option::is_none")]
    pub activity_7d: Option<GatewayActivityMetrics>,
    /// Real LN activity metrics over the requested API window
    #[serde(skip_serializing_if = "Option::is_none")]
    pub activity_window: Option<GatewayActivityMetrics>,
    /// Uptime metrics computed from periodic gateway snapshots over the
    /// requested window
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uptime_window: Option<GatewayUptimeMetrics>,
    /// The window label used for `activity_window` and `uptime_window`, e.g.
    /// `7d`
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metrics_window: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayActivityMetrics {
    pub fund_count: u64,
    pub settle_count: u64,
    pub cancel_count: u64,
    pub total_volume_msat: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayUptimeMetrics {
    pub sample_count: u64,
    pub seen_samples: u64,
    pub online_minutes: u64,
    pub offline_minutes: u64,
    pub uptime_pct: f64,
}

/// One row of the session list: precomputed counts from `session_stats` plus
/// the estimated wall-clock time from `session_times`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionSummary {
    pub session_index: i64,
    pub estimated_time: Option<i64>,
    pub tx_count: i64,
    pub items_by_kind: serde_json::Value,
}

/// One item (a transaction or a consensus item) within a session's ordered
/// item list. `session_index` is always populated: redundant in the
/// session-scope detail view, but needed when this same type is reused for a
/// federation-wide item stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionItem {
    pub session_index: i64,
    pub item_index: i64,
    /// "transaction" | "ci"
    pub item_type: String,
    pub kind: Option<String>,
    pub peer_id: Option<i32>,
    pub txid: Option<String>,
    pub user_tx_key: Option<String>,
    /// The gold-layer `user_transactions.kind` this tx belongs to (e.g.
    /// `"peg_in"`, `"ln_send"`), or `None` for CI items and orphan
    /// transactions not (yet) folded into a user transaction.
    pub user_tx_kind: Option<String>,
    /// "in" | "out" | "internal", mirroring `user_transactions.direction`;
    /// `None` alongside `user_tx_kind`.
    pub direction: Option<String>,
    pub details: Option<serde_json::Value>,
}

/// A keyset-paginated page of the federation-wide consensus item stream.
/// `next` is the `(session_index, item_index)` of the last item returned,
/// to be passed back as `before_session`/`before_item` for the next page, or
/// `None` when fewer than the requested `limit` items were returned (i.e.
/// this was the last page).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusPage {
    pub items: Vec<SessionItem>,
    pub next: Option<(i64, i64)>,
}

/// One input or output of a structured transaction detail, read straight from
/// `transaction_inputs`/`transaction_outputs` (not the Debug-string decode).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxItemPart {
    pub index: i32,
    pub kind: String,
    pub amount_msat: Option<i64>,
    pub details: Option<serde_json::Value>,
}

/// Structured detail of one fedimint transaction: its inputs/outputs (kind +
/// amount, read from the structural silver tables) plus, if this tx is part
/// of a deduplicated gold-layer user transaction, that user transaction's key
/// (join it via `/federations/:federation_id/user-transactions/:user_tx_key`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxDetail {
    pub txid: String,
    pub session_index: i64,
    pub item_index: i64,
    pub inputs: Vec<TxItemPart>,
    pub outputs: Vec<TxItemPart>,
    pub user_tx_key: Option<String>,
}

/// One fedimint transaction that is a member (leg) of a gold-layer user
/// transaction, with its role in that user transaction's lifecycle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberTx {
    pub txid: String,
    /// "offer" | "fund" | "claim" | "cancel" | "refund" | "self"
    pub role: String,
    pub session_index: i64,
}

/// A deduplicated gold-layer user transaction (see `fmo_core::gold`):
/// grain is `contract_id` for LN kinds, `txid` otherwise. `member_txs` lists
/// every underlying fedimint transaction and its role.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserTransaction {
    pub kind: String,
    /// "in" | "out" | "internal"
    pub direction: String,
    pub amount_msat: Option<i64>,
    pub fedimint_fee_msat: Option<i64>,
    pub gateway_fee_estimate_msat: Option<i64>,
    pub num_fedimint_txs: i64,
    pub first_timestamp: Option<i64>,
    pub last_timestamp: Option<i64>,
    pub member_txs: Vec<MemberTx>,
}
