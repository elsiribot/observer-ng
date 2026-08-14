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
    /// All-time transaction volume (summed input amounts), from the
    /// `federation_tx_daily` matview. `Amount::ZERO` for federations with no
    /// rows yet.
    pub total_volume: Amount,
    /// All-time fedimint-transaction count, from the `federation_tx_daily`
    /// matview. `0` for federations with no rows yet.
    pub total_tx_count: u64,
    /// Threshold-aware federation uptime over the last 30 days: the percentage
    /// of health polls at which at least `threshold` guardians were
    /// participating (online AND caught up). `None` when there are no health
    /// samples yet (see [`FederationUptime`] for the full breakdown served by
    /// the detail endpoint).
    pub uptime_pct: Option<f64>,
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
    /// Bitcoin block height as reported by the guardian's `block_count_local`
    /// endpoint. `None` for federations without a v1 wallet module (e.g.
    /// walletv2-only ones), which don't expose a block height.
    pub block_height: Option<u32>,
    /// Whether the guardian's block height lags our own by more than the
    /// tolerance. `None` when no block height is available (see above).
    pub block_outdated: Option<bool>,
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

/// A half-open time interval `[start, end)` in unix epoch seconds. Used for
/// both per-guardian offline runs and federation-wide inoperable runs in the
/// guardian outage timeline.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct TimeInterval {
    /// Interval start, unix epoch seconds (inclusive).
    pub start: i64,
    /// Interval end, unix epoch seconds (exclusive). For an outage still
    /// ongoing at `window_end` this equals `window_end`.
    pub end: i64,
}

/// One guardian's lane in the outage timeline: its display name plus the
/// maximal runs during which it was observed offline within the window.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GuardianLane {
    pub guardian_id: u16,
    pub name: String,
    /// Maximal runs where the guardian was offline (its `guardian_health`
    /// samples had a NULL `status`), ordered by `start`. Empty when the
    /// guardian was online for the whole window (or had no samples).
    pub offline_intervals: Vec<TimeInterval>,
    /// Maximal runs where the guardian was online but *lagging*: its reported
    /// consensus `session_count` trailed the highest among its peers by more
    /// than one, so it was not effectively participating in consensus. Ordered
    /// by `start`. Disjoint from `offline_intervals` (an offline guardian
    /// reports no session count). Lagging time counts against the federation's
    /// participating-guardian total for the inoperable threshold.
    pub lagging_intervals: Vec<TimeInterval>,
}

/// Guardian outage timeline for a federation over a time window: one lane per
/// guardian plus the windows during which the federation was inoperable
/// (fewer than `threshold` guardians online, so consensus could not be
/// reached).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GuardianTimeline {
    /// Window start, unix epoch seconds.
    pub window_start: i64,
    /// Window end, unix epoch seconds (the query time / "now").
    pub window_end: i64,
    /// Total number of guardians in the federation (from its config).
    pub num_guardians: usize,
    /// Consensus threshold: `NumPeers::from(num_guardians).threshold()`. The
    /// federation is inoperable when fewer than this many guardians are
    /// *participating* (online AND not lagging).
    pub threshold: usize,
    pub guardians: Vec<GuardianLane>,
    /// Maximal runs where the participating guardian count (online AND caught
    /// up) dropped below `threshold`, i.e. the federation could not reach
    /// consensus, ordered by `start`. A deeply lagging guardian counts as
    /// non-participating here just like an offline one.
    pub inoperable_intervals: Vec<TimeInterval>,
}

/// Threshold-aware federation uptime over a window: the fraction of health
/// polls at which the federation was *operable* — at least `threshold`
/// guardians participating (online AND caught up to the consensus tip), the
/// same rule as the timeline's inoperable bands. Sample-based (a poll with no
/// data doesn't count), so it mirrors the per-guardian uptime figure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederationUptime {
    /// Window start, unix epoch seconds.
    pub window_start: i64,
    /// Window end, unix epoch seconds (the query time / "now").
    pub window_end: i64,
    /// Total number of guardians in the federation (from its config).
    pub num_guardians: usize,
    /// Consensus threshold; the federation is operable when at least this many
    /// guardians participate.
    pub threshold: usize,
    /// Number of polls observed in the window.
    pub total_polls: i64,
    /// Number of those polls at which the federation was operable.
    pub operable_polls: i64,
    /// Operable fraction as a percentage, `None` when no polls were observed.
    pub uptime_pct: Option<f64>,
}

/// A guardian identified by peer id + display name, used to label the series in
/// [`GuardianLatencySeries`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GuardianRef {
    pub guardian_id: u16,
    pub name: String,
}

/// One time bucket of the guardian API-latency series.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyBucket {
    /// Bucket start, unix epoch seconds.
    pub time: i64,
    /// Average API latency (ms) in this bucket, one entry per guardian in the
    /// same order as [`GuardianLatencySeries::guardians`]. `None` where the
    /// guardian produced no successful response in the bucket (so its line has
    /// a gap rather than a fabricated point).
    pub latencies: Vec<Option<f64>>,
    /// The federation's effective consensus latency in this bucket: the average
    /// over the bucket's polls of the slowest latency among the `threshold`
    /// fastest responding guardians at each poll (e.g. the 5th-fastest of 7 in
    /// a 5/7). `None` when no poll in the bucket had `threshold` responders.
    pub quorum_ms: Option<f64>,
}

/// Guardian API-latency time series for a federation over a window: one line
/// per guardian plus the derived quorum-latency line (see
/// [`LatencyBucket::quorum_ms`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GuardianLatencySeries {
    /// Window start, unix epoch seconds.
    pub window_start: i64,
    /// Window end, unix epoch seconds (the query time / "now").
    pub window_end: i64,
    /// Number of guardians in the federation (from its config).
    pub num_guardians: usize,
    /// Consensus threshold: how many of the fastest guardians the quorum line
    /// tracks (the k in "slowest of the k fastest").
    pub threshold: usize,
    /// Bucket width in seconds (chosen from the window for ~a few hundred
    /// points).
    pub bucket_seconds: i64,
    /// The guardians, in peer-id order; indexes align with
    /// [`LatencyBucket::latencies`].
    pub guardians: Vec<GuardianRef>,
    /// Time buckets, ascending by `time`.
    pub buckets: Vec<LatencyBucket>,
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
    /// Point-estimate wall-clock time (epoch seconds): the midpoint of the
    /// vote-based interval, or its lower bound if unbounded. Kept for
    /// back-compat; prefer `time_lower`/`time_upper`/`time_source`.
    pub estimated_time: Option<i64>,
    /// Lower bound of the estimated-time interval (epoch seconds).
    pub time_lower: Option<i64>,
    /// Upper bound of the estimated-time interval (epoch seconds), `None` when
    /// unbounded (session more recent than the last known vote).
    pub time_upper: Option<i64>,
    /// How the time was derived: `"voted"` (zero-width) or `"interpolated"`
    /// (has a spread); `None` when unavailable. Sessions never carry the
    /// per-item `"observed"` source.
    pub time_source: Option<String>,
    pub tx_count: i64,
    pub items_by_kind: serde_json::Value,
    /// Peer ids of the guardians that contributed at least one consensus item
    /// to this session, ascending. Derived from `consensus_items.peer_id`; a
    /// guardian missing here proposed no CI in the session (transactions are
    /// not attributed to a proposing peer in storage, so they don't count).
    pub guardians: Vec<u16>,
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
    /// The item's best point-estimate wall-clock time (epoch seconds): the
    /// exact observed time if the item was seen live, else the midpoint of
    /// the vote-based uncertainty interval (or its lower bound if unbounded).
    /// `None` if the session has no time information yet. Kept for
    /// back-compat; prefer `time_lower`/`time_upper`/`time_source`.
    pub estimated_time: Option<i64>,
    /// Lower bound of the estimated-time interval (epoch seconds). Equals
    /// `time_upper` for an exactly-known (observed or directly-voted) time.
    pub time_lower: Option<i64>,
    /// Upper bound of the estimated-time interval (epoch seconds), or `None`
    /// when unbounded (a session more recent than the last known vote).
    pub time_upper: Option<i64>,
    /// How the time was derived: `"observed"` (exact, seen live),
    /// `"voted"` (a direct time vote for this session, zero-width interval),
    /// or `"interpolated"` (forward-filled between votes, has a spread).
    /// `None` when no time information is available.
    pub time_source: Option<String>,
    /// The tx's role in its gold user transaction: offer/fund/claim/cancel/
    /// refund/self; `None` for CIs and unclassified txs.
    pub role: Option<String>,
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
