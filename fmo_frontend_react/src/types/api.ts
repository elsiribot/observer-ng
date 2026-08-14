export interface FedimintTotals {
  federations: number;
  tx_volume: number;
  tx_count: number;
}

export interface FederationSummary {
  id: string;
  name: string | null;
  last_7d_activity: FederationActivity[];
  deposits: number;
  invite: string;
  nostr_votes: FederationRating;
  health: FederationHealth;
  // All-time transaction volume in millisatoshis (Amount serializes as a raw
  // number), from the `federation_tx_daily` matview. 0 when no rows yet.
  total_volume: number;
  // All-time fedimint-transaction count. 0 when no rows yet.
  total_tx_count: number;
}

export interface FederationRating {
  count: number;
  avg: number | null;
}

export interface FederationActivity {
  num_transactions: number;
  amount_transferred: number;
}

export interface FederationUtxo {
  address: string;
  out_point: string;
  amount: number;
}

export interface GuardianHealth {
  avg_uptime: number;
  avg_latency: number;
  latest: GuardianHealthLatest | null;
}

export interface GuardianHealthLatest {
  // null for federations without a v1 wallet module (walletv2-only), which
  // don't report a bitcoin block height.
  block_height: number | null;
  block_outdated: boolean | null;
  session_count: number;
  session_outdated: boolean;
}

export type FederationHealth = 'online' | 'degraded' | 'offline';

/// A half-open time interval [start, end) in unix epoch seconds.
export interface TimeInterval {
  start: number;
  end: number;
}

/// One guardian's lane in the outage timeline: its display name plus the
/// maximal runs during which it was observed offline within the window.
export interface GuardianLane {
  guardian_id: number;
  name: string;
  offline_intervals: TimeInterval[];
  /// Runs where the guardian was online but lagging (its consensus
  /// session_count trailed its peers), so it was not effectively
  /// participating. Disjoint from `offline_intervals`.
  lagging_intervals: TimeInterval[];
}

/// Guardian outage timeline for a federation over a time window. One lane per
/// guardian plus the windows where the federation was inoperable (fewer than
/// `threshold` guardians online, so consensus could not be reached). All times
/// are unix epoch seconds.
export interface GuardianTimeline {
  window_start: number;
  window_end: number;
  num_guardians: number;
  threshold: number;
  guardians: GuardianLane[];
  inoperable_intervals: TimeInterval[];
}

export interface NavItem {
  name: string;
  href: string;
  active: boolean;
}

/// Real Lightning activity metrics for a gateway over a time window,
/// computed from the modular contract tables. Counts are event counts;
/// `total_volume_msat` is the funded outgoing volume in millisatoshis.
export interface GatewayActivityMetrics {
  fund_count: number;
  settle_count: number;
  cancel_count: number;
  total_volume_msat: number;
}

/// Uptime metrics for a gateway over a time window, computed from periodic
/// poll snapshots. `uptime_pct` is 0-100.
export interface GatewayUptimeMetrics {
  sample_count: number;
  seen_samples: number;
  online_minutes: number;
  offline_minutes: number;
  uptime_pct: number;
}

/// A Lightning gateway registered with a federation, plus optional activity
/// and uptime metrics over the requested window. Optional fields mirror the
/// backend's `skip_serializing_if = Option::is_none`: absent keys deserialize
/// to `undefined`.
export interface GatewayInfo {
  /** Gateway's public key (hex-encoded). */
  gateway_id: string;
  /** LN node public key (hex-encoded). */
  node_pub_key: string;
  lightning_alias: string;
  /** URL of the gateway's public API. */
  api_endpoint: string;
  /** Whether the federation has vetted this gateway. */
  vetted: boolean;
  /** Full raw announcement, useful for forwards-compatible client usage. */
  raw?: unknown;
  /** First time this gateway was seen by the observer (RFC3339 timestamp). */
  first_seen?: string;
  /** Most recent time this gateway was seen by the observer (RFC3339). */
  last_seen?: string;
  /** Real LN activity metrics over the last 7 days. */
  activity_7d?: GatewayActivityMetrics;
  /** Real LN activity metrics over the requested API window. */
  activity_window?: GatewayActivityMetrics;
  /** Uptime metrics over the requested window. */
  uptime_window?: GatewayUptimeMetrics;
  /** The window label used for `activity_window`/`uptime_window`, e.g. "7d". */
  metrics_window?: string;
}

/// One row of the session list: precomputed counts plus the estimated
/// wall-clock time.
export interface SessionSummary {
  session_index: number;
  /** Point-estimate wall-clock time (epoch seconds); prefer the interval
   * fields below. */
  estimated_time: number | null;
  /** Lower bound of the estimated-time interval (epoch seconds). */
  time_lower: number | null;
  /** Upper bound (epoch seconds), null when unbounded. */
  time_upper: number | null;
  /** "voted" | "interpolated" | null (sessions never carry "observed"). */
  time_source: string | null;
  tx_count: number;
  items_by_kind: Record<string, unknown>;
  /** Peer ids of guardians that contributed >=1 consensus item, ascending. */
  guardians: number[];
}

/// One item (a transaction or a consensus item) within a session's ordered
/// item list, or within the federation-wide consensus stream.
export interface SessionItem {
  session_index: number;
  item_index: number;
  /** "transaction" | "ci" */
  item_type: 'transaction' | 'ci';
  kind: string | null;
  peer_id: number | null;
  txid: string | null;
  user_tx_key: string | null;
  /** The gold-layer user transaction's `kind` (e.g. "peg_in", "ln_send"),
   * or null for CI items and transactions not (yet) folded into a user
   * transaction. */
  user_tx_kind: string | null;
  /** "in" | "out" | "internal", null alongside `user_tx_kind`. */
  direction: string | null;
  details: Record<string, unknown> | null;
  /** Best point-estimate wall-clock time (unix epoch seconds): exact observed
   * time if seen live, else the midpoint of the vote-based interval (or its
   * lower bound if unbounded). Null if no time info. Prefer the interval
   * fields below. */
  estimated_time: number | null;
  /** Lower bound of the estimated-time interval (epoch seconds); equals
   * `time_upper` for an exactly-known time. */
  time_lower: number | null;
  /** Upper bound of the estimated-time interval (epoch seconds), null when
   * unbounded (session more recent than the last known vote). */
  time_upper: number | null;
  /** How the time was derived: "observed" (exact, seen live), "voted"
   * (direct vote, zero-width), or "interpolated" (forward-filled, has a
   * spread); null when no time info. */
  time_source: string | null;
  /** The tx's role in its gold user transaction: offer/fund/claim/cancel/
   * refund/self; null for CIs and unclassified txs. */
  role: string | null;
}

/// A keyset-paginated page of the federation-wide consensus item stream.
/// `next` is the `(session_index, item_index)` of the last item returned, to
/// be passed back as `before_session`/`before_item` for the next page, or
/// `null` when this was the last page.
export interface ConsensusPage {
  items: SessionItem[];
  next: [number, number] | null;
}

/// One input or output of a structured transaction detail.
export interface TxItemPart {
  index: number;
  kind: string;
  amount_msat: number | null;
  details: Record<string, unknown> | null;
}

/// Structured detail of one fedimint transaction: its inputs/outputs plus,
/// if this tx is part of a deduplicated gold-layer user transaction, that
/// user transaction's key.
export interface TxDetail {
  txid: string;
  session_index: number;
  item_index: number;
  inputs: TxItemPart[];
  outputs: TxItemPart[];
  user_tx_key: string | null;
}

/// One fedimint transaction that is a member (leg) of a gold-layer user
/// transaction, with its role in that user transaction's lifecycle.
export interface MemberTx {
  txid: string;
  /** "offer" | "fund" | "claim" | "cancel" | "refund" | "self" */
  role: string;
  session_index: number;
}

/// A deduplicated gold-layer user transaction: grain is `contract_id` for LN
/// kinds, `txid` otherwise. `member_txs` lists every underlying fedimint
/// transaction and its role.
export interface UserTransaction {
  kind: string;
  /** "in" | "out" | "internal" */
  direction: 'in' | 'out' | 'internal';
  amount_msat: number | null;
  fedimint_fee_msat: number | null;
  gateway_fee_estimate_msat: number | null;
  num_fedimint_txs: number;
  first_timestamp: number | null;
  last_timestamp: number | null;
  member_txs: MemberTx[];
}
