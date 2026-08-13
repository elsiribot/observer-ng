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

export interface NavItem {
  name: string;
  href: string;
  active: boolean;
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
