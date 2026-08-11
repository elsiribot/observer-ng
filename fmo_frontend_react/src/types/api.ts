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
  block_height: number;
  block_outdated: boolean;
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
  estimated_time: number | null;
  tx_count: number;
  items_by_kind: Record<string, number>;
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
  details: Record<string, unknown> | null;
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
