import type {
  ConsensusPage,
  FedimintTotals,
  FederationSummary,
  SessionItem,
  SessionSummary,
  TxDetail,
  UserTransaction,
} from '../types/api';
import { auth } from './auth';

const BASE_URL = import.meta.env.VITE_FMO_API_BASE_URL || 'https://observer.fedimint.org/api';

function withAuth(opts: RequestInit, token: string | null): RequestInit {
  if (!token) {
    return opts;
  }
  return {
    ...opts,
    headers: { ...(opts.headers ?? {}), Authorization: `Bearer ${token}` },
  };
}

// Wraps fetch with bearer auth and a 401 retry, returning the raw Response so
// callers that need to inspect status/ok/headers (or read the body themselves)
// can. On 401 it clears the stale token, asks the auth manager for a fresh one
// (single-flight overlay), and retries. On a server that never 401s the loop is
// never entered, so the public unauthenticated instance is unaffected.
export async function authedFetch(path: string, opts: RequestInit = {}): Promise<Response> {
  let res = await fetch(`${BASE_URL}${path}`, withAuth(opts, auth.getToken()));
  while (res.status === 401) {
    auth.clearToken();
    const token = await auth.ensureToken();
    res = await fetch(`${BASE_URL}${path}`, withAuth(opts, token));
  }
  return res;
}

// JSON convenience over authedFetch: throws on any non-ok (non-2xx) response.
async function request<T>(path: string, opts: RequestInit = {}): Promise<T> {
  const res = await authedFetch(path, opts);
  if (!res.ok) {
    throw new Error(`Request to ${path} failed with status ${res.status}`);
  }
  return res.json() as Promise<T>;
}

// Builds a `?a=1&b=2` query string from a params object, dropping
// null/undefined entries so optional keyset-pagination args (before, limit,
// ...) are simply omitted rather than sent as "undefined".
function toQueryString(params: Record<string, string | number | undefined | null>): string {
  const entries = Object.entries(params).filter(
    (entry): entry is [string, string | number] => entry[1] !== undefined && entry[1] !== null
  );
  if (entries.length === 0) {
    return '';
  }
  const search = new URLSearchParams();
  for (const [key, value] of entries) {
    search.set(key, String(value));
  }
  return `?${search.toString()}`;
}

export interface ConsensusPageParams {
  filter?: string;
  beforeSession?: number;
  beforeItem?: number;
  limit?: number;
}

export const api = {
  getTotals: () => request<FedimintTotals>('/federations/totals'),

  getFederations: () => request<FederationSummary[]>('/federations'),

  getNostrFederations: () => request<Record<string, string>>('/nostr/federations'),

  // Keyset-paginated session list (newest first). `before` is the
  // `session_index` of the last row of the previous page.
  getSessionPage: (federationId: string, before?: number, limit?: number) =>
    request<SessionSummary[]>(
      `/federations/${federationId}/sessions${toQueryString({ before, limit })}`
    ),

  // Full ordered item list (transactions + consensus items) of one session.
  getSessionItems: (federationId: string, sessionIndex: number) =>
    request<SessionItem[]>(`/federations/${federationId}/sessions/${sessionIndex}`),

  // Federation-wide, keyset-paginated consensus item stream, newest first.
  // `filter` is "all", "transaction", or a module kind.
  getConsensusPage: (federationId: string, params: ConsensusPageParams = {}) =>
    request<ConsensusPage>(
      `/federations/${federationId}/consensus${toQueryString({
        filter: params.filter,
        before_session: params.beforeSession,
        before_item: params.beforeItem,
        limit: params.limit,
      })}`
    ),

  // Structured transaction detail: inputs/outputs (kind + amount) plus the
  // gold-layer user_tx_key, if any.
  getTxDetail: (federationId: string, txid: string) =>
    request<TxDetail>(`/federations/${federationId}/tx/${txid}`),

  // Deduplicated gold-layer user transaction plus its member fedimint txs.
  getUserTransaction: (federationId: string, userTxKey: string) =>
    request<UserTransaction>(`/federations/${federationId}/user-transactions/${userTxKey}`),
};
