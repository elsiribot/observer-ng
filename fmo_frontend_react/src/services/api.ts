import type { FedimintTotals, FederationSummary } from '../types/api';
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

// Wraps fetch with bearer auth. On 401 it clears the (stale/wrong) token,
// asks the auth manager for a fresh one (single-flight overlay), and retries.
// On a server that never returns 401 the loop is never entered, so the public
// unauthenticated instance behaves exactly as before.
async function request<T>(path: string, opts: RequestInit = {}): Promise<T> {
  let res = await fetch(`${BASE_URL}${path}`, withAuth(opts, auth.getToken()));
  while (res.status === 401) {
    auth.clearToken();
    const token = await auth.ensureToken();
    res = await fetch(`${BASE_URL}${path}`, withAuth(opts, token));
  }
  if (!res.ok) {
    throw new Error(`Request to ${path} failed with status ${res.status}`);
  }
  return res.json() as Promise<T>;
}

export const api = {
  getTotals: () => request<FedimintTotals>('/federations/totals'),

  getFederations: () => request<FederationSummary[]>('/federations'),

  getNostrFederations: () => request<Record<string, string>>('/nostr/federations'),

  async getFederation(id: string): Promise<FederationSummary> {
    // The backend has no full per-federation summary endpoint yet, so we fetch
    // the list and find it (unchanged behavior, now authenticated).
    const allFederations = await request<FederationSummary[]>('/federations');
    const federation = allFederations.find(f => f.id === id);
    if (!federation) {
      throw new Error(`Federation ${id} not found`);
    }
    return federation;
  },

  getFederationConfig: (id: string) =>
    request<Record<string, unknown>>(`/federations/${id}/config`),

  getFederationUtxos: (id: string) =>
    request<unknown[]>(`/federations/${id}/utxos`),

  getFederationHistogram: (id: string) =>
    request<Record<string, { num_transactions: number; amount_transferred: number }>>(
      `/federations/${id}/transactions/histogram`,
    ),

  getFederationHealth: (id: string) =>
    request<Record<string, unknown>>(`/federations/${id}/health`),
};
