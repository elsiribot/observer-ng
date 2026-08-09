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

export const api = {
  getTotals: () => request<FedimintTotals>('/federations/totals'),

  getFederations: () => request<FederationSummary[]>('/federations'),

  getNostrFederations: () => request<Record<string, string>>('/nostr/federations'),
};
