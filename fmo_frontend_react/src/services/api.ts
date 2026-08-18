import type {
  ConsensusPage,
  EcashAnonScatter,
  FedimintTotals,
  FederationSummary,
  FederationUptime,
  GatewayInfo,
  GuardianLatencySeries,
  GuardianTimeline,
  MintDenomination,
  SessionItem,
  SessionSummary,
  SpAccount,
  SpAccountsPage,
  SpAccountTxPage,
  SpSeriesPoint,
  SpSummary,
  SpTransferEdge,
  SpTxAccount,
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

// Which Lightning module a gateway was registered with. LNv1 entries are rich
// (node key, alias, vetting, activity/uptime); LNv2 entries are thin (just the
// gateway's API URL as `gateway_id`/`api_endpoint`, plus first/last seen).
export type GatewayModule = 'LNv1' | 'LNv2';

// Gateways from both Lightning modules, kept separate so the UI can tag them
// and so an empty state only shows when *both* are empty. A federation may run
// only one of the modules, so either list may be empty.
export interface FederationGateways {
  v1: GatewayInfo[];
  v2: GatewayInfo[];
}

export const api = {
  getTotals: () => request<FedimintTotals>('/federations/totals'),

  getFederations: () => request<FederationSummary[]>('/federations'),

  getFederationSummary: (id: string) => request<FederationSummary>(`/federations/${id}/summary`),

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

  // Lightning gateways registered with the federation, fetched from BOTH
  // Lightning modules: LNv1 (`/modules/ln/gateways`, rich metrics over the
  // given window: 1h | 24h | 7d | 30d | 90d, backend default 7d) and LNv2
  // (`/modules/lnv2/gateways`, thin — URL + first/last seen, ignores `window`).
  // Each endpoint is fetched independently and a failing/absent module (e.g. a
  // 404 when the federation doesn't run that module) is treated as an empty
  // list rather than failing the whole tab.
  async getGateways(federationId: string, window: string = '7d'): Promise<FederationGateways> {
    const qs = toQueryString({ window });
    const [v1, v2] = await Promise.all([
      request<GatewayInfo[]>(`/federations/${federationId}/modules/ln/gateways${qs}`).catch(
        () => [] as GatewayInfo[]
      ),
      request<GatewayInfo[]>(`/federations/${federationId}/modules/lnv2/gateways${qs}`).catch(
        () => [] as GatewayInfo[]
      ),
    ]);
    return { v1, v2 };
  },

  // Guardian outage timeline over the given window (7d | 30d; backend default
  // 30d): per-guardian offline + lagging intervals plus federation-wide
  // inoperable (quorum-lost) intervals. `despike` (default true) filters
  // transient single-poll false positives; pass false to see raw samples.
  getGuardianTimeline: (federationId: string, window: string = '30d', despike: boolean = true) =>
    request<GuardianTimeline>(
      `/federations/${federationId}/health/timeline${toQueryString({
        window,
        despike: despike ? undefined : 'false',
      })}`
    ),

  // Threshold-aware federation uptime over the given window (default 30d): the
  // fraction of health polls at which the federation was operable (>= threshold
  // participating guardians).
  getFederationUptime: (federationId: string, window: string = '30d') =>
    request<FederationUptime>(
      `/federations/${federationId}/health/uptime${toQueryString({ window })}`
    ),

  // Guardian API-latency time series over the given window (7d | 30d; backend
  // default 30d): one bucketed line per guardian plus the quorum line (slowest
  // latency of the fastest `threshold` guardians per poll).
  getGuardianLatency: (federationId: string, window: string = '30d') =>
    request<GuardianLatencySeries>(
      `/federations/${federationId}/health/latency${toQueryString({ window })}`
    ),

  // Ecash note denominations for the federation from the mint module: per
  // power-of-two denomination, the number of notes ever issued and currently in
  // circulation. Empty for federations without a mint module or with no notes.
  getMintDenominations: (federationId: string) =>
    request<MintDenomination[]>(`/federations/${federationId}/modules/mint/denominations`),

  // Same as `getMintDenominations` but for the next-generation `mintv2` module.
  // A federation may run either mint module (or, transitionally, both), so the
  // Ecash tab fetches both and renders whichever return data.
  getMintV2Denominations: (federationId: string) =>
    request<MintDenomination[]>(`/federations/${federationId}/modules/mintv2/denominations`),

  // Ecash-spend anonymity scatter data (Ecash tab): a random sample of
  // per-transaction anon-bits points plus rolling-7d percentile lines.
  getEcashAnonScatter: (federationId: string) =>
    request<EcashAnonScatter>(`/federations/${federationId}/ecash/anon-scatter`),

  // --- Stability pool gold layer (Stability Pool tab + account pages) ------
  // All under /modules/multi_sig_stability_pool/*. Fiat values are the
  // federation's stable-currency base unit (cents for USD).

  getSpSummary: (federationId: string) =>
    request<SpSummary>(`/federations/${federationId}/modules/multi_sig_stability_pool/summary`),

  // Accounts, offset-paginated. `order` is "net" | "activity" | "recent".
  getSpAccounts: (
    federationId: string,
    params: { order?: string; limit?: number; offset?: number } = {}
  ) =>
    request<SpAccountsPage>(
      `/federations/${federationId}/modules/multi_sig_stability_pool/accounts${toQueryString({
        order: params.order,
        limit: params.limit,
        offset: params.offset,
      })}`
    ),

  getSpAccount: (federationId: string, accountId: string) =>
    request<SpAccount>(
      `/federations/${federationId}/modules/multi_sig_stability_pool/account/${accountId}`
    ),

  // Folded fiat history for one account, keyset-paginated by (session, tx_key).
  getSpAccountTransactions: (
    federationId: string,
    accountId: string,
    params: { beforeSession?: number; beforeTxKey?: string; limit?: number } = {}
  ) =>
    request<SpAccountTxPage>(
      `/federations/${federationId}/modules/multi_sig_stability_pool/account/${accountId}/transactions${toQueryString(
        {
          before_session: params.beforeSession,
          before_tx_key: params.beforeTxKey,
          limit: params.limit,
        }
      )}`
    ),

  getSpAccountTransfers: (federationId: string, accountId: string) =>
    request<SpTransferEdge[]>(
      `/federations/${federationId}/modules/multi_sig_stability_pool/account/${accountId}/transfers`
    ),

  // Per-cycle price + cumulative net-flow series (ascending) for the charts.
  getSpSeries: (federationId: string) =>
    request<SpSeriesPoint[]>(
      `/federations/${federationId}/modules/multi_sig_stability_pool/series`
    ),

  // Which account each stability-pool input/output of a fedimint tx touches,
  // to cross-link the transaction-detail rows to account pages.
  getSpTxAccounts: (federationId: string, txid: string) =>
    request<SpTxAccount[]>(
      `/federations/${federationId}/modules/multi_sig_stability_pool/tx/${txid}/accounts`
    ),
};

// Parses one SSE frame (everything between a pair of blank-line-delimited
// `\n\n` boundaries) into its event name (default "message" per the SSE
// spec) and data payload. Multiple `data:` lines are concatenated with `\n`,
// also per spec. Exported standalone so tests can exercise frame parsing
// without a real streamed response.
export function parseSseFrame(frame: string): { event: string; data: string } {
  let event = 'message';
  const dataLines: string[] = [];
  for (const rawLine of frame.split('\n')) {
    const line = rawLine.replace(/\r$/, '');
    if (line.startsWith('event:')) {
      event = line.slice('event:'.length).trim();
    } else if (line.startsWith('data:')) {
      dataLines.push(line.slice('data:'.length).replace(/^ /, ''));
    }
  }
  return { event, data: dataLines.join('\n') };
}

const LIVE_RECONNECT_DELAY_MS = 1000;

export interface LiveHandlers {
  onItem: (item: SessionItem) => void;
  onRollover?: (sessionIndex: number) => void;
  onError?: (err: unknown) => void;
}

// Subscribes to a federation's live consensus session via SSE, using
// `authedFetch` (not `EventSource`) so the bearer token can travel as an
// `Authorization` header, per the transport decision in the Task 6 brief.
// Runs an internal reconnect loop: on stream end or error, waits briefly and
// reconnects, unless `signal` is aborted. Fire-and-forget (void return); the
// caller controls the subscription lifetime entirely via `signal`.
export function subscribeLive(
  federationId: string,
  handlers: LiveHandlers,
  signal: AbortSignal
): void {
  void runLiveLoop(federationId, handlers, signal);
}

async function runLiveLoop(
  federationId: string,
  handlers: LiveHandlers,
  signal: AbortSignal
): Promise<void> {
  while (!signal.aborted) {
    try {
      const res = await authedFetch(`/federations/${federationId}/live`, {
        signal,
        headers: { Accept: 'text/event-stream' },
      });
      if (!res.ok || !res.body) {
        throw new Error(`Live stream request failed with status ${res.status}`);
      }
      await readLiveStream(res.body, handlers);
      // Stream ended cleanly (server closed it) without abort: fall through
      // to the reconnect delay below and try again.
    } catch (err) {
      if (signal.aborted) {
        return;
      }
      handlers.onError?.(err);
    }
    if (signal.aborted) {
      return;
    }
    await delay(LIVE_RECONNECT_DELAY_MS, signal);
  }
}

async function readLiveStream(body: ReadableStream<Uint8Array>, handlers: LiveHandlers): Promise<void> {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) {
        return;
      }
      buffer += decoder.decode(value, { stream: true });
      let boundary = buffer.indexOf('\n\n');
      while (boundary !== -1) {
        const frame = buffer.slice(0, boundary);
        buffer = buffer.slice(boundary + 2);
        dispatchSseFrame(frame, handlers);
        boundary = buffer.indexOf('\n\n');
      }
    }
  } catch (err) {
    // Release the stream promptly on a mid-stream error instead of leaving
    // it dangling until GC -- the caller (`runLiveLoop`) reconnects right
    // after, so a fresh `fetch` starts while the old stream is still
    // technically open otherwise. `cancel()`'s own result/rejection isn't
    // useful here (we're already erroring out), so it's intentionally
    // ignored.
    void reader.cancel().catch(() => {});
    throw err;
  }
}

function dispatchSseFrame(frame: string, handlers: LiveHandlers): void {
  // The backend enables axum's SSE `KeepAlive`, which periodically sends a
  // comment-only frame (a bare `:...` line, no `event:`/`data:` field) to
  // keep the connection alive through proxies. Such a frame carries no
  // event/data field at all, so skip it here rather than falling through to
  // `JSON.parse('')`, which would throw.
  const hasField = frame
    .split('\n')
    .some((rawLine) => /^(event|data):/.test(rawLine.replace(/\r$/, '')));
  if (!hasField) {
    return;
  }
  const { event, data } = parseSseFrame(frame);
  if (event === 'message') {
    if (!data) {
      return;
    }
    handlers.onItem(JSON.parse(data) as SessionItem);
  } else if (event === 'rollover') {
    handlers.onRollover?.(Number(data));
  }
}

// Waits `ms` milliseconds, resolving early (without throwing) if `signal` is
// aborted first, so the live-loop's reconnect backoff doesn't keep a pending
// timer alive past unmount.
function delay(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const onAbort = () => {
      clearTimeout(timer);
      resolve();
    };
    const timer = setTimeout(() => {
      signal.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    signal.addEventListener('abort', onAbort, { once: true });
  });
}
