import { useCallback, useEffect, useMemo, useState } from 'react';
import { api, subscribeLive } from '../../services/api';
import type { SessionItem } from '../../types/api';
import { ItemList } from './ItemList';

const PAGE_SIZE = 25;

// Known module kinds. The module set observed by this server is small and
// effectively static (one crate per kind, see fmo_modules/), so a hardcoded
// list is simpler and cheaper than adding a session-stats aggregate endpoint
// just to derive it. An unrecognized/future kind simply won't get a filter
// chip; it still shows up under "All".
const MODULE_KINDS = [
  'ln',
  'lnv2',
  'wallet',
  'walletv2',
  'mint',
  'mintv2',
  'stability_pool',
  'multi_sig_stability_pool',
  'meta',
];

const FILTERS: { value: string; label: string }[] = [
  { value: 'all', label: 'All' },
  { value: 'transaction', label: 'Transactions' },
  ...MODULE_KINDS.map((kind) => ({ value: kind, label: kind })),
];

function itemKey(item: SessionItem): string {
  return `${item.session_index}-${item.item_index}`;
}

// Whether a live-streamed item belongs in the current filter view, mirroring
// the backend consensus-stream filter semantics (`all` / `transaction` / a
// module kind). Applied client-side so the live buffer matches the chips.
function matchesFilter(item: SessionItem, filter: string): boolean {
  if (filter === 'all') return true;
  if (filter === 'transaction') return item.item_type === 'transaction';
  return item.item_type === 'ci' && item.kind === filter;
}

interface ConsensusTabProps {
  federationId: string;
}

// Federation-wide consensus item stream: filter chips (All / Transactions /
// per module kind) plus keyset infinite scroll, rendered via <ItemList>.
// Switching a filter resets the cursor and reloads from the start.
//
// A "Live" toggle streams the current (in-progress) session over SSE and
// buffers those items newest-first ABOVE the paginated history, so new
// consensus items appear at the top as guardians accept them. The live buffer
// respects the active filter and is deduped against the loaded history (the
// open session's items can also appear in a fetched page).
export function ConsensusTab({ federationId }: ConsensusTabProps) {
  const [filter, setFilter] = useState('all');
  const [items, setItems] = useState<SessionItem[]>([]);
  const [liveItems, setLiveItems] = useState<SessionItem[]>([]);
  const [cursor, setCursor] = useState<[number, number] | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [live, setLive] = useState(false);
  const [liveError, setLiveError] = useState<string | null>(null);

  const loadPage = useCallback(
    (activeFilter: string, before?: [number, number]) => {
      setLoading(true);
      setError(null);
      api
        .getConsensusPage(federationId, {
          filter: activeFilter,
          beforeSession: before?.[0],
          beforeItem: before?.[1],
          limit: PAGE_SIZE,
        })
        .then((page) => {
          setItems((prev) => (before === undefined ? page.items : [...prev, ...page.items]));
          setCursor(page.next);
        })
        .catch((err: unknown) => {
          setError(err instanceof Error ? err.message : 'Failed to load consensus items');
        })
        .finally(() => setLoading(false));
    },
    [federationId]
  );

  // (Re)load the first page whenever the federation or the active filter
  // changes, resetting the keyset cursor.
  useEffect(() => {
    setItems([]);
    setCursor(null);
    loadPage(filter, undefined);
  }, [federationId, filter, loadPage]);

  // Live subscription. Enabled by the toggle; a federation or filter change
  // resets the buffer and reconnects so it matches the current view (the SSE
  // connect replays the current session's items, which we re-filter). Each
  // incoming item is prepended (newest at the top) and deduped by key.
  useEffect(() => {
    setLiveItems([]);
    setLiveError(null);
    if (!live) {
      return;
    }
    const controller = new AbortController();
    subscribeLive(
      federationId,
      {
        onItem: (item) => {
          if (!matchesFilter(item, filter)) {
            return;
          }
          setLiveError(null);
          setLiveItems((prev) =>
            prev.some((existing) => itemKey(existing) === itemKey(item))
              ? prev
              : [item, ...prev]
          );
        },
        onError: (err) => {
          setLiveError(
            err instanceof Error ? err.message : 'Live connection interrupted, retrying…'
          );
        },
      },
      controller.signal
    );
    return () => controller.abort();
  }, [federationId, filter, live]);

  const handleLoadMore = useCallback(() => {
    if (cursor) {
      loadPage(filter, cursor);
    }
  }, [cursor, filter, loadPage]);

  // Live buffer (newest-first) on top of the paginated history, dropping any
  // history row already shown live so the open session isn't duplicated.
  const merged = useMemo(() => {
    if (liveItems.length === 0) {
      return items;
    }
    const liveKeys = new Set(liveItems.map(itemKey));
    return [...liveItems, ...items.filter((it) => !liveKeys.has(itemKey(it)))];
  }, [liveItems, items]);

  return (
    <div>
      <div className="flex items-center justify-between gap-2 flex-wrap mb-4">
        <div className="flex gap-2 flex-wrap">
          {FILTERS.map(({ value, label }) => (
            <button
              key={value}
              type="button"
              onClick={() => setFilter(value)}
              aria-pressed={filter === value}
              className={`px-3 py-1 rounded-full text-xs font-medium border whitespace-nowrap ${
                filter === value
                  ? 'bg-blue-600 text-white border-blue-600 dark:bg-blue-500 dark:border-blue-500'
                  : 'bg-gray-100 text-gray-700 border-gray-300 hover:bg-gray-200 dark:bg-gray-800 dark:text-gray-300 dark:border-gray-600 dark:hover:bg-gray-700'
              }`}
            >
              {label}
            </button>
          ))}
        </div>
        <button
          type="button"
          onClick={() => setLive((v) => !v)}
          aria-pressed={live}
          title={live ? 'Stop streaming live consensus items' : 'Stream live consensus items'}
          className={`inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-medium border whitespace-nowrap ${
            live
              ? 'bg-green-600 text-white border-green-600 dark:bg-green-500 dark:border-green-500'
              : 'bg-gray-100 text-gray-700 border-gray-300 hover:bg-gray-200 dark:bg-gray-800 dark:text-gray-300 dark:border-gray-600 dark:hover:bg-gray-700'
          }`}
        >
          <span className="relative flex h-2 w-2">
            {live && (
              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-300 opacity-75" />
            )}
            <span
              className={`relative inline-flex rounded-full h-2 w-2 ${
                live ? 'bg-white' : 'bg-gray-400'
              }`}
            />
          </span>
          Live
        </button>
      </div>
      {live && liveError && (
        <div className="mb-4 p-2 text-xs sm:text-sm text-yellow-800 rounded-lg bg-yellow-50 dark:bg-gray-800 dark:text-yellow-300">
          {liveError}
        </div>
      )}
      <ItemList
        items={merged}
        scope="consensus"
        onLoadMore={handleLoadMore}
        hasMore={cursor !== null}
        loading={loading}
        error={error}
      />
    </div>
  );
}
