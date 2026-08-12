import { useCallback, useEffect, useState } from 'react';
import { api } from '../../services/api';
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

interface ConsensusTabProps {
  federationId: string;
}

// Federation-wide consensus item stream: filter chips (All / Transactions /
// per module kind) plus keyset infinite scroll, rendered via <ItemList>.
// Switching a filter resets the cursor and reloads from the start.
export function ConsensusTab({ federationId }: ConsensusTabProps) {
  const [filter, setFilter] = useState('all');
  const [items, setItems] = useState<SessionItem[]>([]);
  const [cursor, setCursor] = useState<[number, number] | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

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

  const handleLoadMore = useCallback(() => {
    if (cursor) {
      loadPage(filter, cursor);
    }
  }, [cursor, filter, loadPage]);

  return (
    <div>
      <div className="flex gap-2 flex-wrap mb-4">
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
      <ItemList
        items={items}
        scope="consensus"
        onLoadMore={handleLoadMore}
        hasMore={cursor !== null}
        loading={loading}
        error={error}
      />
    </div>
  );
}
