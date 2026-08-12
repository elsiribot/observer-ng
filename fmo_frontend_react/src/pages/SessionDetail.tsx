import { useEffect, useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import { api } from '../services/api';
import type { SessionItem } from '../types/api';
import { ItemList } from '../components/explorer/ItemList';
import { Alert } from '../components/Alert';
import { formatNumber, formatTimestamp } from '../utils/format';

// Session-detail page: the full ordered item list (transactions + consensus
// items) of one session, drilled into from the federation's Sessions tab.
export function SessionDetail() {
  const { id, session_index: sessionIndexParam } = useParams<{
    id: string;
    session_index: string;
  }>();
  const sessionIndex = sessionIndexParam !== undefined ? Number(sessionIndexParam) : NaN;

  const [items, setItems] = useState<SessionItem[]>([]);
  // The session's estimated time isn't part of the item list response, so we
  // derive it from the one-row keyset page that starts just after this
  // session (session_index + 1, limit 1) — the same endpoint the Sessions
  // tab uses.
  const [estimatedTime, setEstimatedTime] = useState<number | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!id || Number.isNaN(sessionIndex)) {
      setError('Invalid session index');
      setLoading(false);
      return;
    }
    setLoading(true);
    setError(null);
    setEstimatedTime(null);
    Promise.all([
      api.getSessionItems(id, sessionIndex),
      api.getSessionPage(id, sessionIndex + 1, 1),
    ])
      .then(([sessionItems, page]) => {
        setItems(sessionItems);
        setEstimatedTime(
          page.length > 0 && page[0].session_index === sessionIndex
            ? page[0].estimated_time
            : null
        );
      })
      .catch((err: unknown) => {
        setError(err instanceof Error ? err.message : 'Failed to load session');
      })
      .finally(() => setLoading(false));
  }, [id, sessionIndex]);

  const txCount = items.filter((item) => item.item_type === 'transaction').length;
  const ciCount = items.length - txCount;

  return (
    <div className="py-4 sm:py-8 px-4 sm:px-0">
      <div className="mb-4 sm:mb-6">
        <Link
          to={`/federations/${id}`}
          className="text-sm sm:text-base text-blue-600 dark:text-blue-400 hover:underline"
        >
          ← Back to Federation
        </Link>
      </div>

      <h1 className="text-2xl sm:text-3xl font-bold text-gray-900 dark:text-white mb-2 break-words">
        Session {Number.isNaN(sessionIndex) ? sessionIndexParam : formatNumber(sessionIndex)}
      </h1>
      <div className="text-xs sm:text-sm text-gray-500 dark:text-gray-400 mb-6 sm:mb-8">
        {formatTimestamp(estimatedTime)} · {formatNumber(txCount)} transactions ·{' '}
        {formatNumber(ciCount)} consensus items
      </div>

      {error && items.length === 0 ? (
        <Alert level="error" message={error} />
      ) : (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow-md border border-gray-200 dark:border-gray-700 p-4 sm:p-6">
          <ItemList
            items={items}
            scope="session"
            onLoadMore={() => {}}
            hasMore={false}
            loading={loading}
            error={items.length === 0 ? null : error}
          />
        </div>
      )}
    </div>
  );
}
