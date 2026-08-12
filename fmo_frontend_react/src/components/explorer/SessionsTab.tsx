import { useCallback, useEffect, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import { api } from '../../services/api';
import type { SessionSummary } from '../../types/api';
import { Badge } from '../Badge';
import { Alert } from '../Alert';
import { formatNumber, formatTimestamp } from '../../utils/format';

const PAGE_SIZE = 25;

interface SessionsTabProps {
  federationId: string;
}

// Infinite-scroll list of a federation's sessions (newest first), keyset
// paginated via `api.getSessionPage` using the last loaded row's
// `session_index` as the `before` cursor. Each row links to the
// session-detail page for drilling into that session's items.
export function SessionsTab({ federationId }: SessionsTabProps) {
  const [sessions, setSessions] = useState<SessionSummary[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [hasMore, setHasMore] = useState(true);
  const sentinelRef = useRef<HTMLDivElement | null>(null);

  const loadPage = useCallback(
    (before?: number) => {
      setLoading(true);
      setError(null);
      api
        .getSessionPage(federationId, before, PAGE_SIZE)
        .then((page) => {
          setSessions((prev) => (before === undefined ? page : [...prev, ...page]));
          setHasMore(page.length >= PAGE_SIZE);
        })
        .catch((err: unknown) => {
          setError(err instanceof Error ? err.message : 'Failed to load sessions');
        })
        .finally(() => setLoading(false));
    },
    [federationId]
  );

  // (Re)load the first page whenever the federation changes.
  useEffect(() => {
    setSessions([]);
    setHasMore(true);
    loadPage(undefined);
  }, [federationId, loadPage]);

  const lastSession = sessions.length > 0 ? sessions[sessions.length - 1] : null;

  useEffect(() => {
    const node = sentinelRef.current;
    if (!node || !hasMore || loading || !lastSession) {
      return;
    }
    const observer = new IntersectionObserver((entries) => {
      if (entries.some((entry) => entry.isIntersecting)) {
        loadPage(lastSession.session_index);
      }
    });
    observer.observe(node);
    return () => observer.disconnect();
  }, [hasMore, loading, lastSession, loadPage]);

  if (error && sessions.length === 0) {
    return <Alert level="error" message={error} />;
  }

  if (!loading && sessions.length === 0) {
    return (
      <div className="text-center text-xs sm:text-sm text-gray-500 dark:text-gray-400 py-12">
        No sessions found
      </div>
    );
  }

  return (
    <div>
      {error && sessions.length > 0 && <Alert level="error" message={error} />}
      <div className="divide-y divide-gray-200 dark:divide-gray-700">
        {sessions.map((session) => (
          <Link
            key={session.session_index}
            to={`/federations/${federationId}/session/${session.session_index}`}
            className="block py-3 hover:bg-gray-50 dark:hover:bg-gray-700/40 -mx-2 px-2 rounded"
          >
            <div className="flex flex-col sm:flex-row sm:items-center gap-1 sm:gap-3">
              <span className="font-mono text-sm text-gray-900 dark:text-white shrink-0">
                Session {formatNumber(session.session_index)}
              </span>
              <span className="text-xs text-gray-500 dark:text-gray-400 shrink-0">
                {formatTimestamp(session.estimated_time)}
              </span>
              <span className="text-xs text-gray-500 dark:text-gray-400 shrink-0">
                {formatNumber(session.tx_count)} tx
              </span>
              <div className="flex gap-1 flex-wrap">
                {Object.entries(session.items_by_kind).map(([kind, count]) =>
                  typeof count === 'number' ? (
                    <Badge key={kind} level="info">
                      {kind}: {formatNumber(count)}
                    </Badge>
                  ) : null
                )}
              </div>
            </div>
          </Link>
        ))}
      </div>
      {loading && (
        <div className="text-center text-xs sm:text-sm text-gray-500 dark:text-gray-400 py-4">
          Loading…
        </div>
      )}
      {hasMore && !loading && <div ref={sentinelRef} className="h-1" aria-hidden="true" />}
    </div>
  );
}
