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
  /** peer id -> guardian display name, for labeling contribution chips and
   * showing which configured guardians are *missing* from a session. */
  guardianNames?: Record<number, string>;
}

// Compact per-guardian contribution chips for one session. When the full
// guardian set is known (`guardianNames`), every configured guardian gets a
// chip: solid if it contributed >=1 consensus item this session, red-outlined
// if it did not (the anomaly worth spotting). Without the guardian set, only
// the contributing peer ids are listed.
function GuardianChips({
  guardians,
  guardianNames,
}: {
  guardians: number[];
  guardianNames?: Record<number, string>;
}) {
  const contributed = new Set(guardians);
  const ids = guardianNames
    ? Object.keys(guardianNames)
        .map(Number)
        .sort((a, b) => a - b)
    : guardians;

  if (ids.length === 0) {
    return null;
  }

  return (
    <div className="flex gap-0.5 flex-wrap items-center">
      {ids.map((id) => {
        const present = contributed.has(id);
        const name = guardianNames?.[id];
        return (
          <span
            key={id}
            title={
              name
                ? `${name} ${present ? 'contributed a CI' : 'contributed no CI'}`
                : `Guardian ${id} contributed a CI`
            }
            className={
              'inline-flex items-center justify-center min-w-[1.1rem] h-[1.1rem] px-1 rounded text-[10px] font-mono ' +
              (present
                ? 'bg-gray-200 text-gray-700 dark:bg-gray-600 dark:text-gray-200'
                : 'border border-red-400 text-red-500 dark:border-red-500 dark:text-red-400')
            }
          >
            {id}
          </span>
        );
      })}
    </div>
  );
}

// Infinite-scroll list of a federation's sessions (newest first), keyset
// paginated via `api.getSessionPage` using the last loaded row's
// `session_index` as the `before` cursor. Each row links to the
// session-detail page for drilling into that session's items.
export function SessionsTab({ federationId, guardianNames }: SessionsTabProps) {
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
              <div className="sm:ml-auto">
                <GuardianChips guardians={session.guardians} guardianNames={guardianNames} />
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
