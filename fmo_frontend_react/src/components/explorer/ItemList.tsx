import { useEffect, useRef } from 'react';
import type { SessionItem } from '../../types/api';
import { formatTimestamp, timeAgo } from '../../utils/format';
import { Alert } from '../Alert';
import { itemLabel, renderItem } from './itemRenderers';

interface ItemListProps {
  items: SessionItem[];
  /** Used only to tailor the empty-state copy. */
  scope: 'session' | 'consensus';
  /** Called with the last item currently loaded when the "load more"
   * sentinel scrolls into view, so the caller can derive its next keyset
   * cursor (session_index, or session_index+item_index) from it. Not
   * called while `loading` or when `hasMore` is false. */
  onLoadMore: (lastItem: SessionItem) => void;
  loading?: boolean;
  error?: string | null;
  /** Whether there may be more items to load. Defaults to true. */
  hasMore?: boolean;
}

export function ItemList({
  items,
  scope,
  onLoadMore,
  loading = false,
  error = null,
  hasMore = true,
}: ItemListProps) {
  const sentinelRef = useRef<HTMLDivElement | null>(null);
  const lastItem = items.length > 0 ? items[items.length - 1] : null;

  useEffect(() => {
    const node = sentinelRef.current;
    if (!node || !hasMore || loading || !lastItem) {
      return;
    }
    const observer = new IntersectionObserver((entries) => {
      if (entries.some((entry) => entry.isIntersecting)) {
        onLoadMore(lastItem);
      }
    });
    observer.observe(node);
    return () => observer.disconnect();
  }, [hasMore, loading, lastItem, onLoadMore]);

  if (error) {
    return <Alert level="error" message={error} />;
  }

  if (!loading && items.length === 0) {
    return (
      <div className="text-center text-xs sm:text-sm text-gray-500 dark:text-gray-400 py-12">
        {scope === 'session' ? 'No items in this session' : 'No consensus items found'}
      </div>
    );
  }

  return (
    <div>
      <div className="divide-y divide-gray-200 dark:divide-gray-700">
        {items.map((item, idx) => {
          // In the federation-wide consensus stream, mark where each new
          // session begins with a labeled horizontal rule. (In session scope
          // every item shares one session, so no dividers.)
          const startsNewSession =
            scope === 'consensus' &&
            (idx === 0 || item.session_index !== items[idx - 1].session_index);
          return (
            <div key={`${item.session_index}-${item.item_index}`}>
              {startsNewSession && (
                <div
                  className="flex items-center gap-2 pt-3 pb-1 text-xs font-mono text-gray-400 dark:text-gray-500"
                  aria-label={`Session ${item.session_index}`}
                >
                  <span className="shrink-0">
                    Session {item.session_index}
                    {item.estimated_time !== null && (
                      <>
                        {' · '}
                        {formatTimestamp(item.estimated_time)}{' '}
                        <span className="text-gray-300 dark:text-gray-600">
                          ({timeAgo(item.estimated_time)})
                        </span>
                      </>
                    )}
                  </span>
                  <span className="flex-1 border-t border-gray-300 dark:border-gray-600" />
                </div>
              )}
              <div className="flex items-start gap-2 sm:gap-3 py-3">
                <span
                  className="shrink-0 font-mono text-[11px] leading-5 text-gray-400 dark:text-gray-500 tabular-nums"
                  title={`Session ${item.session_index}, item ${item.item_index}`}
                >
                  {item.session_index}.{item.item_index}
                </span>
                <div className="shrink-0 w-24 sm:w-36">{itemLabel(item)}</div>
                <div className="flex-1 min-w-0">{renderItem(item)}</div>
              </div>
            </div>
          );
        })}
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
