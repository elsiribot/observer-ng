import { useEffect, useRef } from 'react';
import type { SessionItem } from '../../types/api';
import { Alert } from '../Alert';
import { renderItem } from './itemRenderers';

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
        {items.map((item) => (
          <div key={`${item.session_index}-${item.item_index}`}>{renderItem(item)}</div>
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
