/* eslint-disable react-refresh/only-export-components --
 * `foldLiveItem` is exported alongside the component purely so the boundary
 * logic can be unit-tested as a pure function, mirroring the pattern in
 * `itemRenderers.tsx`. */
import { useEffect, useState } from 'react';
import { subscribeLive } from '../../services/api';
import type { SessionItem } from '../../types/api';
import { ItemList } from './ItemList';

interface LiveViewProps {
  federationId: string;
}

interface LiveState {
  currentSession: number | null;
  items: SessionItem[];
}

const EMPTY_LIVE_STATE: LiveState = { currentSession: null, items: [] };

// Folds one incoming item into the current live state. Pure function of
// (prevState, item) — no external mutation — so it's safe to pass to
// `setState`'s functional-updater form under React 18 StrictMode's
// double-invoke-in-dev behavior.
//
// Session-boundary decision (see Task 6 brief): the backend's `rollover`
// event is a hint only and may be coalesced/dropped, so the boundary is
// derived from the item stream itself. An incoming item whose
// `session_index` exceeds the displayed session means the previous session
// sealed, so the list resets to just that item. An item at
// `=== currentSession` appends (deduped by `${session_index}-${item_index}`
// to tolerate a reconnect replaying items already shown). An item from an
// older session (a stale/late arrival) is ignored.
export function foldLiveItem(prev: LiveState, item: SessionItem): LiveState {
  if (prev.currentSession === null || item.session_index > prev.currentSession) {
    return { currentSession: item.session_index, items: [item] };
  }
  if (item.session_index < prev.currentSession) {
    return prev;
  }
  const key = `${item.session_index}-${item.item_index}`;
  if (prev.items.some((existing) => `${existing.session_index}-${existing.item_index}` === key)) {
    return prev;
  }
  return { currentSession: prev.currentSession, items: [...prev.items, item] };
}

// Live tab: streams the federation's current (in-progress) consensus
// session via SSE and renders it with SP-1's <ItemList>/renderer registry.
export function LiveView({ federationId }: LiveViewProps) {
  const [state, setState] = useState<LiveState>(EMPTY_LIVE_STATE);
  const [error, setError] = useState<string | null>(null);
  const [justSealed, setJustSealed] = useState<number | null>(null);

  useEffect(() => {
    // Reset all view state on federation change so a stale item from the
    // previous subscription can't linger after the switch.
    setState(EMPTY_LIVE_STATE);
    setError(null);
    setJustSealed(null);

    const controller = new AbortController();

    subscribeLive(
      federationId,
      {
        onItem: (item) => {
          setError(null);
          setState((prev) => foldLiveItem(prev, item));
        },
        onRollover: (sessionIndex) => {
          setJustSealed(sessionIndex);
        },
        onError: (err) => {
          setError(err instanceof Error ? err.message : 'Live connection interrupted, retrying…');
        },
      },
      controller.signal
    );

    return () => controller.abort();
  }, [federationId]);

  const { items, currentSession } = state;

  return (
    <div>
      <div className="flex items-center gap-3 mb-4">
        <span className="relative flex h-2.5 w-2.5">
          <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75" />
          <span className="relative inline-flex rounded-full h-2.5 w-2.5 bg-green-500" />
        </span>
        <span className="text-xs sm:text-sm font-semibold text-gray-700 dark:text-gray-300">
          LIVE
        </span>
        <span className="text-xs sm:text-sm text-gray-500 dark:text-gray-400">
          Session {currentSession ?? '—'}
        </span>
        {justSealed !== null && (
          <span className="text-xs text-gray-400 dark:text-gray-500">
            (session {justSealed} sealed)
          </span>
        )}
      </div>

      {error && (
        <div className="mb-4 p-2 text-xs sm:text-sm text-yellow-800 rounded-lg bg-yellow-50 dark:bg-gray-800 dark:text-yellow-300">
          {error}
        </div>
      )}

      {items.length === 0 && !error && (
        <div className="text-center text-xs sm:text-sm text-gray-500 dark:text-gray-400 py-12">
          Waiting for the next consensus item…
        </div>
      )}

      {items.length > 0 && (
        <ItemList items={items} scope="consensus" onLoadMore={() => {}} hasMore={false} />
      )}
    </div>
  );
}
