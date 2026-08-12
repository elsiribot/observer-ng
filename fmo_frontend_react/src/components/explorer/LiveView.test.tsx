import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, act } from '@testing-library/react';
import { LiveView } from './LiveView';
import { subscribeLive } from '../../services/api';
import type { SessionItem } from '../../types/api';
import type { LiveHandlers } from '../../services/api';

vi.mock('../../services/api', () => ({
  subscribeLive: vi.fn(),
}));

function makeItem(overrides: Partial<SessionItem> = {}): SessionItem {
  return {
    session_index: 10,
    item_index: 0,
    item_type: 'ci',
    kind: 'wallet',
    peer_id: 0,
    txid: null,
    user_tx_key: null,
    user_tx_kind: null,
    direction: null,
    details: null,
    ...overrides,
  };
}

// Captures the (handlers, signal) passed to `subscribeLive` on the most
// recent call, so tests can drive `onItem`/`onRollover`/`onError` directly
// without a real fetch stream.
function latestSubscription(): { handlers: LiveHandlers; signal: AbortSignal } {
  const calls = vi.mocked(subscribeLive).mock.calls;
  const [, handlers, signal] = calls[calls.length - 1];
  return { handlers, signal };
}

describe('LiveView', () => {
  beforeEach(() => {
    vi.mocked(subscribeLive).mockReset();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('subscribes on mount and shows an idle hint before the first item', () => {
    render(<LiveView federationId="fed1" />);

    expect(subscribeLive).toHaveBeenCalledTimes(1);
    const [federationId] = vi.mocked(subscribeLive).mock.calls[0];
    expect(federationId).toBe('fed1');
    expect(screen.getByText('Waiting for the next consensus item…')).toBeInTheDocument();
    expect(screen.getByText('Session —')).toBeInTheDocument();
  });

  it('renders items as they arrive and appends within the same session', () => {
    render(<LiveView federationId="fed1" />);
    const { handlers } = latestSubscription();

    act(() => handlers.onItem(makeItem({ session_index: 10, item_index: 0, kind: 'wallet' })));
    act(() => handlers.onItem(makeItem({ session_index: 10, item_index: 1, kind: 'ln' })));

    expect(screen.getByText('Session 10')).toBeInTheDocument();
    expect(screen.queryByText('Waiting for the next consensus item…')).not.toBeInTheDocument();
  });

  it('dedupes an item replayed by a reconnect (same session_index/item_index)', () => {
    render(<LiveView federationId="fed1" />);
    const { handlers } = latestSubscription();

    act(() => handlers.onItem(makeItem({ session_index: 10, item_index: 0 })));
    act(() => handlers.onItem(makeItem({ session_index: 10, item_index: 1 })));
    // Reconnect replays item 0 again.
    act(() => handlers.onItem(makeItem({ session_index: 10, item_index: 0 })));

    const rows = document.querySelectorAll('.divide-y > div');
    expect(rows.length).toBe(2);
  });

  it('seals the previous session and resets the list when session_index advances', () => {
    render(<LiveView federationId="fed1" />);
    const { handlers } = latestSubscription();

    act(() => handlers.onItem(makeItem({ session_index: 10, item_index: 0 })));
    act(() => handlers.onItem(makeItem({ session_index: 10, item_index: 1 })));
    expect(document.querySelectorAll('.divide-y > div').length).toBe(2);

    act(() => handlers.onItem(makeItem({ session_index: 11, item_index: 0 })));

    expect(screen.getByText('Session 11')).toBeInTheDocument();
    expect(document.querySelectorAll('.divide-y > div').length).toBe(1);
  });

  it('ignores an item from an older session than the one currently displayed', () => {
    render(<LiveView federationId="fed1" />);
    const { handlers } = latestSubscription();

    act(() => handlers.onItem(makeItem({ session_index: 11, item_index: 0 })));
    act(() => handlers.onItem(makeItem({ session_index: 10, item_index: 5 })));

    expect(screen.getByText('Session 11')).toBeInTheDocument();
    expect(document.querySelectorAll('.divide-y > div').length).toBe(1);
  });

  it('shows a non-fatal banner on error without clearing items', () => {
    render(<LiveView federationId="fed1" />);
    const { handlers } = latestSubscription();

    act(() => handlers.onItem(makeItem({ session_index: 10, item_index: 0 })));
    act(() => handlers.onError?.(new Error('connection dropped')));

    expect(screen.getByText('connection dropped')).toBeInTheDocument();
    expect(screen.getByText('Session 10')).toBeInTheDocument();
  });

  it('aborts the subscription signal on unmount', () => {
    const { unmount } = render(<LiveView federationId="fed1" />);
    const { signal } = latestSubscription();
    expect(signal.aborted).toBe(false);

    unmount();

    expect(signal.aborted).toBe(true);
  });

  it('re-subscribes with a fresh signal when federationId changes', () => {
    const { rerender } = render(<LiveView federationId="fed1" />);
    const { signal: firstSignal } = latestSubscription();

    rerender(<LiveView federationId="fed2" />);

    expect(subscribeLive).toHaveBeenCalledTimes(2);
    const [secondFederationId] = vi.mocked(subscribeLive).mock.calls[1];
    expect(secondFederationId).toBe('fed2');
    expect(firstSignal.aborted).toBe(true);
  });
});
