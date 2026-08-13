import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor, fireEvent, act } from '@testing-library/react';
import { ConsensusTab } from './ConsensusTab';
import { api, subscribeLive, type LiveHandlers } from '../../services/api';
import type { SessionItem, ConsensusPage } from '../../types/api';

vi.mock('../../services/api', () => ({
  api: {
    getConsensusPage: vi.fn(),
  },
  subscribeLive: vi.fn(),
}));

// Counts the item rows <ItemList> renders (each item is a direct child of the
// `divide-y` container), so a test can assert prepend/dedup without coupling
// to any renderer's internal markup.
function renderedItemCount(container: HTMLElement): number {
  return container.querySelector('.divide-y')?.children.length ?? 0;
}

// jsdom has no IntersectionObserver; stub it and capture the callback so
// tests can simulate the "load more" sentinel scrolling into view.
let ioCallback: ((entries: Partial<IntersectionObserverEntry>[]) => void) | null = null;

class FakeIntersectionObserver {
  constructor(callback: (entries: Partial<IntersectionObserverEntry>[]) => void) {
    ioCallback = callback;
  }
  observe() {}
  disconnect() {}
  unobserve() {}
}

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
    estimated_time: null,
    time_lower: null,
    time_upper: null,
    time_source: null,
    role: null,
    ...overrides,
  };
}

function makePage(items: SessionItem[], next: [number, number] | null): ConsensusPage {
  return { items, next };
}

function triggerLoadMore() {
  act(() => {
    ioCallback?.([{ isIntersecting: true }]);
  });
}

describe('ConsensusTab', () => {
  beforeEach(() => {
    ioCallback = null;
    // `vi.restoreAllMocks()` in afterEach only restores real spies
    // (vi.spyOn); the plain `vi.fn()` from the module mock above keeps its
    // call history across tests unless explicitly reset here.
    vi.mocked(api.getConsensusPage).mockReset();
    vi.mocked(subscribeLive).mockReset();
    vi.stubGlobal('IntersectionObserver', FakeIntersectionObserver);
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it('loads the "all" filter by default with no cursor', async () => {
    vi.mocked(api.getConsensusPage).mockResolvedValueOnce(makePage([makeItem()], null));

    render(<ConsensusTab federationId="fed1" />);

    await waitFor(() =>
      expect(api.getConsensusPage).toHaveBeenCalledWith('fed1', {
        filter: 'all',
        beforeSession: undefined,
        beforeItem: undefined,
        limit: 25,
      })
    );
  });

  it('re-queries with the new filter and resets the cursor when a chip is clicked', async () => {
    vi.mocked(api.getConsensusPage)
      .mockResolvedValueOnce(makePage([makeItem({ item_index: 1 })], [10, 1]))
      .mockResolvedValueOnce(makePage([makeItem({ item_index: 2, kind: 'ln' })], null));

    render(<ConsensusTab federationId="fed1" />);

    await waitFor(() => expect(api.getConsensusPage).toHaveBeenCalledTimes(1));

    fireEvent.click(screen.getByRole('button', { name: 'ln' }));

    await waitFor(() =>
      expect(api.getConsensusPage).toHaveBeenLastCalledWith('fed1', {
        filter: 'ln',
        beforeSession: undefined,
        beforeItem: undefined,
        limit: 25,
      })
    );
    expect(api.getConsensusPage).toHaveBeenCalledTimes(2);
  });

  it('passes the `next` cursor from the previous page as before_session/before_item on load-more', async () => {
    vi.mocked(api.getConsensusPage)
      .mockResolvedValueOnce(makePage([makeItem({ item_index: 5 })], [10, 5]))
      .mockResolvedValueOnce(makePage([makeItem({ item_index: 4 })], null));

    render(<ConsensusTab federationId="fed1" />);

    await waitFor(() => expect(api.getConsensusPage).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(ioCallback).not.toBeNull());

    triggerLoadMore();

    await waitFor(() =>
      expect(api.getConsensusPage).toHaveBeenLastCalledWith('fed1', {
        filter: 'all',
        beforeSession: 10,
        beforeItem: 5,
        limit: 25,
      })
    );
  });

  it('stops fetching and renders no sentinel once `next` is null', async () => {
    vi.mocked(api.getConsensusPage).mockResolvedValueOnce(makePage([makeItem()], null));

    const { container } = render(<ConsensusTab federationId="fed1" />);

    await waitFor(() => expect(api.getConsensusPage).toHaveBeenCalledTimes(1));
    expect(container.querySelector('[aria-hidden="true"]')).not.toBeInTheDocument();
  });

  it('toggling Live subscribes and prepends streamed items above the history, deduped', async () => {
    vi.mocked(api.getConsensusPage).mockResolvedValueOnce(
      makePage([makeItem({ session_index: 10, item_index: 0 })], null)
    );

    // Capture the handlers subscribeLive receives so the test can drive the
    // stream.
    let handlers: LiveHandlers | undefined;
    vi.mocked(subscribeLive).mockImplementation((_fed, h) => {
      handlers = h;
    });

    const { container } = render(<ConsensusTab federationId="fed1" />);
    await waitFor(() => expect(api.getConsensusPage).toHaveBeenCalledTimes(1));
    expect(renderedItemCount(container)).toBe(1); // history only

    fireEvent.click(screen.getByRole('button', { name: /Live/i }));
    await waitFor(() =>
      expect(subscribeLive).toHaveBeenCalledWith('fed1', expect.anything(), expect.any(AbortSignal))
    );

    // A live item from the current (newer) open session arrives.
    const live = makeItem({ session_index: 11, item_index: 0 });
    act(() => handlers?.onItem(live));
    expect(renderedItemCount(container)).toBe(2); // live prepended above history

    // The same item replayed on reconnect is deduped, not duplicated.
    act(() => handlers?.onItem({ ...live }));
    expect(renderedItemCount(container)).toBe(2);
  });

  it('live items not matching the active filter are ignored', async () => {
    vi.mocked(api.getConsensusPage).mockResolvedValue(makePage([], null));

    let handlers: LiveHandlers | undefined;
    vi.mocked(subscribeLive).mockImplementation((_fed, h) => {
      handlers = h;
    });

    const { container } = render(<ConsensusTab federationId="fed1" />);
    await waitFor(() => expect(api.getConsensusPage).toHaveBeenCalledTimes(1));

    // Filter to Transactions, then toggle live on.
    fireEvent.click(screen.getByRole('button', { name: 'Transactions' }));
    await waitFor(() => expect(api.getConsensusPage).toHaveBeenCalledTimes(2));
    fireEvent.click(screen.getByRole('button', { name: /Live/i }));
    await waitFor(() => expect(subscribeLive).toHaveBeenCalled());

    // A consensus-item (non-transaction) live item is dropped under the
    // "Transactions" filter; a transaction item is kept.
    act(() => handlers?.onItem(makeItem({ session_index: 11, item_index: 0, item_type: 'ci' })));
    expect(renderedItemCount(container)).toBe(0);
    act(() =>
      handlers?.onItem(
        makeItem({ session_index: 11, item_index: 1, item_type: 'transaction', kind: null })
      )
    );
    expect(renderedItemCount(container)).toBe(1);
  });

  it('turning Live off aborts the subscription', async () => {
    vi.mocked(api.getConsensusPage).mockResolvedValue(makePage([], null));

    let signal: AbortSignal | undefined;
    vi.mocked(subscribeLive).mockImplementation((_fed, _h, s) => {
      signal = s;
    });

    render(<ConsensusTab federationId="fed1" />);
    await waitFor(() => expect(api.getConsensusPage).toHaveBeenCalled());

    fireEvent.click(screen.getByRole('button', { name: /Live/i }));
    await waitFor(() => expect(subscribeLive).toHaveBeenCalled());
    expect(signal?.aborted).toBe(false);

    fireEvent.click(screen.getByRole('button', { name: /Live/i }));
    await waitFor(() => expect(signal?.aborted).toBe(true));
  });
});
