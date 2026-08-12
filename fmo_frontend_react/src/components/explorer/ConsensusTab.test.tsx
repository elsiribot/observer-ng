import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor, fireEvent, act } from '@testing-library/react';
import { ConsensusTab } from './ConsensusTab';
import { api } from '../../services/api';
import type { SessionItem, ConsensusPage } from '../../types/api';

vi.mock('../../services/api', () => ({
  api: {
    getConsensusPage: vi.fn(),
  },
}));

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
});
