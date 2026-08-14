import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { SessionsTab } from './SessionsTab';
import { api } from '../../services/api';
import type { SessionSummary } from '../../types/api';

vi.mock('../../services/api', () => ({
  api: {
    getSessionPage: vi.fn(),
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

function makeSession(overrides: Partial<SessionSummary> = {}): SessionSummary {
  return {
    session_index: 100,
    estimated_time: 1_700_000_000,
    time_lower: null,
    time_upper: null,
    time_source: null,
    tx_count: 3,
    items_by_kind: { ln: 2, wallet: 1, ignored_not_a_number: { nested: true } },
    guardians: [0, 1, 2, 3],
    ...overrides,
  };
}

function triggerLoadMore() {
  ioCallback?.([{ isIntersecting: true }]);
}

describe('SessionsTab', () => {
  beforeEach(() => {
    ioCallback = null;
    vi.stubGlobal('IntersectionObserver', FakeIntersectionObserver);
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it('renders session rows with per-kind badges and formatted time, linking to the session route', async () => {
    vi.mocked(api.getSessionPage).mockResolvedValueOnce([makeSession()]);

    render(
      <MemoryRouter>
        <SessionsTab federationId="fed1" />
      </MemoryRouter>
    );

    expect(await screen.findByText('Session 100')).toBeInTheDocument();
    expect(screen.getByText('3 tx')).toBeInTheDocument();
    // Non-numeric items_by_kind entries must not blow up rendering.
    expect(screen.queryByText(/ignored_not_a_number/)).not.toBeInTheDocument();
    expect(screen.getByText('ln: 2')).toBeInTheDocument();
    expect(screen.getByText('wallet: 1')).toBeInTheDocument();
    expect(screen.getByText(new Date(1_700_000_000 * 1000).toLocaleString())).toBeInTheDocument();

    const link = screen.getByRole('link', { name: /session 100/i });
    expect(link).toHaveAttribute('href', '/federations/fed1/session/100');

    expect(api.getSessionPage).toHaveBeenCalledWith('fed1', undefined, expect.any(Number));
  });

  it('flags a guardian that contributed no CI to a session', async () => {
    // Session had CIs from guardians 0 and 2 only; the federation has 0..3.
    vi.mocked(api.getSessionPage).mockResolvedValueOnce([makeSession({ guardians: [0, 2] })]);

    render(
      <MemoryRouter>
        <SessionsTab
          federationId="fed1"
          guardianNames={{ 0: 'Alice', 1: 'Bob', 2: 'Carol', 3: 'Dave' }}
        />
      </MemoryRouter>
    );

    // Contributing guardians describe their name + "contributed a CI".
    expect(await screen.findByTitle(/Alice contributed a CI/)).toBeInTheDocument();
    expect(screen.getByTitle(/Carol contributed a CI/)).toBeInTheDocument();
    // Missing guardians (1, 3) are flagged as having contributed no CI.
    expect(screen.getByTitle(/Bob contributed no CI/)).toBeInTheDocument();
    expect(screen.getByTitle(/Dave contributed no CI/)).toBeInTheDocument();
  });

  it('shows "unknown" for a null estimated_time', async () => {
    vi.mocked(api.getSessionPage).mockResolvedValueOnce([
      makeSession({ session_index: 5, estimated_time: null }),
    ]);

    render(
      <MemoryRouter>
        <SessionsTab federationId="fed1" />
      </MemoryRouter>
    );

    expect(await screen.findByText('Session 5')).toBeInTheDocument();
    expect(screen.getByText('unknown')).toBeInTheDocument();
  });

  it('loads the next page with the last session_index as the before cursor when the sentinel intersects', async () => {
    const firstPage: SessionSummary[] = Array.from({ length: 25 }, (_, i) =>
      makeSession({ session_index: 100 - i })
    );
    const secondPage: SessionSummary[] = [makeSession({ session_index: 74 })];

    vi.mocked(api.getSessionPage)
      .mockResolvedValueOnce(firstPage)
      .mockResolvedValueOnce(secondPage);

    render(
      <MemoryRouter>
        <SessionsTab federationId="fed1" />
      </MemoryRouter>
    );

    await screen.findByText('Session 100');
    expect(api.getSessionPage).toHaveBeenCalledWith('fed1', undefined, 25);

    await waitFor(() => expect(ioCallback).not.toBeNull());
    triggerLoadMore();

    await waitFor(() =>
      expect(api.getSessionPage).toHaveBeenLastCalledWith('fed1', 76, 25)
    );
    expect(await screen.findByText('Session 74')).toBeInTheDocument();
  });

  it('stops loading more once a page returns fewer than the limit', async () => {
    vi.mocked(api.getSessionPage).mockResolvedValueOnce([makeSession({ session_index: 1 })]);

    const { container } = render(
      <MemoryRouter>
        <SessionsTab federationId="fed1" />
      </MemoryRouter>
    );

    await screen.findByText('Session 1');
    // No sentinel should be rendered once hasMore is false (a short page).
    expect(container.querySelector('[aria-hidden="true"]')).not.toBeInTheDocument();
  });
});
