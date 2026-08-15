import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import { SessionDetail, sessionItemBreakdown } from './SessionDetail';
import { api } from '../services/api';
import type { SessionItem, SessionSummary } from '../types/api';

vi.mock('../services/api', () => ({
  api: {
    getSessionItems: vi.fn(),
    getSessionPage: vi.fn(),
  },
}));

// jsdom has no IntersectionObserver; ItemList uses it, so stub it (see
// ConsensusTab.test.tsx / ItemList.test.tsx for the same pattern).
class FakeIntersectionObserver {
  observe() {}
  disconnect() {}
  unobserve() {}
}

function makeItem(overrides: Partial<SessionItem> = {}): SessionItem {
  return {
    session_index: 5,
    item_index: 0,
    item_type: 'ci',
    kind: 'wallet',
    peer_id: 0,
    txid: null,
    ecash_anon_bits: null,
    user_tx_key: null,
    user_tx_kind: null,
    direction: null,
    details: null,
    estimated_time: 1_700_000_000,
    time_lower: null,
    time_upper: null,
    time_source: null,
    role: null,
    ...overrides,
  };
}

function makeSummary(overrides: Partial<SessionSummary> = {}): SessionSummary {
  return {
    session_index: 5,
    estimated_time: 1_700_000_000,
    time_lower: null,
    time_upper: null,
    time_source: null,
    tx_count: 0,
    items_by_kind: {},
    ...overrides,
  };
}

function renderPage(federationId = 'fed1', sessionIndex = 5) {
  return render(
    <MemoryRouter initialEntries={[`/federations/${federationId}/session/${sessionIndex}`]}>
      <Routes>
        <Route path="/federations/:id/session/:session_index" element={<SessionDetail />} />
      </Routes>
    </MemoryRouter>
  );
}

describe('sessionItemBreakdown', () => {
  it('orders transactions first, then kinds by count desc then label asc', () => {
    const items: SessionItem[] = [
      makeItem({ item_type: 'transaction', item_index: 0 }),
      makeItem({ item_type: 'transaction', item_index: 1 }),
      makeItem({ item_type: 'ci', kind: 'ln', item_index: 2 }),
      makeItem({ item_type: 'ci', kind: 'ln', item_index: 3 }),
      makeItem({ item_type: 'ci', kind: 'ln', item_index: 4 }),
      makeItem({ item_type: 'ci', kind: 'wallet', item_index: 5 }),
      makeItem({ item_type: 'ci', kind: null, item_index: 6 }),
    ];

    expect(sessionItemBreakdown(items)).toEqual([
      { label: 'transactions', count: 2 },
      { label: 'ln', count: 3 },
      { label: 'unknown', count: 1 },
      { label: 'wallet', count: 1 },
    ]);
  });
});

describe('SessionDetail page', () => {
  beforeEach(() => {
    vi.mocked(api.getSessionItems).mockReset();
    vi.mocked(api.getSessionPage).mockReset();
    vi.stubGlobal('IntersectionObserver', FakeIntersectionObserver);
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it('renders prev/next session links with correct hrefs', async () => {
    vi.mocked(api.getSessionItems).mockResolvedValueOnce([makeItem()]);
    vi.mocked(api.getSessionPage).mockResolvedValueOnce([makeSummary()]);

    renderPage('fed1', 5);

    expect(await screen.findByText('Session 5')).toBeInTheDocument();

    const prevLink = screen.getByRole('link', { name: /Session 4/i });
    expect(prevLink).toHaveAttribute('href', '/federations/fed1/session/4');

    const nextLink = screen.getByRole('link', { name: /Session 6/i });
    expect(nextLink).toHaveAttribute('href', '/federations/fed1/session/6');
  });

  it('omits the prev-session link at session 0', async () => {
    vi.mocked(api.getSessionItems).mockResolvedValueOnce([]);
    vi.mocked(api.getSessionPage).mockResolvedValueOnce([]);

    renderPage('fed1', 0);

    expect(await screen.findByText('Session 0')).toBeInTheDocument();

    expect(screen.queryByRole('link', { name: /Session -1/i })).not.toBeInTheDocument();
    expect(screen.getByRole('link', { name: /Session 1/i })).toHaveAttribute(
      'href',
      '/federations/fed1/session/1'
    );
  });
});
