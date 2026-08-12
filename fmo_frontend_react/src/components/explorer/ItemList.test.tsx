import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { ItemList } from './ItemList';
import type { SessionItem } from '../../types/api';

// jsdom has no IntersectionObserver; ItemList sets one up for its load-more
// sentinel, so stub it out.
class FakeIntersectionObserver {
  observe() {}
  disconnect() {}
  unobserve() {}
}

function makeItem(overrides: Partial<SessionItem> = {}): SessionItem {
  return {
    session_index: 12,
    item_index: 0,
    item_type: 'ci',
    kind: null,
    peer_id: 0,
    txid: null,
    user_tx_key: null,
    user_tx_kind: null,
    direction: null,
    details: null,
    estimated_time: null,
    role: null,
    ...overrides,
  };
}

function renderList(items: SessionItem[], scope: 'consensus' | 'session') {
  return render(
    <MemoryRouter initialEntries={['/federations/fed1']}>
      <ItemList items={items} scope={scope} onLoadMore={() => {}} hasMore={false} />
    </MemoryRouter>
  );
}

describe('ItemList', () => {
  beforeEach(() => {
    vi.stubGlobal('IntersectionObserver', FakeIntersectionObserver);
  });
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('renders a session.item index next to every item', () => {
    renderList([makeItem({ session_index: 12, item_index: 3 }), makeItem({ session_index: 12, item_index: 2 })], 'consensus');
    expect(screen.getByText('12.3')).toBeInTheDocument();
    expect(screen.getByText('12.2')).toBeInTheDocument();
  });

  it('inserts a "Session N" divider at each new session in consensus scope', () => {
    renderList(
      [
        makeItem({ session_index: 12, item_index: 1 }),
        makeItem({ session_index: 12, item_index: 0 }),
        makeItem({ session_index: 11, item_index: 4 }),
      ],
      'consensus'
    );
    // A divider for the first session and for the boundary into session 11,
    // but not a second one within session 12.
    expect(screen.getByText('Session 12')).toBeInTheDocument();
    expect(screen.getByText('Session 11')).toBeInTheDocument();
    expect(screen.getAllByText(/^Session \d+$/)).toHaveLength(2);
  });

  it('shows no session dividers in session scope', () => {
    renderList([makeItem({ session_index: 12, item_index: 0 }), makeItem({ session_index: 12, item_index: 1 })], 'session');
    expect(screen.queryByText(/^Session \d+$/)).not.toBeInTheDocument();
    // The per-item index is still shown.
    expect(screen.getByText('12.0')).toBeInTheDocument();
  });

  it('shows the estimated time and age on the divider when the session has one', () => {
    renderList(
      [makeItem({ session_index: 12, item_index: 0, estimated_time: Math.floor(Date.now() / 1000) - 120 })],
      'consensus'
    );
    const divider = screen.getByLabelText('Session 12');
    expect(divider.textContent).toMatch(/Session 12 · .+\(2m ago\)/);
  });
});
