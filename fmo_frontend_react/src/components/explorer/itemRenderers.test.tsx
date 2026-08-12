import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import { renderItem } from './itemRenderers';
import type { SessionItem } from '../../types/api';

function renderWithRouter(item: SessionItem) {
  return render(
    <MemoryRouter initialEntries={['/federations/fed1']}>
      <Routes>
        <Route path="/federations/:id" element={<>{renderItem(item)}</>} />
      </Routes>
    </MemoryRouter>
  );
}

const baseItem: SessionItem = {
  session_index: 1,
  item_index: 0,
  item_type: 'ci',
  kind: null,
  peer_id: null,
  txid: null,
  user_tx_key: null,
  user_tx_kind: null,
  direction: null,
  details: null,
};

describe('renderItem', () => {
  it('renders a transaction item with a classification badge and the user-tx link', () => {
    renderWithRouter({
      ...baseItem,
      item_type: 'transaction',
      txid: 'abc123txiddeadbeef',
      user_tx_key: 'deadbeefcafe',
    });

    expect(screen.getByText('Transaction')).toBeInTheDocument();
    const link = screen.getByRole('link', { name: /part of user transaction/i });
    expect(link).toHaveAttribute('href', '/federations/fed1/user-transactions/deadbeefcafe');
  });

  it('renders the gold-layer classification badge for a known user_tx_kind', () => {
    renderWithRouter({
      ...baseItem,
      item_type: 'transaction',
      txid: 'abc123txiddeadbeef',
      user_tx_key: 'deadbeefcafe',
      user_tx_kind: 'ln_send',
      direction: 'out',
    });

    expect(screen.getByText('LN Send')).toBeInTheDocument();
    expect(screen.queryByText('Transaction')).not.toBeInTheDocument();
  });

  it('falls back to a generic "Transaction" badge when user_tx_kind is null', () => {
    expect(() =>
      renderWithRouter({
        ...baseItem,
        item_type: 'transaction',
        txid: 'abc123txiddeadbeef',
        user_tx_key: null,
        user_tx_kind: null,
      })
    ).not.toThrow();

    expect(screen.getByText('Transaction')).toBeInTheDocument();
  });

  it('does not render the user-tx link when user_tx_key is absent', () => {
    renderWithRouter({
      ...baseItem,
      item_type: 'transaction',
      txid: 'abc123txiddeadbeef',
      user_tx_key: null,
    });

    expect(screen.queryByRole('link', { name: /part of user transaction/i })).not.toBeInTheDocument();
  });

  it('renders a friendly summary for an ln consensus item', () => {
    renderWithRouter({
      ...baseItem,
      kind: 'ln',
      peer_id: 2,
      details: { BlockCount: 945696 },
    });

    expect(screen.getByText(/block count vote/i)).toBeInTheDocument();
    expect(screen.getByText('945,696')).toBeInTheDocument();
    expect(screen.getByText('Guardian 2')).toBeInTheDocument();
    expect(document.querySelector('pre')).not.toBeInTheDocument();
  });

  it('renders a friendly summary for a wallet consensus item', () => {
    renderWithRouter({
      ...baseItem,
      kind: 'wallet',
      peer_id: 0,
      details: { Feerate: { sats_per_kvb: 10000 } },
    });

    expect(screen.getByText(/fee rate vote/i)).toBeInTheDocument();
  });

  it('renders the raw-JSON fallback for an unhandled/undecoded kind', () => {
    const { container } = renderWithRouter({
      ...baseItem,
      kind: 'stability_pool',
      peer_id: 1,
      details: { SomeVariant: { foo: 'bar' } },
    });

    const pre = container.querySelector('pre');
    expect(pre).toBeInTheDocument();
    expect(pre?.textContent).toContain('SomeVariant');
  });

  it('never throws on an unexpected details shape for a known kind', () => {
    expect(() =>
      renderWithRouter({
        ...baseItem,
        kind: 'ln',
        details: { BlockCount: 'not-a-number' },
      })
    ).not.toThrow();
  });
});
