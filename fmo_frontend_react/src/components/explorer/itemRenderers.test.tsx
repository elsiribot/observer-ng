import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import { renderItem, TxDetailBody } from './itemRenderers';
import type { SessionItem, TxDetail, TxItemPart } from '../../types/api';

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
  estimated_time: null,
  role: null,
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

  it('shows the variant tag as a label above the raw-JSON fallback for an unknown variant', () => {
    const { container } = renderWithRouter({
      ...baseItem,
      kind: 'stability_pool',
      peer_id: 1,
      details: { SomeVariant: { foo: 'bar' } },
    });

    expect(screen.getByText('SomeVariant')).toBeInTheDocument();
    expect(container.querySelector('pre')).toBeInTheDocument();
  });

  it('links a DecryptPreimage contract id to its user-transaction page', () => {
    renderWithRouter({
      ...baseItem,
      kind: 'ln',
      details: { DecryptPreimage: ['contractid1234567890', 'other'] },
    });

    const link = screen.getByTitle('contractid1234567890');
    expect(link.tagName).toBe('A');
    expect(link).toHaveAttribute(
      'href',
      '/federations/fed1/user-transactions/contractid1234567890'
    );
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

describe('CopyButton', () => {
  it('copies the full txid to the clipboard when clicked', () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, 'clipboard', {
      value: { writeText },
      writable: true,
      configurable: true,
    });

    renderWithRouter({
      ...baseItem,
      item_type: 'transaction',
      txid: 'abc123txiddeadbeefreallylongtxid',
    });

    fireEvent.click(screen.getByRole('button', { name: /copy/i }));

    expect(writeText).toHaveBeenCalledWith('abc123txiddeadbeefreallylongtxid');
  });
});

describe('TxDetailBody', () => {
  const part = (index: number, kind: string, amount_msat: number | null): TxItemPart => ({
    index,
    kind,
    amount_msat,
    details: null,
  });

  function detail(inputs: TxItemPart[], outputs: TxItemPart[]): TxDetail {
    return { txid: 'tx1', session_index: 1, item_index: 0, inputs, outputs, user_tx_key: null };
  }

  it('shows total in / out / fee in sats (fee = inputs − outputs)', () => {
    render(
      <TxDetailBody
        detail={detail(
          [part(0, 'mint', 100_000), part(1, 'mint', 50_000)],
          [part(0, 'ln', 120_000)]
        )}
      />
    );
    expect(screen.getByText('Total in')).toBeInTheDocument();
    expect(screen.getByText('150 sats')).toBeInTheDocument(); // 150_000 msat
    expect(screen.getByText('120 sats')).toBeInTheDocument(); // 120_000 msat
    expect(screen.getByText('Fee')).toBeInTheDocument();
    expect(screen.getByText('30 sats')).toBeInTheDocument(); // 30_000 msat
  });

  it('marks totals approximate and fee unknown when an amount is null', () => {
    render(
      <TxDetailBody detail={detail([part(0, 'walletv2', null)], [part(0, 'mint', 40_000)])} />
    );
    // Input total unknown → approximate marker, and the fee can't be computed.
    expect(screen.getByText('≥ 0 sats')).toBeInTheDocument();
    expect(screen.getByText('unknown')).toBeInTheDocument();
  });

  it('renders loading and error states without a detail', () => {
    const { rerender } = render(<TxDetailBody detail={null} loading />);
    expect(screen.getByText('Loading…')).toBeInTheDocument();
    rerender(<TxDetailBody detail={null} error="boom" />);
    expect(screen.getByText('boom')).toBeInTheDocument();
  });
});
