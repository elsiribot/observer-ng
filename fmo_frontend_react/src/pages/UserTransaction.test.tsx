import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import { UserTransaction } from './UserTransaction';
import { api } from '../services/api';
import type { UserTransaction as UserTransactionData } from '../types/api';

vi.mock('../services/api', () => ({
  api: {
    getUserTransaction: vi.fn(),
  },
}));

function renderPage(federationId = 'fed1', key = 'deadbeef') {
  return render(
    <MemoryRouter initialEntries={[`/federations/${federationId}/user-transactions/${key}`]}>
      <Routes>
        <Route path="/federations/:id/user-transactions/:key" element={<UserTransaction />} />
      </Routes>
    </MemoryRouter>
  );
}

function makeUserTx(overrides: Partial<UserTransactionData> = {}): UserTransactionData {
  return {
    kind: 'ln_receive',
    direction: 'in',
    amount_msat: 990_000,
    fedimint_fee_msat: 10_000,
    gateway_fee_estimate_msat: null,
    num_fedimint_txs: 3,
    first_timestamp: 1_700_000_000,
    last_timestamp: 1_700_000_100,
    member_txs: [
      { txid: 'tx_offer_deadbeef', role: 'offer', session_index: 0 },
      { txid: 'tx_fund_deadbeef', role: 'fund', session_index: 1 },
      { txid: 'tx_claim_deadbeef', role: 'claim', session_index: 2 },
    ],
    ...overrides,
  };
}

describe('UserTransaction page', () => {
  beforeEach(() => {
    vi.mocked(api.getUserTransaction).mockReset();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('renders the gold summary (kind, direction, amount, fees, count)', async () => {
    vi.mocked(api.getUserTransaction).mockResolvedValueOnce(makeUserTx());

    renderPage();

    expect(await screen.findByText('LN Receive')).toBeInTheDocument();
    expect(screen.getByText('in')).toBeInTheDocument();
    expect(screen.getByText('0.000010 BTC')).toBeInTheDocument(); // amount 990_000 msat
    expect(screen.getByText('0.000000 BTC')).toBeInTheDocument(); // fee 10_000 msat
    expect(screen.getByText('n/a')).toBeInTheDocument(); // gateway fee estimate
    expect(screen.getByText('3')).toBeInTheDocument(); // num_fedimint_txs

    expect(api.getUserTransaction).toHaveBeenCalledWith('fed1', 'deadbeef');
  });

  it('renders member-tx rows with role badges linking to the correct tx-detail hrefs', async () => {
    vi.mocked(api.getUserTransaction).mockResolvedValueOnce(makeUserTx());

    renderPage();

    await screen.findByText('LN Receive');

    expect(screen.getByText('offer')).toBeInTheDocument();
    expect(screen.getByText('fund')).toBeInTheDocument();
    expect(screen.getByText('claim')).toBeInTheDocument();

    const offerLink = screen.getByRole('link', { name: /tx_offer_/i });
    expect(offerLink).toHaveAttribute('href', '/federations/fed1/tx/tx_offer_deadbeef');

    const fundLink = screen.getByRole('link', { name: /tx_fund_/i });
    expect(fundLink).toHaveAttribute('href', '/federations/fed1/tx/tx_fund_deadbeef');

    const claimLink = screen.getByRole('link', { name: /tx_claim_/i });
    expect(claimLink).toHaveAttribute('href', '/federations/fed1/tx/tx_claim_deadbeef');
  });

  it('shows an error state when the fetch fails', async () => {
    vi.mocked(api.getUserTransaction).mockRejectedValueOnce(new Error('boom'));

    renderPage();

    expect(await screen.findByText('boom')).toBeInTheDocument();
  });

  it('renders an empty-state message when there are no member transactions', async () => {
    vi.mocked(api.getUserTransaction).mockResolvedValueOnce(makeUserTx({ member_txs: [] }));

    renderPage();

    expect(await screen.findByText('No member transactions')).toBeInTheDocument();
  });
});
