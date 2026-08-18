import { describe, expect, it } from 'vitest';
import type { SpAccountTx, SpSeriesPoint } from '../types/api';
import {
  buildAccountNetOption,
  buildNetFlowOption,
  buildPriceOption,
  signedFiatDelta,
} from './spCharts';

function tx(partial: Partial<SpAccountTx>): SpAccountTx {
  return {
    tx_key: 'k',
    kind: 'deposit_seek',
    direction: 'in',
    amount_msat: null,
    fiat_amount: 0,
    fiat_is_target: false,
    cycle_index: null,
    cycle_price_fiat: null,
    session_index: 0,
    timestamp: null,
    primary_txid: 'ab',
    secondary_txid: null,
    counterparty: null,
    ...partial,
  };
}

describe('signedFiatDelta', () => {
  it('adds deposits and incoming transfers, subtracts withdrawals and outgoing transfers', () => {
    expect(signedFiatDelta(tx({ kind: 'deposit_seek', fiat_amount: 100 }))).toBe(100);
    expect(signedFiatDelta(tx({ kind: 'deposit_provide', fiat_amount: 50 }))).toBe(50);
    expect(signedFiatDelta(tx({ kind: 'transfer_in', fiat_amount: 25 }))).toBe(25);
    expect(signedFiatDelta(tx({ kind: 'withdraw', fiat_amount: 40 }))).toBe(-40);
    expect(signedFiatDelta(tx({ kind: 'transfer_out', fiat_amount: 10 }))).toBe(-10);
  });

  it('treats a null fiat amount as zero', () => {
    expect(signedFiatDelta(tx({ kind: 'withdraw', fiat_amount: null }))).toBe(0);
  });
});

describe('buildPriceOption', () => {
  it('drops points without a start time and converts base units to dollars', () => {
    const series: SpSeriesPoint[] = [
      { cycle_index: 1, start_time: 1_700_000_000, price_fiat: 6_000_000_000, cumulative_msat: null, cumulative_fiat: null },
      { cycle_index: 2, start_time: null, price_fiat: 6_100_000_000, cumulative_msat: null, cumulative_fiat: null },
    ];
    const opt = buildPriceOption(series);
    expect(opt.series[0].data).toEqual([[1_700_000_000_000, 60_000_000]]);
  });
});

describe('buildNetFlowOption', () => {
  it('keeps only cycles with a cumulative value', () => {
    const series: SpSeriesPoint[] = [
      { cycle_index: 1, start_time: 1_700_000_000, price_fiat: 1, cumulative_msat: 5, cumulative_fiat: 2_100_000_000 },
      { cycle_index: 2, start_time: 1_700_003_600, price_fiat: 1, cumulative_msat: null, cumulative_fiat: null },
    ];
    const opt = buildNetFlowOption(series);
    expect(opt.series[0].data).toEqual([[1_700_000_000_000, 21_000_000]]);
  });
});

describe('buildAccountNetOption', () => {
  it('accumulates signed deltas in session order and emits timestamped points', () => {
    const txs: SpAccountTx[] = [
      tx({ kind: 'withdraw', fiat_amount: 1_200_000_000, session_index: 3, timestamp: 300 }),
      tx({ kind: 'deposit_seek', fiat_amount: 3_000_000_000, session_index: 1, timestamp: 100 }),
    ];
    const opt = buildAccountNetOption(txs);
    // session 1 (+30M) then session 3 (−12M) → 30M, then 18M (base units /100).
    expect(opt.series[0].data).toEqual([
      [100_000, 30_000_000],
      [300_000, 18_000_000],
    ]);
  });
});
