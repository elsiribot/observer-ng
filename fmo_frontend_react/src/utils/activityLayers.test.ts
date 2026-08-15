import { describe, it, expect } from 'vitest';
import {
  kindToLayer,
  buildStackedSeries,
  ACTIVITY_LAYERS,
  type ActivityByDay,
} from './activityLayers';

describe('kindToLayer', () => {
  it('collapses v1/v2 and send/receive variants onto shared layers', () => {
    expect(kindToLayer('peg_in')).toBe('peg_in');
    expect(kindToLayer('peg_in_v2')).toBe('peg_in');
    expect(kindToLayer('peg_out_v2')).toBe('peg_out');
    expect(kindToLayer('ecash_transfer')).toBe('ecash');
    expect(kindToLayer('ecash_transfer_v2')).toBe('ecash');
    expect(kindToLayer('ln_send')).toBe('lightning');
    expect(kindToLayer('lnv2_receive')).toBe('lightning');
    expect(kindToLayer('lightning')).toBe('lightning');
    expect(kindToLayer('stability_pool')).toBe('stability_pool');
  });

  it('falls back to other for unknown kinds', () => {
    expect(kindToLayer('other')).toBe('other');
    expect(kindToLayer('some_future_kind')).toBe('other');
  });
});

describe('buildStackedSeries', () => {
  const byDay: ActivityByDay = {
    '2024-05-31': {
      peg_in: { num_transactions: 2, amount_transferred: 100_000_000_000 }, // 1 BTC
      ln_send: { num_transactions: 3, amount_transferred: 50_000_000_000 }, // 0.5 BTC
    },
    '2024-06-01': {
      peg_in_v2: { num_transactions: 1, amount_transferred: 200_000_000_000 }, // 2 BTC
      ecash_transfer: { num_transactions: 5, amount_transferred: 0 },
    },
  };

  it('orders days ascending and exposes iso + timestamps', () => {
    const s = buildStackedSeries(byDay, 'count');
    expect(s.isoDates).toEqual(['2024-05-31', '2024-06-01']);
    expect(s.dates).toHaveLength(2);
    expect(s.timestamps[0]).toBe(new Date('2024-05-31').getTime());
  });

  it('sums count per layer per day, merging kind variants', () => {
    const s = buildStackedSeries(byDay, 'count');
    const pegIn = s.series.find((x) => x.key === 'peg_in');
    const lightning = s.series.find((x) => x.key === 'lightning');
    const ecash = s.series.find((x) => x.key === 'ecash');
    // peg_in (day0) + peg_in_v2 (day1) collapse to one layer
    expect(pegIn?.data).toEqual([2, 1]);
    expect(lightning?.data).toEqual([3, 0]);
    expect(ecash?.data).toEqual([0, 5]);
  });

  it('converts volume from millisats to BTC', () => {
    const s = buildStackedSeries(byDay, 'volume');
    const pegIn = s.series.find((x) => x.key === 'peg_in');
    expect(pegIn?.data).toEqual([1, 2]);
  });

  it('omits layers with no nonzero values across the range', () => {
    const s = buildStackedSeries(byDay, 'count');
    expect(s.series.some((x) => x.key === 'peg_out')).toBe(false);
    expect(s.series.some((x) => x.key === 'stability_pool')).toBe(false);
  });

  it('keeps a zero-volume-but-nonzero-count layer under volume only if it has volume', () => {
    // ecash has count 5 but volume 0 -> dropped under volume, kept under count.
    expect(buildStackedSeries(byDay, 'volume').series.some((x) => x.key === 'ecash')).toBe(false);
    expect(buildStackedSeries(byDay, 'count').series.some((x) => x.key === 'ecash')).toBe(true);
  });

  it('returns series in ACTIVITY_LAYERS order', () => {
    const s = buildStackedSeries(byDay, 'count');
    const order = s.series.map((x) => x.key);
    const expectedOrder = ACTIVITY_LAYERS.map((l) => l.key).filter((k) => order.includes(k));
    expect(order).toEqual(expectedOrder);
  });

  it('handles an empty response', () => {
    const s = buildStackedSeries({}, 'count');
    expect(s.dates).toEqual([]);
    expect(s.series).toEqual([]);
  });
});
