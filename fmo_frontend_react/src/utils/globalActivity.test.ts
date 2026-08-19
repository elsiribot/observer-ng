import { describe, expect, it } from 'vitest';
import type { GlobalActivityPoint } from '../types/api';
import { buildGlobalActivityOption } from './globalActivity';

describe('buildGlobalActivityOption', () => {
  const points: GlobalActivityPoint[] = [
    { date: '2026-08-17', tx_count: 10, volume_msat: 100_000_000_000 },
    { date: '2026-08-18', tx_count: 25, volume_msat: 250_000_000_000 },
  ];

  it('maps volume to BTC bars and tx count to a line, keyed by date', () => {
    const opt = buildGlobalActivityOption(points);
    expect(opt.xAxis.data).toEqual(['2026-08-17', '2026-08-18']);
    const [volume, count] = opt.series;
    expect(volume.type).toBe('bar');
    expect(volume.data).toEqual([1, 2.5]); // msat / 1e11
    expect(count.type).toBe('line');
    expect(count.yAxisIndex).toBe(1);
    expect(count.data).toEqual([10, 25]);
  });

  it('handles an empty series without throwing', () => {
    const opt = buildGlobalActivityOption([]);
    expect(opt.xAxis.data).toEqual([]);
    expect(opt.series[0].data).toEqual([]);
  });
});
