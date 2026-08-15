import { describe, it, expect } from 'vitest';
import type { EcashAnonScatter } from '../types/api';
import { buildAnonScatterOption } from './anonScatter';

describe('buildAnonScatterOption', () => {
  const data: EcashAnonScatter = {
    points: [
      { t: 1_700_000_000, bits: 5.5 },
      { t: 1_700_086_400, bits: 7.2 },
    ],
    percentiles: [
      { t: 1_700_000_000, p10: 3, p50: 5, p90: 9 },
      { t: 1_700_086_400, p10: 4, p50: 6, p90: 10 },
    ],
  };

  it('maps points to a scatter series with [ms, bits] pairs', () => {
    const option = buildAnonScatterOption(data);
    const scatter = option.series[0];

    expect(scatter.type).toBe('scatter');
    expect(scatter.data).toEqual([
      [1_700_000_000_000, 5.5],
      [1_700_086_400_000, 7.2],
    ]);
  });

  it('maps percentiles to three line series (p10/p50/p90)', () => {
    const option = buildAnonScatterOption(data);
    const lineSeries = option.series.filter((s) => s.type === 'line');

    expect(lineSeries).toHaveLength(3);

    const [p10, p50, p90] = lineSeries;
    expect(p10.name).toBe('p10');
    expect(p10.data).toEqual([
      [1_700_000_000_000, 3],
      [1_700_086_400_000, 4],
    ]);

    expect(p50.name).toBe('p50 (median)');
    expect(p50.data).toEqual([
      [1_700_000_000_000, 5],
      [1_700_086_400_000, 6],
    ]);

    expect(p90.name).toBe('p90');
    expect(p90.data).toEqual([
      [1_700_000_000_000, 9],
      [1_700_086_400_000, 10],
    ]);
  });

  it('handles empty data without throwing', () => {
    const option = buildAnonScatterOption({ points: [], percentiles: [] });
    expect(option.series[0].data).toEqual([]);
    expect(option.series.filter((s) => s.type === 'line')).toHaveLength(3);
    for (const line of option.series.filter((s) => s.type === 'line')) {
      expect(line.data).toEqual([]);
    }
  });

  it('formats the Y-axis label as a crowd size, not raw bits', () => {
    const option = buildAnonScatterOption(data);
    expect(option.yAxis.axisLabel.formatter(10)).toBe('1.02k');
  });
});
