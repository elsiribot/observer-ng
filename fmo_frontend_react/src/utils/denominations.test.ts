import { describe, it, expect } from 'vitest';
import type { MintDenomination } from '../types/api';
import {
  buildDenominationChartOption,
  formatDenominationLong,
  formatDenominationMsat,
} from './denominations';

describe('formatDenominationMsat', () => {
  it('shows raw msat below 1000', () => {
    expect(formatDenominationMsat(1)).toBe('1');
    expect(formatDenominationMsat(512)).toBe('512');
  });

  it('uses K/M/G suffixes above 1000 msat', () => {
    expect(formatDenominationMsat(1024)).toBe('1.02K');
    expect(formatDenominationMsat(2_000_000)).toBe('2.00M');
    expect(formatDenominationMsat(1_073_741_824)).toBe('1.07G');
  });
});

describe('formatDenominationLong', () => {
  it('gives exact msat plus the sat equivalent', () => {
    expect(formatDenominationLong(1024)).toBe('1,024 msat (1.024 sat)');
    expect(formatDenominationLong(500)).toBe('500 msat (0.5 sat)');
  });
});

describe('buildDenominationChartOption', () => {
  const data: MintDenomination[] = [
    { denomination_msat: 1000, issued: 30, in_circulation: 12 },
    { denomination_msat: 2000, issued: 20, in_circulation: 5 },
    { denomination_msat: 4000, issued: 8, in_circulation: 8 },
  ];

  it('maps denominations to the category axis in order', () => {
    const option = buildDenominationChartOption(data);
    expect(option.xAxis.data).toEqual(['1K', '2K', '4K']);
  });

  it('binds ever-issued to the left axis and in-circulation to the right', () => {
    const option = buildDenominationChartOption(data);
    const [issued, circulation] = option.series;

    expect(issued.name).toBe('Ever issued');
    expect(issued.yAxisIndex).toBe(0);
    expect(issued.data).toEqual([30, 20, 8]);

    expect(circulation.name).toBe('In circulation');
    expect(circulation.yAxisIndex).toBe(1);
    expect(circulation.data).toEqual([12, 5, 8]);

    // Two separate value axes, left + right.
    expect(option.yAxis).toHaveLength(2);
    expect(option.yAxis[0].position).toBe('left');
    expect(option.yAxis[1].position).toBe('right');
  });

  it('handles an empty dataset without throwing', () => {
    const option = buildDenominationChartOption([]);
    expect(option.xAxis.data).toEqual([]);
    expect(option.series[0].data).toEqual([]);
    expect(option.series[1].data).toEqual([]);
  });
});
