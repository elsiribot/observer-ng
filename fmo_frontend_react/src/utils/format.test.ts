import { describe, it, expect } from 'vitest';
import { asSats, humanizeSpread, formatEstimatedTime, formatTimestamp } from './format';

describe('asSats', () => {
  it('rounds to the nearest whole satoshi', () => {
    // 102_500 msat = 102.5 sats, rounds up to 103
    expect(asSats(102_500)).toBe('103 sats');
  });

  it('formats exactly one satoshi', () => {
    expect(asSats(1_000)).toBe('1 sats');
  });

  it('formats zero', () => {
    expect(asSats(0)).toBe('0 sats');
  });

  it('adds thousands separators for large values', () => {
    expect(asSats(1_234_567_000)).toBe('1,234,567 sats');
  });
});

describe('humanizeSpread', () => {
  it('renders sub-minute spreads in seconds', () => {
    expect(humanizeSpread(45)).toBe('45s');
  });

  it('renders minutes (e.g. 240 -> 4m)', () => {
    expect(humanizeSpread(240)).toBe('4m');
  });

  it('renders hours', () => {
    expect(humanizeSpread(7_200)).toBe('2h');
  });

  it('renders days', () => {
    expect(humanizeSpread(172_800)).toBe('2d');
  });
});

describe('formatEstimatedTime', () => {
  it('returns "unknown" when there is no time info', () => {
    const r = formatEstimatedTime({
      estimated_time: null,
      time_lower: null,
      time_upper: null,
      time_source: null,
    });
    expect(r.text).toBe('unknown');
    expect(r.title).toBeNull();
  });

  it('renders an observed item as the exact time with no spread', () => {
    const t = 1_700_000_000;
    const r = formatEstimatedTime({
      estimated_time: t,
      time_lower: t,
      time_upper: t,
      time_source: 'observed',
    });
    expect(r.text).toBe(formatTimestamp(t));
    expect(r.text).not.toContain('±');
    expect(r.title).toBeNull();
  });

  it('renders a voted (zero-width) item as the exact time', () => {
    const t = 1_700_000_000;
    const r = formatEstimatedTime({
      estimated_time: t,
      time_lower: t,
      time_upper: t,
      time_source: 'voted',
    });
    expect(r.text).toBe(formatTimestamp(t));
    expect(r.title).toBeNull();
  });

  it('renders an interpolated item as midpoint ± spread with a range title', () => {
    const lower = 1_700_000_000;
    const upper = lower + 480; // half-width 240s -> "4m"
    const mid = (lower + upper) / 2;
    const r = formatEstimatedTime({
      estimated_time: mid,
      time_lower: lower,
      time_upper: upper,
      time_source: 'interpolated',
    });
    expect(r.text).toBe(`≈ ${formatTimestamp(mid)} ·±4m`);
    expect(r.title).toBe(`${formatTimestamp(lower)} – ${formatTimestamp(upper)}`);
  });

  it('renders an unbounded interpolated item as "≳ lower"', () => {
    const lower = 1_700_000_000;
    const r = formatEstimatedTime({
      estimated_time: lower,
      time_lower: lower,
      time_upper: null,
      time_source: 'interpolated',
    });
    expect(r.text).toBe(`≳ ${formatTimestamp(lower)}`);
    expect(r.title).toBe(`after ${formatTimestamp(lower)}`);
  });
});
