import { describe, it, expect } from 'vitest';
import { asSats } from './format';

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
