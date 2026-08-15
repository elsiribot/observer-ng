import { describe, it, expect } from 'vitest';
import { formatAnonSet, formatAnonSetCount, formatSi } from './anonSet';

describe('formatAnonSet', () => {
  it('renders bits and the note-count lower bound', () => {
    expect(formatAnonSet(4)).toBe('≈ 4.0 bits (≥ 16 notes)');
    expect(formatAnonSet(10)).toBe('≈ 10.0 bits (≥ 1,024 notes)');
  });
  it('returns null when not applicable', () => {
    expect(formatAnonSet(null)).toBeNull();
  });
  it('flags a dangerously small set', () => {
    expect(formatAnonSet(0)).toBe('≈ 0.0 bits (≥ 1 notes)');
  });
});

describe('formatSi', () => {
  it('shows small numbers as-is', () => {
    expect(formatSi(8)).toBe('8');
    expect(formatSi(512)).toBe('512');
    expect(formatSi(999)).toBe('999');
  });
  it('uses SI suffixes with 3 significant figures', () => {
    expect(formatSi(2048)).toBe('2.05k');
    expect(formatSi(16384)).toBe('16.4k');
    expect(formatSi(131072)).toBe('131k');
    expect(formatSi(1048576)).toBe('1.05M');
    expect(formatSi(1073741824)).toBe('1.07G');
  });
  it('does not strip trailing zeros from integer results', () => {
    expect(formatSi(150000)).toBe('150k');
  });
});

describe('formatAnonSetCount', () => {
  it('rounds bits down to the next whole bit and shows the crowd size', () => {
    // 11.87 -> floor 11 -> 2^11 = 2048 -> "2.05k"
    expect(formatAnonSetCount(11.87)).toBe('2.05k');
    // 14.75 -> floor 14 -> 16384 -> "16.4k"
    expect(formatAnonSetCount(14.75)).toBe('16.4k');
    // 4.0 -> 16
    expect(formatAnonSetCount(4)).toBe('16');
  });
  it('returns null when not applicable', () => {
    expect(formatAnonSetCount(null)).toBeNull();
  });
});
