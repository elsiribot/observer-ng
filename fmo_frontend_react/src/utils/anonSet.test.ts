import { describe, it, expect } from 'vitest';
import { formatAnonSet } from './anonSet';

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
