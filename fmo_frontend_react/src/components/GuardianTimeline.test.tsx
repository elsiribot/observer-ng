import { describe, it, expect } from 'vitest';
import { intervalLayout, formatDuration } from './GuardianTimeline';

const WINDOW_START = 1_000_000;
const WINDOW_END = 1_000_000 + 1000; // span of 1000s

describe('intervalLayout', () => {
  it('maps an interval fully inside the window to left/width percentages', () => {
    // [start=+250, end=+750) of a 1000s window => left 25%, width 50%.
    const { leftPct, widthPct } = intervalLayout(
      { start: WINDOW_START + 250, end: WINDOW_START + 750 },
      WINDOW_START,
      WINDOW_END
    );
    expect(leftPct).toBeCloseTo(25);
    expect(widthPct).toBeCloseTo(50);
  });

  it('places an interval at the very start of the window at 0%', () => {
    const { leftPct, widthPct } = intervalLayout(
      { start: WINDOW_START, end: WINDOW_START + 100 },
      WINDOW_START,
      WINDOW_END
    );
    expect(leftPct).toBeCloseTo(0);
    expect(widthPct).toBeCloseTo(10);
  });

  it('clamps an interval that began before the window to left 0%', () => {
    // Starts 500s before the window, ends 300s in => visible part is [0, 300).
    const { leftPct, widthPct } = intervalLayout(
      { start: WINDOW_START - 500, end: WINDOW_START + 300 },
      WINDOW_START,
      WINDOW_END
    );
    expect(leftPct).toBeCloseTo(0);
    expect(widthPct).toBeCloseTo(30);
  });

  it('clamps an ongoing interval that extends past window_end to reach 100%', () => {
    // Starts at +800, ends far past the window => visible [80%, 100%].
    const { leftPct, widthPct } = intervalLayout(
      { start: WINDOW_START + 800, end: WINDOW_END + 100_000 },
      WINDOW_START,
      WINDOW_END
    );
    expect(leftPct).toBeCloseTo(80);
    expect(widthPct).toBeCloseTo(20);
  });

  it('returns zero width for an interval entirely outside the window', () => {
    const { widthPct } = intervalLayout(
      { start: WINDOW_END + 10, end: WINDOW_END + 20 },
      WINDOW_START,
      WINDOW_END
    );
    expect(widthPct).toBe(0);
  });

  it('returns zero width for a zero/negative-span window', () => {
    const { leftPct, widthPct } = intervalLayout(
      { start: WINDOW_START, end: WINDOW_START + 100 },
      WINDOW_START,
      WINDOW_START
    );
    expect(leftPct).toBe(0);
    expect(widthPct).toBe(0);
  });
});

describe('formatDuration', () => {
  it('formats sub-minute durations in seconds', () => {
    expect(formatDuration(45)).toBe('45s');
  });

  it('formats minutes and seconds', () => {
    expect(formatDuration(90)).toBe('1m');
    expect(formatDuration(3599)).toBe('59m');
  });

  it('formats hours and minutes', () => {
    expect(formatDuration(3600)).toBe('1h');
    expect(formatDuration(3600 + 12 * 60)).toBe('1h 12m');
  });

  it('formats days and hours', () => {
    expect(formatDuration(86400)).toBe('1d');
    expect(formatDuration(86400 + 5 * 3600)).toBe('1d 5h');
  });
});
