import { useEffect, useState } from 'react';
import { api } from '../services/api';
import type { GuardianTimeline as GuardianTimelineData, TimeInterval } from '../types/api';
import { formatTimestamp } from '../utils/format';

/// Position of an interval within the window, as percentages across the shared
/// time axis. Both are clamped to [0, 100] so intervals that straddle the
/// window edges (e.g. an outage that began before `windowStart`, or is still
/// ongoing at `windowEnd`) render inside the track. Returns `widthPct === 0`
/// for an interval that lies entirely outside the window.
export function intervalLayout(
  interval: TimeInterval,
  windowStart: number,
  windowEnd: number
): { leftPct: number; widthPct: number } {
  const span = windowEnd - windowStart;
  if (span <= 0) {
    return { leftPct: 0, widthPct: 0 };
  }
  const clampedStart = Math.min(Math.max(interval.start, windowStart), windowEnd);
  const clampedEnd = Math.min(Math.max(interval.end, windowStart), windowEnd);
  const leftPct = ((clampedStart - windowStart) / span) * 100;
  const widthPct = Math.max(0, ((clampedEnd - clampedStart) / span) * 100);
  return { leftPct, widthPct };
}

/// Compact "Xh Ym"/"Xd Yh"/"Xm Ys" duration for tooltips, showing the two
/// largest non-zero units.
export function formatDuration(seconds: number): string {
  const s = Math.max(0, Math.round(seconds));
  if (s < 60) {
    return `${s}s`;
  }
  const d = Math.floor(s / 86400);
  const h = Math.floor((s % 86400) / 3600);
  const m = Math.floor((s % 3600) / 60);
  if (d > 0) {
    return h > 0 ? `${d}d ${h}h` : `${d}d`;
  }
  if (h > 0) {
    return m > 0 ? `${h}h ${m}m` : `${h}h`;
  }
  return `${m}m`;
}

// Tooltip text for an interval bar: "<start> – <end> (<duration>)".
function intervalTooltip(interval: TimeInterval, label: string): string {
  return `${label}: ${formatTimestamp(interval.start)} – ${formatTimestamp(interval.end)} (${formatDuration(
    interval.end - interval.start
  )})`;
}

const WINDOWS: { label: string; value: string }[] = [
  { label: '7 days', value: '7d' },
  { label: '30 days', value: '30d' },
];

// A few evenly spaced axis ticks across the window.
const TICK_COUNT = 4;

// Height of one guardian lane, in pixels (kept in JS so the inoperable overlay
// and axis can align exactly).
const LANE_HEIGHT = 28;

interface GuardianTimelineProps {
  federationId: string;
}

export function GuardianTimeline({ federationId }: GuardianTimelineProps) {
  const [window, setWindow] = useState('30d');
  // Filtering of transient single-poll false positives is opt-out: on by
  // default, unchecking shows the raw samples (every blip as an interval).
  const [filterBlips, setFilterBlips] = useState(true);
  const [data, setData] = useState<GuardianTimelineData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    api
      .getGuardianTimeline(federationId, window, filterBlips)
      .then((timeline) => {
        if (!cancelled) {
          setData(timeline);
          setLoading(false);
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : 'Failed to load timeline');
          setLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [federationId, window, filterBlips]);

  const windowSelector = (
    <div className="flex gap-1" role="group" aria-label="Timeline window">
      {WINDOWS.map((w) => (
        <button
          key={w.value}
          onClick={() => setWindow(w.value)}
          className={`px-2.5 py-1 text-xs rounded-md border ${
            window === w.value
              ? 'bg-blue-600 border-blue-600 text-white'
              : 'bg-white dark:bg-gray-700 border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-200 hover:bg-gray-50 dark:hover:bg-gray-600'
          }`}
        >
          {w.label}
        </button>
      ))}
    </div>
  );

  const header = (
    <div className="flex items-center justify-between gap-3 mb-3 flex-wrap">
      <div className="text-xs sm:text-sm text-gray-500 dark:text-gray-400">
        {data && (
          <>
            Consensus needs{' '}
            <span className="font-medium text-gray-700 dark:text-gray-200">
              {data.threshold} of {data.num_guardians}
            </span>{' '}
            guardians online
          </>
        )}
      </div>
      <div className="flex items-center gap-3">
        <label
          className="flex items-center gap-1.5 text-xs text-gray-500 dark:text-gray-400 cursor-pointer select-none"
          title="Hide transient single-poll blips (a lone missed or lagging poll bracketed by healthy ones). Uncheck to see raw samples."
        >
          <input
            type="checkbox"
            className="rounded border-gray-300 dark:border-gray-600"
            checked={filterBlips}
            onChange={(e) => setFilterBlips(e.target.checked)}
          />
          Filter transient blips
        </label>
        {windowSelector}
      </div>
    </div>
  );

  if (loading) {
    return (
      <div>
        {header}
        <div className="py-10 text-center text-sm text-gray-500 dark:text-gray-400">
          Loading timeline…
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div>
        {header}
        <div className="py-10 text-center text-sm text-red-500">Error: {error}</div>
      </div>
    );
  }

  if (!data || data.guardians.length === 0) {
    return (
      <div>
        {header}
        <div className="py-10 text-center text-sm text-gray-500 dark:text-gray-400">
          No guardian health data available
        </div>
      </div>
    );
  }

  const { window_start, window_end, guardians, inoperable_intervals } = data;
  const span = Math.max(1, window_end - window_start);

  const totalOutages =
    inoperable_intervals.length +
    guardians.reduce(
      (n, g) => n + g.offline_intervals.length + g.lagging_intervals.length,
      0
    );

  const ticks = Array.from({ length: TICK_COUNT + 1 }, (_, i) => {
    const fraction = i / TICK_COUNT;
    return {
      leftPct: fraction * 100,
      time: window_start + fraction * span,
    };
  });

  const lanesHeight = guardians.length * LANE_HEIGHT;

  return (
    <div>
      {header}

      {/* Legend */}
      <div className="flex items-center gap-4 mb-3 text-xs text-gray-500 dark:text-gray-400 flex-wrap">
        <span className="flex items-center gap-1.5">
          <span className="inline-block w-3 h-3 rounded-sm bg-red-500" />
          Guardian offline
        </span>
        <span className="flex items-center gap-1.5">
          <span className="inline-block w-3 h-3 rounded-sm bg-amber-400" />
          Guardian lagging (behind on consensus)
        </span>
        <span className="flex items-center gap-1.5">
          <span className="inline-block w-3 h-3 rounded-sm bg-orange-500/30 border border-orange-500/60" />
          Federation inoperable (quorum lost)
        </span>
      </div>

      <div className="flex">
        {/* Guardian name labels (outside the time track so they don't scale) */}
        <div className="shrink-0 pr-3 w-24 sm:w-36">
          {guardians.map((g) => (
            <div
              key={g.guardian_id}
              className="flex items-center text-xs sm:text-sm text-gray-700 dark:text-gray-200 truncate"
              style={{ height: LANE_HEIGHT }}
              title={g.name}
            >
              <span className="truncate">{g.name}</span>
            </div>
          ))}
          {/* spacer aligning the label column with the axis row */}
          <div style={{ height: 20 }} />
        </div>

        {/* Shared time track */}
        <div className="relative flex-1 min-w-0">
          <div className="relative" style={{ height: lanesHeight }}>
            {/* Inoperable bands span all lanes, painted behind the lane bars. */}
            <div className="absolute inset-0 pointer-events-none">
              {inoperable_intervals.map((iv, i) => {
                const { leftPct, widthPct } = intervalLayout(iv, window_start, window_end);
                if (widthPct <= 0) return null;
                return (
                  <div
                    key={`inop-${i}`}
                    className="absolute inset-y-0 bg-orange-500/25 border-x border-orange-500/50 pointer-events-auto"
                    style={{ left: `${leftPct}%`, width: `${Math.max(widthPct, 0.4)}%` }}
                    title={intervalTooltip(iv, 'Inoperable')}
                  />
                );
              })}
            </div>

            {/* One lane per guardian. */}
            {guardians.map((g) => (
              <div
                key={g.guardian_id}
                className="relative border-b border-gray-100 dark:border-gray-700/50 bg-gray-50 dark:bg-gray-900/40"
                style={{ height: LANE_HEIGHT }}
              >
                {g.lagging_intervals.map((iv, i) => {
                  const { leftPct, widthPct } = intervalLayout(iv, window_start, window_end);
                  if (widthPct <= 0) return null;
                  return (
                    <div
                      key={`lag-${i}`}
                      className="absolute top-1 bottom-1 bg-amber-400 hover:bg-amber-300 rounded-sm"
                      style={{ left: `${leftPct}%`, width: `${Math.max(widthPct, 0.4)}%` }}
                      title={intervalTooltip(iv, `${g.name} (lagging)`)}
                    />
                  );
                })}
                {g.offline_intervals.map((iv, i) => {
                  const { leftPct, widthPct } = intervalLayout(iv, window_start, window_end);
                  if (widthPct <= 0) return null;
                  return (
                    <div
                      key={`off-${i}`}
                      className="absolute top-1 bottom-1 bg-red-500 hover:bg-red-400 rounded-sm"
                      style={{ left: `${leftPct}%`, width: `${Math.max(widthPct, 0.4)}%` }}
                      title={intervalTooltip(iv, g.name)}
                    />
                  );
                })}
              </div>
            ))}
          </div>

          {/* Time axis with a few tick labels. */}
          <div className="relative" style={{ height: 20 }}>
            {ticks.map((tick, i) => (
              <div
                key={i}
                className="absolute top-0 text-[10px] text-gray-400 dark:text-gray-500 whitespace-nowrap"
                style={{
                  left: `${tick.leftPct}%`,
                  transform:
                    i === 0
                      ? 'translateX(0)'
                      : i === ticks.length - 1
                        ? 'translateX(-100%)'
                        : 'translateX(-50%)',
                }}
                title={formatTimestamp(tick.time)}
              >
                {new Date(tick.time * 1000).toLocaleDateString()}
              </div>
            ))}
          </div>
        </div>
      </div>

      {totalOutages === 0 && (
        <div className="mt-3 text-center text-xs sm:text-sm text-gray-500 dark:text-gray-400">
          No outages recorded in this window
        </div>
      )}
    </div>
  );
}
