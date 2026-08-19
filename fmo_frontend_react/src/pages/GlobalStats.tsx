import { useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { api } from '../services/api';
import type { FederationSummary } from '../types/api';
import { Totals } from '../components/Totals';
import { GlobalActivityChart } from '../components/GlobalActivityChart';
import { asBitcoin, formatNumber } from '../utils/format';

// Network-wide statistics page: headline totals, the fleet activity chart, and
// aggregates derived across all observed federations (assets, health, uptime,
// recent activity, leaderboards).
export function GlobalStats() {
  const [federations, setFederations] = useState<FederationSummary[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    api
      .getFederations()
      .then((data) => {
        if (!cancelled) setFederations(data);
      })
      .catch((err) => {
        if (!cancelled) setError(err instanceof Error ? err.message : 'Failed to load federations');
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const stats = useMemo(() => {
    if (!federations) return null;
    const totalAssets = federations.reduce((sum, f) => sum + f.deposits, 0);
    const health = { online: 0, degraded: 0, offline: 0 };
    for (const f of federations) health[f.health] += 1;
    const withUptime = federations.filter((f) => f.uptime_pct !== null);
    const avgUptime =
      withUptime.length > 0
        ? withUptime.reduce((s, f) => s + (f.uptime_pct as number), 0) / withUptime.length
        : null;
    const last7d = federations.reduce(
      (acc, f) => {
        for (const d of f.last_7d_activity) {
          acc.txs += d.num_transactions;
          acc.volume += d.amount_transferred;
        }
        return acc;
      },
      { txs: 0, volume: 0 }
    );
    const topByVolume = [...federations].sort((a, b) => b.total_volume - a.total_volume).slice(0, 5);
    const topByAssets = [...federations].sort((a, b) => b.deposits - a.deposits).slice(0, 5);
    return { totalAssets, health, avgUptime, last7d, topByVolume, topByAssets };
  }, [federations]);

  return (
    <div className="py-4 sm:py-8 px-4 sm:px-0">
      <h1 className="text-2xl sm:text-3xl font-bold text-gray-900 dark:text-white mb-6 sm:mb-8">
        Network statistics
      </h1>

      <div className="mb-8">
        <Totals />
      </div>

      <div className="mb-8">
        <GlobalActivityChart />
      </div>

      {error && <div className="py-6 text-center text-sm text-red-500">Error: {error}</div>}

      {stats && (
        <>
          <div className="mb-8 grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
            <Stat label="Total assets" value={asBitcoin(stats.totalAssets, 4)} />
            <Stat label="Online" value={formatNumber(stats.health.online)} tone="online" />
            <Stat label="Degraded" value={formatNumber(stats.health.degraded)} tone="degraded" />
            <Stat label="Offline" value={formatNumber(stats.health.offline)} tone="offline" />
            <Stat
              label="Avg uptime (30d)"
              value={stats.avgUptime !== null ? `${stats.avgUptime.toFixed(1)}%` : '—'}
            />
            <Stat label="Txs (7d)" value={formatNumber(stats.last7d.txs)} />
          </div>

          <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
            <Leaderboard
              title="Top federations by volume (all-time)"
              rows={stats.topByVolume.map((f) => ({
                id: f.id,
                name: f.name || 'Unnamed',
                value: asBitcoin(f.total_volume, 3),
              }))}
            />
            <Leaderboard
              title="Top federations by assets"
              rows={stats.topByAssets.map((f) => ({
                id: f.id,
                name: f.name || 'Unnamed',
                value: asBitcoin(f.deposits, 3),
              }))}
            />
          </div>
        </>
      )}
    </div>
  );
}

const TONE: Record<string, string> = {
  online: 'text-emerald-600 dark:text-emerald-400',
  degraded: 'text-yellow-600 dark:text-yellow-400',
  offline: 'text-red-600 dark:text-red-400',
};

function Stat({ label, value, tone }: { label: string; value: string; tone?: string }) {
  return (
    <div className="rounded-lg border border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-900/40 px-3 py-2">
      <div className="text-[10px] uppercase tracking-wide text-gray-500 dark:text-gray-400">
        {label}
      </div>
      <div
        className={`text-lg font-semibold tabular-nums ${
          tone ? TONE[tone] : 'text-gray-900 dark:text-white'
        }`}
      >
        {value}
      </div>
    </div>
  );
}

function Leaderboard({
  title,
  rows,
}: {
  title: string;
  rows: { id: string; name: string; value: string }[];
}) {
  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow-md border border-gray-200 dark:border-gray-700 p-4 sm:p-6">
      <h2 className="mb-3 text-sm font-semibold text-gray-900 dark:text-white">{title}</h2>
      {rows.length === 0 ? (
        <div className="text-sm text-gray-500 dark:text-gray-400">No data</div>
      ) : (
        <ol className="space-y-1.5">
          {rows.map((r, i) => (
            <li key={r.id} className="flex items-center gap-2 text-sm">
              <span className="w-4 text-right text-xs text-gray-400 dark:text-gray-500">
                {i + 1}
              </span>
              <Link
                to={`/federations/${r.id}`}
                className="truncate text-blue-600 dark:text-blue-400 hover:underline"
              >
                {r.name}
              </Link>
              <span className="ml-auto tabular-nums text-gray-900 dark:text-white">{r.value}</span>
            </li>
          ))}
        </ol>
      )}
    </div>
  );
}
