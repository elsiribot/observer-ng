import { useEffect, useState } from 'react';
import { api } from '../services/api';
import type { SpAccount, SpSeriesPoint, SpSummary } from '../types/api';
import { asBitcoin, formatFiat, formatNumber, formatTimestamp } from '../utils/format';
import { accTypeLabel, multisigLabel } from '../utils/sp';
import { Badge } from './Badge';
import { AccountLink } from './explorer/itemRenderers';
import { SpNetFlowChart, SpPriceChart } from './StabilityPoolCharts';

interface Props {
  federationId: string;
}

type Order = 'net' | 'activity' | 'recent';
const PAGE_SIZE = 25;

// Stability Pool tab: federation-wide summary + price / net-flow charts + a
// sortable, paginated accounts table. Every account links to its detail page.
// All fiat is the federation's stable-currency base unit (cents), shown as USD.
export function StabilityPoolTab({ federationId }: Props) {
  const [summary, setSummary] = useState<SpSummary | null>(null);
  const [series, setSeries] = useState<SpSeriesPoint[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    Promise.all([
      api.getSpSummary(federationId),
      api.getSpSeries(federationId).catch(() => [] as SpSeriesPoint[]),
    ])
      .then(([summaryData, seriesData]) => {
        if (!cancelled) {
          setSummary(summaryData);
          setSeries(seriesData);
          setLoading(false);
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : 'Failed to load stability pool data');
          setLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [federationId]);

  if (loading) {
    return (
      <div className="py-10 text-center text-sm text-gray-500 dark:text-gray-400">
        Loading stability pool…
      </div>
    );
  }
  if (error) {
    return <div className="py-10 text-center text-sm text-red-500">Error: {error}</div>;
  }
  if (!summary || summary.account_count === 0) {
    return (
      <div className="py-10 text-center text-sm text-gray-500 dark:text-gray-400">
        No stability-pool activity observed for this federation
      </div>
    );
  }

  return (
    <div>
      <div className="mb-3 text-xs sm:text-sm text-gray-500 dark:text-gray-400">
        The stability pool lets users hold a fiat-stabilized balance. Figures are exact{' '}
        <span className="font-medium">net flows</span> from consensus (deposits − withdrawals),
        valued at each cycle&apos;s price; they are not live guardian balances. Fiat is the
        federation&apos;s stable-currency base unit, shown as USD.
      </div>

      <div className="mb-6 grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
        <Stat label="Accounts" value={formatNumber(summary.account_count)} />
        <Stat
          label="Seek / Provide"
          value={`${formatNumber(summary.seeker_count)} / ${formatNumber(summary.provider_count)}`}
        />
        <Stat label="Multisig" value={formatNumber(summary.multisig_count)} />
        <Stat label="Net pool (fiat)" value={formatFiat(summary.net_fiat)} />
        <Stat label="Net pool (BTC)" value={asBitcoin(summary.net_msat, 4)} />
        <Stat
          label="Latest price / BTC"
          value={summary.latest_price_fiat !== null ? formatFiat(summary.latest_price_fiat) : '—'}
        />
      </div>

      {series.length > 0 && (
        <div className="mb-8 grid grid-cols-1 gap-6 lg:grid-cols-2">
          <ChartCard title="Cycle price / BTC">
            <SpPriceChart series={series} />
          </ChartCard>
          <ChartCard title="Cumulative net contributed (fiat)">
            <SpNetFlowChart series={series} />
          </ChartCard>
        </div>
      )}

      <AccountsTable federationId={federationId} />
    </div>
  );
}

function AccountsTable({ federationId }: { federationId: string }) {
  const [order, setOrder] = useState<Order>('net');
  const [accounts, setAccounts] = useState<SpAccount[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    api
      .getSpAccounts(federationId, { order, limit: PAGE_SIZE, offset: 0 })
      .then((page) => {
        if (!cancelled) {
          setAccounts(page.items);
          setTotal(page.total);
          setLoading(false);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setAccounts([]);
          setLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [federationId, order]);

  const loadMore = () => {
    api
      .getSpAccounts(federationId, { order, limit: PAGE_SIZE, offset: accounts.length })
      .then((page) => setAccounts((prev) => [...prev, ...page.items]))
      .catch(() => {});
  };

  return (
    <div>
      <div className="mb-2 flex items-center justify-between">
        <h3 className="text-sm font-semibold text-gray-900 dark:text-white">
          Accounts <span className="text-gray-400 dark:text-gray-500">({formatNumber(total)})</span>
        </h3>
        <div className="flex gap-1 text-xs">
          {(['net', 'activity', 'recent'] as Order[]).map((o) => (
            <button
              key={o}
              onClick={() => setOrder(o)}
              className={`rounded px-2 py-1 ${
                order === o
                  ? 'bg-blue-600 text-white'
                  : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300'
              }`}
            >
              {o === 'net' ? 'Top net' : o === 'activity' ? 'Most active' : 'Recent'}
            </button>
          ))}
        </div>
      </div>

      {loading ? (
        <div className="py-8 text-center text-sm text-gray-500 dark:text-gray-400">Loading…</div>
      ) : (
        <div className="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
          <table className="min-w-full text-sm">
            <thead className="bg-gray-50 dark:bg-gray-900/40 text-left text-xs uppercase text-gray-400 dark:text-gray-500">
              <tr>
                <th className="px-3 py-2">Account</th>
                <th className="px-3 py-2">Type</th>
                <th className="px-3 py-2 text-right">Net (fiat)</th>
                <th className="px-3 py-2 text-right">Net (BTC)</th>
                <th className="px-3 py-2 text-right">Txs</th>
                <th className="px-3 py-2 text-right">Last seen</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200 dark:divide-gray-700">
              {accounts.map((a) => (
                <tr key={a.account_id} className="text-gray-900 dark:text-white">
                  <td className="px-3 py-2">
                    <AccountLink id={a.account_id} />
                  </td>
                  <td className="px-3 py-2">
                    <span className="mr-1">{accTypeLabel(a.acc_type)}</span>
                    {a.n_keys !== null && a.n_keys > 1 && (
                      <Badge level="info">{multisigLabel(a)}</Badge>
                    )}
                  </td>
                  <td className="px-3 py-2 text-right tabular-nums">{formatFiat(a.fiat_net)}</td>
                  <td className="px-3 py-2 text-right tabular-nums text-gray-500 dark:text-gray-400">
                    {asBitcoin(a.msat_net, 6)}
                  </td>
                  <td className="px-3 py-2 text-right tabular-nums">{formatNumber(a.tx_count)}</td>
                  <td className="px-3 py-2 text-right text-xs text-gray-500 dark:text-gray-400">
                    {formatTimestamp(a.last_seen)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {!loading && accounts.length < total && (
        <div className="mt-3 text-center">
          <button
            onClick={loadMore}
            className="rounded bg-gray-100 dark:bg-gray-700 px-3 py-1.5 text-xs text-gray-700 dark:text-gray-200 hover:bg-gray-200 dark:hover:bg-gray-600"
          >
            Load more
          </button>
        </div>
      )}
    </div>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-900/40 px-3 py-2">
      <div className="text-[10px] uppercase tracking-wide text-gray-500 dark:text-gray-400">
        {label}
      </div>
      <div className="text-sm font-semibold text-gray-900 dark:text-white tabular-nums">
        {value}
      </div>
    </div>
  );
}

function ChartCard({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div>
      <h3 className="mb-1 text-sm font-semibold text-gray-900 dark:text-white">{title}</h3>
      {children}
    </div>
  );
}
