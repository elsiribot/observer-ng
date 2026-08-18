import { useEffect, useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import { api } from '../services/api';
import type { SpAccount, SpAccountTx, SpTransferEdge } from '../types/api';
import { Alert } from '../components/Alert';
import { Badge } from '../components/Badge';
import { Copyable } from '../components/Copyable';
import { AccountLink, shorten } from '../components/explorer/itemRenderers';
import { SpAccountNetChart } from '../components/StabilityPoolCharts';
import { asBitcoin, asSats, formatFiat, formatNumber, formatTimestamp } from '../utils/format';
import { accTypeLabel, multisigLabel, spKindLabel, spKindLevel } from '../utils/sp';

// Stability-pool account page: net-flow totals, observed multisig structure, a
// cumulative-position chart, the folded transaction history (each row linking to
// its fedimint tx(s) and, for transfers, the counterparty account), and the
// aggregated transfer graph neighbors.
export function AccountDetail() {
  const { id, account_id: accountId } = useParams<{ id: string; account_id: string }>();

  const [account, setAccount] = useState<SpAccount | null>(null);
  const [txs, setTxs] = useState<SpAccountTx[]>([]);
  const [next, setNext] = useState<[number, string] | null>(null);
  const [transfers, setTransfers] = useState<SpTransferEdge[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!id || !accountId) {
      setError('Invalid account reference');
      setLoading(false);
      return;
    }
    let cancelled = false;
    setLoading(true);
    setError(null);
    setTxs([]);
    Promise.all([
      api.getSpAccount(id, accountId),
      api.getSpAccountTransactions(id, accountId, { limit: 50 }),
      api.getSpAccountTransfers(id, accountId).catch(() => [] as SpTransferEdge[]),
    ])
      .then(([acc, txPage, edges]) => {
        if (!cancelled) {
          setAccount(acc);
          setTxs(txPage.items);
          setNext(txPage.next);
          setTransfers(edges);
          setLoading(false);
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : 'Failed to load account');
          setLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [id, accountId]);

  const loadMore = () => {
    if (!id || !accountId || !next) return;
    api
      .getSpAccountTransactions(id, accountId, {
        beforeSession: next[0],
        beforeTxKey: next[1],
        limit: 50,
      })
      .then((page) => {
        setTxs((prev) => [...prev, ...page.items]);
        setNext(page.next);
      })
      .catch(() => {});
  };

  if (loading) {
    return (
      <div className="py-4 sm:py-8 px-4 sm:px-0 text-sm text-gray-500 dark:text-gray-400">
        Loading…
      </div>
    );
  }
  if (error || !account) {
    return (
      <div className="py-4 sm:py-8 px-4 sm:px-0">
        <Alert level="error" message={error ?? 'Account not found'} />
      </div>
    );
  }

  return (
    <div className="py-4 sm:py-8 px-4 sm:px-0">
      <div className="mb-4 sm:mb-6">
        <Link
          to={`/federations/${id}`}
          className="text-sm sm:text-base text-blue-600 dark:text-blue-400 hover:underline"
        >
          ← Back to Federation
        </Link>
      </div>

      <div className="flex flex-wrap items-center gap-2 mb-2">
        <Badge level="info">{accTypeLabel(account.acc_type)}</Badge>
        <Badge level={account.n_keys !== null && account.n_keys > 1 ? 'warning' : 'info'}>
          {multisigLabel(account)}
        </Badge>
      </div>
      <div className="mb-6 sm:mb-8">
        <Copyable text={account.account_id} />
      </div>

      <div className="bg-white dark:bg-gray-800 rounded-lg shadow-md border border-gray-200 dark:border-gray-700 p-4 sm:p-6 mb-6 sm:mb-8 grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-4 text-sm">
        <Field label="Net (fiat)">{formatFiat(account.fiat_net, true)}</Field>
        <Field label="Net (BTC)">{asBitcoin(account.msat_net, 8)}</Field>
        <Field label="Deposited">{formatFiat(account.fiat_deposited)}</Field>
        <Field label="Withdrawn">{formatFiat(account.fiat_withdrawn)}</Field>
        <Field label="Transfers in">{formatFiat(account.transfers_in_fiat)}</Field>
        <Field label="Transfers out">{formatFiat(account.transfers_out_fiat)}</Field>
        <Field label="Transactions">{formatNumber(account.tx_count)}</Field>
        <Field label="First / last seen">
          {formatTimestamp(account.first_seen)} – {formatTimestamp(account.last_seen)}
        </Field>
      </div>

      {account.acc_type === 'seeker' && (
        <div className="mb-6 -mt-4 text-xs text-gray-500 dark:text-gray-400">
          For a seeker, net fiat ≈ the current stabilized balance (seeks preserve fiat value).
        </div>
      )}
      {account.acc_type === 'provider' && (
        <div className="mb-6 -mt-4 text-xs text-gray-500 dark:text-gray-400">
          For a provider, net fiat is capital contributed, not a live balance — providers take BTC
          price exposure and earn fees settled in guardian-internal state.
        </div>
      )}

      {txs.length > 0 && (
        <div className="mb-8">
          <h2 className="mb-1 text-base sm:text-lg font-semibold text-gray-900 dark:text-white">
            Net position over time
          </h2>
          <SpAccountNetChart txs={txs} />
        </div>
      )}

      <h2 className="text-base sm:text-lg font-semibold text-gray-900 dark:text-white mb-3 sm:mb-4">
        Transactions
      </h2>
      {txs.length === 0 ? (
        <div className="text-center text-xs sm:text-sm text-gray-500 dark:text-gray-400 py-12">
          No transactions
        </div>
      ) : (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow-md border border-gray-200 dark:border-gray-700 divide-y divide-gray-200 dark:divide-gray-700">
          {txs.map((tx) => (
            <TxRow key={tx.tx_key} tx={tx} federationId={id!} />
          ))}
        </div>
      )}
      {next && (
        <div className="mt-3 text-center">
          <button
            onClick={loadMore}
            className="rounded bg-gray-100 dark:bg-gray-700 px-3 py-1.5 text-xs text-gray-700 dark:text-gray-200 hover:bg-gray-200 dark:hover:bg-gray-600"
          >
            Load more
          </button>
        </div>
      )}

      {transfers.length > 0 && (
        <div className="mt-8">
          <h2 className="text-base sm:text-lg font-semibold text-gray-900 dark:text-white mb-3 sm:mb-4">
            Transfer partners
          </h2>
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow-md border border-gray-200 dark:border-gray-700 divide-y divide-gray-200 dark:divide-gray-700">
            {transfers.map((edge) => (
              <div
                key={`${edge.direction}-${edge.counterparty}`}
                className="flex flex-wrap items-center gap-2 px-4 py-2.5 text-sm"
              >
                <Badge level={edge.direction === 'in' ? 'success' : 'warning'}>
                  {edge.direction === 'in' ? 'received from' : 'sent to'}
                </Badge>
                <AccountLink id={edge.counterparty} />
                <span className="ml-auto tabular-nums text-gray-900 dark:text-white">
                  {formatFiat(edge.total_fiat)}
                </span>
                <span className="text-xs text-gray-500 dark:text-gray-400">
                  · {formatNumber(edge.n)} transfer{edge.n === 1 ? '' : 's'}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function TxRow({ tx, federationId }: { tx: SpAccountTx; federationId: string }) {
  return (
    <div className="px-4 py-3">
      <div className="flex flex-wrap items-center gap-2 text-sm">
        <Badge level={spKindLevel(tx.kind)}>{spKindLabel(tx.kind)}</Badge>
        {tx.fiat_amount !== null && (
          <span className="font-semibold text-gray-900 dark:text-white tabular-nums">
            {formatFiat(tx.fiat_amount)}
            {tx.fiat_is_target && (
              <span className="ml-1 text-xs font-normal text-gray-400" title="requested target">
                (target)
              </span>
            )}
          </span>
        )}
        {tx.amount_msat !== null && (
          <span className="text-gray-500 dark:text-gray-400 tabular-nums">
            {asSats(tx.amount_msat)}
          </span>
        )}
        {tx.counterparty && (
          <span className="flex items-center gap-1 text-xs text-gray-500 dark:text-gray-400">
            {tx.kind === 'transfer_out' ? '→' : '←'} <AccountLink id={tx.counterparty} />
          </span>
        )}
        <span className="ml-auto text-xs text-gray-400 dark:text-gray-500">
          {formatTimestamp(tx.timestamp)}
        </span>
      </div>
      <div className="mt-1 flex flex-wrap items-center gap-3 text-xs text-gray-500 dark:text-gray-400">
        <Link
          to={`/federations/${federationId}/tx/${tx.primary_txid}`}
          className="font-mono text-blue-600 dark:text-blue-400 hover:underline"
          title={tx.primary_txid}
        >
          tx {shorten(tx.primary_txid)}
        </Link>
        {tx.secondary_txid && (
          <Link
            to={`/federations/${federationId}/tx/${tx.secondary_txid}`}
            className="font-mono text-blue-600 dark:text-blue-400 hover:underline"
            title={tx.secondary_txid}
          >
            unlock {shorten(tx.secondary_txid)}
          </Link>
        )}
        {tx.cycle_index !== null && <span>cycle {formatNumber(tx.cycle_index)}</span>}
      </div>
    </div>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <div className="text-[10px] uppercase tracking-wide text-gray-500 dark:text-gray-400">
        {label}
      </div>
      <div className="font-semibold text-gray-900 dark:text-white tabular-nums">{children}</div>
    </div>
  );
}
