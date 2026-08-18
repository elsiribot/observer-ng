import { useEffect, useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import { api } from '../services/api';
import type {
  EcashDenomAnon,
  SpTxAccount,
  TxDetail,
  TxItemPart,
  UserTransaction,
} from '../types/api';
import { Alert } from '../components/Alert';
import { Badge } from '../components/Badge';
import { AccountLink, classificationBadge, shorten } from '../components/explorer/itemRenderers';
import { asSats, formatNumber } from '../utils/format';
import { formatAnonSet, formatSi } from '../utils/anonSet';
import { spKindLabel } from '../utils/sp';

// Transaction-detail page: the structured inputs/outputs of one fedimint
// transaction, drilled into from a session's item list or the consensus
// stream, with a link onward to the gold-layer user transaction it's part
// of (if any) — closing the session → item → tx → user-tx navigation graph.
export function TransactionDetail() {
  const { id, txid } = useParams<{ id: string; txid: string }>();

  const [detail, setDetail] = useState<TxDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Best-effort enrichment: once we know the tx belongs to a user
  // transaction, fetch its kind/amount so the "Part of user transaction"
  // link can show a friendly label. Failure here doesn't block the page —
  // the link still works without the label.
  const [userTx, setUserTx] = useState<UserTransaction | null>(null);

  // Best-effort: which stability-pool account each input/output touches, so the
  // tx rows can link to account pages. Empty (404 or []) for non-SP txs.
  const [spAccounts, setSpAccounts] = useState<SpTxAccount[]>([]);

  useEffect(() => {
    if (!id || !txid) {
      setError('Invalid transaction reference');
      setLoading(false);
      return;
    }
    setLoading(true);
    setError(null);
    setDetail(null);
    setUserTx(null);
    setSpAccounts([]);
    api
      .getSpTxAccounts(id, txid)
      .then(setSpAccounts)
      .catch(() => {
        // Non-fatal: no SP module / not an SP tx → no account links.
      });
    api
      .getTxDetail(id, txid)
      .then((txDetail) => {
        setDetail(txDetail);
        if (txDetail.user_tx_key) {
          api
            .getUserTransaction(id, txDetail.user_tx_key)
            .then(setUserTx)
            .catch(() => {
              // Non-fatal: the link below just falls back to a plain label.
            });
        }
      })
      .catch((err: unknown) => {
        setError(err instanceof Error ? err.message : 'Failed to load transaction');
      })
      .finally(() => setLoading(false));
  }, [id, txid]);

  if (loading) {
    return (
      <div className="py-4 sm:py-8 px-4 sm:px-0 text-sm text-gray-500 dark:text-gray-400">
        Loading…
      </div>
    );
  }

  if (error || !detail) {
    return (
      <div className="py-4 sm:py-8 px-4 sm:px-0">
        <Alert level="error" message={error ?? 'Transaction not found'} />
      </div>
    );
  }

  const inputAccounts = new Map(
    spAccounts.filter((a) => a.side === 'input').map((a) => [a.index, a] as const)
  );
  const outputAccounts = new Map(
    spAccounts.filter((a) => a.side === 'output').map((a) => [a.index, a] as const)
  );

  return (
    <div className="py-4 sm:py-8 px-4 sm:px-0">
      <div className="mb-4 sm:mb-6">
        <Link
          to={`/federations/${id}/session/${detail.session_index}`}
          className="text-sm sm:text-base text-blue-600 dark:text-blue-400 hover:underline"
        >
          ← Back to Session {formatNumber(detail.session_index)}
        </Link>
      </div>

      <h1
        className="text-xl sm:text-2xl font-bold text-gray-900 dark:text-white mb-2 font-mono break-all"
        title={detail.txid}
      >
        {shorten(detail.txid, 12)}
      </h1>
      <div className="text-xs sm:text-sm text-gray-500 dark:text-gray-400 mb-6">
        Session {formatNumber(detail.session_index)} · Item {formatNumber(detail.item_index)}
      </div>

      {formatAnonSet(detail.ecash_anon_bits) !== null && (
        <div className="mb-6 sm:mb-8">
          <div className="uppercase text-xs text-gray-400 dark:text-gray-500 mb-1">
            Anonymity Set (estimated)
          </div>
          <span
            title="Upper-bound ecash anonymity set — the transaction hides among ~this many spenders of its scarcest spent denomination."
            className="cursor-help text-sm sm:text-base text-gray-900 dark:text-white"
          >
            {formatAnonSet(detail.ecash_anon_bits)}
          </span>
        </div>
      )}

      <AnonBreakdownBox breakdown={detail.ecash_anon_breakdown} />

      {formatAnonSet(detail.ecash_issuance_bits) !== null && (
        <div className="mb-6 sm:mb-8">
          <div className="uppercase text-xs text-gray-400 dark:text-gray-500 mb-1">
            Issuance Crowd (estimated)
          </div>
          <span
            title="Issuance-side estimate — the crowd of same-denomination notes this transaction's freshly-minted notes join. Forward-looking; weaker than the spend-side figure."
            className="cursor-help text-sm sm:text-base text-gray-900 dark:text-white"
          >
            {formatAnonSet(detail.ecash_issuance_bits)}
          </span>
        </div>
      )}

      {detail.user_tx_key && (
        <Link
          to={`/federations/${id}/user-transactions/${detail.user_tx_key}`}
          className="block mb-6 sm:mb-8 bg-blue-50 dark:bg-gray-800 border border-blue-200 dark:border-gray-700 rounded-lg p-4 hover:bg-blue-100 dark:hover:bg-gray-700 transition-colors"
        >
          <span className="text-sm text-blue-800 dark:text-blue-300">
            Part of user transaction:{' '}
            {userTx ? (
              <>
                <Badge level={classificationBadge(userTx.kind).level}>
                  {classificationBadge(userTx.kind).label}
                </Badge>
                {userTx.amount_msat !== null && (
                  <span className="font-semibold">{asSats(userTx.amount_msat)}</span>
                )}
              </>
            ) : (
              <span className="font-mono">{shorten(detail.user_tx_key)}</span>
            )}{' '}
            →
          </span>
        </Link>
      )}

      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 sm:gap-6">
        <TxPartsPanel title="Inputs" parts={detail.inputs} accounts={inputAccounts} />
        <TxPartsPanel title="Outputs" parts={detail.outputs} accounts={outputAccounts} />
      </div>
    </div>
  );
}

// Info box explaining what set the anonymity number: the spent ecash
// denominations, weakest (smallest crowd) first. The rows tied for the
// smallest pool are the weakest link — they equal the tx's ecash_anon_bits.
function AnonBreakdownBox({ breakdown }: { breakdown: EcashDenomAnon[] }) {
  if (!breakdown || breakdown.length === 0) {
    return null;
  }
  const pools = breakdown
    .map((d) => d.pool)
    .filter((p): p is number => p !== null && p !== undefined);
  const minPool = pools.length > 0 ? Math.min(...pools) : null;

  return (
    <div className="mb-6 sm:mb-8 rounded-lg border border-amber-200 dark:border-amber-900/50 bg-amber-50 dark:bg-amber-950/20 p-4">
      <div className="uppercase text-xs text-amber-700 dark:text-amber-500 mb-1">
        What limited the anonymity set
      </div>
      <p className="text-xs text-gray-600 dark:text-gray-400 mb-3">
        Each ecash denomination this transaction spent hides among the notes of
        that denomination already in circulation. The set is only as strong as
        the <span className="font-semibold">scarcest</span> one spent.
      </p>
      <ul className="divide-y divide-amber-200/70 dark:divide-amber-900/40">
        {breakdown.map((d) => {
          const isWeakest = minPool !== null && d.pool === minPool;
          return (
            <li
              key={`${d.kind}-${d.denomination_msat}`}
              className="py-2 flex flex-wrap items-baseline gap-x-3 gap-y-1 text-sm"
            >
              <span
                className="font-mono text-gray-900 dark:text-white"
                title={`${d.denomination_msat.toLocaleString()} msat · ${asSats(d.denomination_msat)}`}
              >
                {formatSi(d.denomination_msat)} msat
              </span>
              <span className="text-xs text-gray-500 dark:text-gray-400">
                ×{formatNumber(d.notes_spent)} spent
              </span>
              <span className="ml-auto text-sm text-gray-700 dark:text-gray-300 tabular-nums">
                {d.pool !== null ? (
                  <>
                    crowd of {formatNumber(d.pool)}
                    {d.bits !== null && (
                      <span className="text-xs text-gray-400 dark:text-gray-500">
                        {' '}
                        (≈{d.bits.toFixed(1)} bits)
                      </span>
                    )}
                  </>
                ) : (
                  <span className="text-gray-400 dark:text-gray-500">no pool data</span>
                )}
              </span>
              {isWeakest && (
                <Badge level="warning">weakest link</Badge>
              )}
            </li>
          );
        })}
      </ul>
    </div>
  );
}

function TxPartsPanel({
  title,
  parts,
  accounts,
}: {
  title: string;
  parts: TxItemPart[];
  accounts?: Map<number, SpTxAccount>;
}) {
  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow-md border border-gray-200 dark:border-gray-700 p-4 sm:p-6">
      <h2 className="text-base sm:text-lg font-semibold text-gray-900 dark:text-white mb-3 sm:mb-4">
        {title}
      </h2>
      {parts.length === 0 ? (
        <div className="text-sm text-gray-500 dark:text-gray-400">none</div>
      ) : (
        <ul className="divide-y divide-gray-200 dark:divide-gray-700">
          {parts.map((part) => (
            <TxPartRow key={part.index} part={part} account={accounts?.get(part.index)} />
          ))}
        </ul>
      )}
    </div>
  );
}

function TxPartRow({ part, account }: { part: TxItemPart; account?: SpTxAccount }) {
  return (
    <li className="py-2 sm:py-3">
      <div className="flex flex-wrap items-center gap-2 text-sm">
        <span className="text-xs text-gray-400 dark:text-gray-500 font-mono">#{part.index}</span>
        <Badge level="info">{part.kind}</Badge>
        {part.amount_msat !== null && (
          <span className="text-gray-900 dark:text-white font-mono">
            {asSats(part.amount_msat)}
          </span>
        )}
      </div>
      {account && (
        <div className="mt-1 flex flex-wrap items-center gap-1.5 text-xs text-gray-500 dark:text-gray-400">
          <span>{spKindLabel(account.kind)} ·</span>
          <span>account</span>
          <AccountLink id={account.account_id} />
          {account.counterparty && (
            <>
              <span>→</span>
              <AccountLink id={account.counterparty} />
            </>
          )}
        </div>
      )}
      {part.details !== null && (
        <details className="mt-1">
          <summary className="cursor-pointer text-xs text-gray-500 dark:text-gray-400">
            Raw details
          </summary>
          <pre className="mt-1 text-xs overflow-x-auto whitespace-pre-wrap break-words bg-gray-50 dark:bg-gray-900 p-2 rounded">
            {JSON.stringify(part.details, null, 2)}
          </pre>
        </details>
      )}
    </li>
  );
}
