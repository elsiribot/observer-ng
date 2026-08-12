import { useEffect, useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import { api } from '../services/api';
import type { TxDetail, TxItemPart, UserTransaction } from '../types/api';
import { Alert } from '../components/Alert';
import { Badge } from '../components/Badge';
import { classificationBadge, shorten } from '../components/explorer/itemRenderers';
import { asBitcoin, formatNumber } from '../utils/format';

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
                  <span className="font-semibold">{asBitcoin(userTx.amount_msat)}</span>
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
        <TxPartsPanel title="Inputs" parts={detail.inputs} />
        <TxPartsPanel title="Outputs" parts={detail.outputs} />
      </div>
    </div>
  );
}

function TxPartsPanel({ title, parts }: { title: string; parts: TxItemPart[] }) {
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
            <TxPartRow key={part.index} part={part} />
          ))}
        </ul>
      )}
    </div>
  );
}

function TxPartRow({ part }: { part: TxItemPart }) {
  return (
    <li className="py-2 sm:py-3">
      <div className="flex flex-wrap items-center gap-2 text-sm">
        <span className="text-xs text-gray-400 dark:text-gray-500 font-mono">#{part.index}</span>
        <Badge level="info">{part.kind}</Badge>
        {part.amount_msat !== null && (
          <span className="text-gray-900 dark:text-white font-mono">
            {asBitcoin(part.amount_msat)}
          </span>
        )}
      </div>
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
