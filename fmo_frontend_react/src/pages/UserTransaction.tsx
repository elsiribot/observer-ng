import { useEffect, useState } from 'react';
import type { ReactNode } from 'react';
import { useParams, Link } from 'react-router-dom';
import { api } from '../services/api';
import type { MemberTx, UserTransaction as UserTransactionData } from '../types/api';
import { Alert } from '../components/Alert';
import { Badge, type BadgeLevel } from '../components/Badge';
import { Copyable } from '../components/Copyable';
import { classificationBadge, shorten } from '../components/explorer/itemRenderers';
import { asSats, formatNumber, formatTimestamp } from '../utils/format';

// Badge levels for a member tx's role in a user transaction's lifecycle
// (see `fmo_core/src/gold.rs`). Unknown/future roles fall back to 'info'.
const ROLE_BADGE_LEVELS: Record<string, BadgeLevel> = {
  offer: 'info',
  fund: 'success',
  claim: 'success',
  cancel: 'error',
  refund: 'warning',
  self: 'info',
};

function roleBadgeLevel(role: string): BadgeLevel {
  return ROLE_BADGE_LEVELS[role] ?? 'info';
}

// User-transaction page: the gold-layer deduplicated summary of one user
// transaction plus every underlying fedimint transaction ("member tx") and
// its role, each linking back to the transaction-detail page — closing the
// session → item → tx → user-tx → member-tx navigation graph.
export function UserTransaction() {
  const { id, key } = useParams<{ id: string; key: string }>();

  const [userTx, setUserTx] = useState<UserTransactionData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!id || !key) {
      setError('Invalid user transaction reference');
      setLoading(false);
      return;
    }
    setLoading(true);
    setError(null);
    setUserTx(null);
    api
      .getUserTransaction(id, key)
      .then(setUserTx)
      .catch((err: unknown) => {
        setError(err instanceof Error ? err.message : 'Failed to load user transaction');
      })
      .finally(() => setLoading(false));
  }, [id, key]);

  if (loading) {
    return (
      <div className="py-4 sm:py-8 px-4 sm:px-0 text-sm text-gray-500 dark:text-gray-400">
        Loading…
      </div>
    );
  }

  if (error || !userTx) {
    return (
      <div className="py-4 sm:py-8 px-4 sm:px-0">
        <Alert level="error" message={error ?? 'User transaction not found'} />
      </div>
    );
  }

  const badge = classificationBadge(userTx.kind);

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
        <Badge level={badge.level}>{badge.label}</Badge>
        <span className="text-xs sm:text-sm text-gray-500 dark:text-gray-400 capitalize">
          {userTx.direction}
        </span>
      </div>
      <h1 className="text-2xl sm:text-3xl font-bold text-gray-900 dark:text-white mb-6 sm:mb-8 break-words">
        {userTx.amount_msat !== null ? asSats(userTx.amount_msat) : 'unknown amount'}
      </h1>

      <div className="bg-white dark:bg-gray-800 rounded-lg shadow-md border border-gray-200 dark:border-gray-700 p-4 sm:p-6 mb-6 sm:mb-8 grid grid-cols-1 sm:grid-cols-2 gap-4 sm:gap-6 text-sm">
        <SummaryField label="Fedimint Fee">
          {userTx.fedimint_fee_msat !== null ? asSats(userTx.fedimint_fee_msat) : 'unknown'}
        </SummaryField>
        <SummaryField label="Gateway Fee (estimated)">
          {userTx.gateway_fee_estimate_msat !== null
            ? asSats(userTx.gateway_fee_estimate_msat)
            : 'n/a'}
        </SummaryField>
        <SummaryField label="Fedimint Transactions">
          {formatNumber(userTx.num_fedimint_txs)}
        </SummaryField>
        <SummaryField label="First / Last Seen">
          {formatTimestamp(userTx.first_timestamp)} – {formatTimestamp(userTx.last_timestamp)}
        </SummaryField>
      </div>

      <h2 className="text-base sm:text-lg font-semibold text-gray-900 dark:text-white mb-3 sm:mb-4">
        Member Transactions
      </h2>
      {userTx.member_txs.length === 0 ? (
        <div className="text-center text-xs sm:text-sm text-gray-500 dark:text-gray-400 py-12">
          No member transactions
        </div>
      ) : (
        <div className="bg-white dark:bg-gray-800 rounded-lg shadow-md border border-gray-200 dark:border-gray-700 divide-y divide-gray-200 dark:divide-gray-700">
          {userTx.member_txs.map((memberTx) => (
            <MemberTxRow
              key={`${memberTx.txid}-${memberTx.role}`}
              federationId={id as string}
              memberTx={memberTx}
            />
          ))}
        </div>
      )}
    </div>
  );
}

function SummaryField({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div>
      <div className="uppercase text-xs text-gray-400 dark:text-gray-500 mb-1">{label}</div>
      <div className="text-sm sm:text-base text-gray-900 dark:text-white">{children}</div>
    </div>
  );
}

function MemberTxRow({ federationId, memberTx }: { federationId: string; memberTx: MemberTx }) {
  return (
    <div className="flex flex-col sm:flex-row sm:items-center gap-2 sm:gap-4 p-4">
      <Badge level={roleBadgeLevel(memberTx.role)}>{memberTx.role}</Badge>
      <Link
        to={`/federations/${federationId}/tx/${memberTx.txid}`}
        className="font-mono text-xs text-blue-600 dark:text-blue-400 hover:underline shrink-0"
        title={memberTx.txid}
      >
        {shorten(memberTx.txid)}
      </Link>
      <div className="flex-1 min-w-0 max-w-xs">
        <Copyable text={memberTx.txid} />
      </div>
      <span className="text-xs text-gray-500 dark:text-gray-400 sm:ml-auto shrink-0">
        Session {formatNumber(memberTx.session_index)}
      </span>
    </div>
  );
}
