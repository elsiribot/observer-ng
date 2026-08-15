/* eslint-disable react-refresh/only-export-components --
 * This module's public surface is `renderItem`, a plain dispatch function
 * (not a component) that composes several small internal-only row
 * components below. That mix is intentional here (it's a renderer registry,
 * not a page), so Fast Refresh boundary purity doesn't apply. */
import { useState } from 'react';
import type { MouseEvent, ReactNode } from 'react';
import { Link, useParams } from 'react-router-dom';
import { Badge, type BadgeLevel } from '../Badge';
import { api } from '../../services/api';
import { asSats, formatNumber } from '../../utils/format';
import { formatAnonSetCount } from '../../utils/anonSet';
import type { SessionItem, TxDetail, TxItemPart } from '../../types/api';

// Renderer registry for one `SessionItem` (a fedimint transaction or a
// consensus item) as it appears in a session's item list or the federation
// consensus stream. Dispatches on `item_type` then `kind`. Every branch is
// defensive: malformed/unexpected `details` shapes fall back to a raw-JSON
// view rather than throwing, since `details` is module-decoded JSON we don't
// control the shape of.
export function renderItem(item: SessionItem): ReactNode {
  if (item.item_type === 'transaction') {
    return <TransactionRow item={item} />;
  }
  return <ConsensusItemRow item={item} />;
}

// Exported so `TransactionDetail`/`UserTransaction` pages can render the
// same shortened hex + classification badge for consistency.
export function shorten(hex: string, chars = 8): string {
  if (hex.length <= chars * 2 + 1) {
    return hex;
  }
  return `${hex.slice(0, chars)}…${hex.slice(-chars)}`;
}

// Compact icon-only copy-to-clipboard button, for placing inline next to a
// hash/id without disrupting row layout (unlike `Copyable`, which pairs an
// input field with the button). Mirrors `Copyable`'s copied-state + icons.
export function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);

  const handleCopy = async (e: MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch (err) {
      console.error('Failed to copy text:', err);
    }
  };

  return (
    <button
      type="button"
      onClick={handleCopy}
      className="shrink-0 inline-flex items-center text-gray-400 hover:text-gray-700 dark:text-gray-500 dark:hover:text-gray-300"
      aria-label={copied ? 'Copied!' : 'Copy'}
      title={copied ? 'Copied!' : 'Copy'}
    >
      {copied ? (
        <svg
          className="w-3.5 h-3.5 text-green-600 dark:text-green-400"
          aria-hidden="true"
          xmlns="http://www.w3.org/2000/svg"
          width="24"
          height="24"
          fill="none"
          viewBox="0 0 24 24"
        >
          <path
            stroke="currentColor"
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth="2.3"
            d="M5 11.917 9.724 16.5 19 7.5"
          />
        </svg>
      ) : (
        <svg
          className="w-3.5 h-3.5"
          aria-hidden="true"
          xmlns="http://www.w3.org/2000/svg"
          width="24"
          height="24"
          fill="none"
          viewBox="0 0 24 24"
        >
          <path
            stroke="currentColor"
            strokeLinejoin="round"
            strokeWidth="2.3"
            d="M9 8v3a1 1 0 0 1-1 1H5m11 4h2a1 1 0 0 0 1-1V5a1 1 0 0 0-1-1h-7a1 1 0 0 0-1 1v1m4 3v10a1 1 0 0 1-1 1H6a1 1 0 0 1-1-1v-7.13a1 1 0 0 1 .24-.65L7.7 8.35A1 1 0 0 1 8.46 8H13a1 1 0 0 1 1 1Z"
          />
        </svg>
      )}
    </button>
  );
}

// ---- Transaction row -------------------------------------------------

// Maps the gold-layer `user_transactions.kind` (see `fmo_core/src/gold.rs`)
// to a friendly label + badge level for the explorer's transaction rows.
// A `user_tx_kind` outside this map (or null — not yet folded into a gold
// user transaction) falls back to a neutral "Transaction" badge below.
const USER_TX_KIND_LABELS: Record<string, { label: string; level: BadgeLevel }> = {
  peg_in: { label: 'Peg-in', level: 'success' },
  peg_out: { label: 'Peg-out', level: 'success' },
  ln_send: { label: 'LN Send', level: 'success' },
  ln_receive: { label: 'LN Receive', level: 'success' },
  ecash_transfer: { label: 'Ecash', level: 'success' },
  lnv2_send: { label: 'LN Send (v2)', level: 'success' },
  lnv2_receive: { label: 'LN Receive (v2)', level: 'success' },
  peg_in_v2: { label: 'Peg-in (v2)', level: 'success' },
  peg_out_v2: { label: 'Peg-out (v2)', level: 'success' },
  ecash_transfer_v2: { label: 'Ecash (v2)', level: 'success' },
  stability_pool: { label: 'Stability Pool', level: 'success' },
};

export function classificationBadge(
  userTxKind: string | null
): { label: string; level: BadgeLevel } {
  if (userTxKind && userTxKind in USER_TX_KIND_LABELS) {
    return USER_TX_KIND_LABELS[userTxKind];
  }
  return { label: 'Transaction', level: 'success' };
}

function directionHint(direction: string | null): string | null {
  switch (direction) {
    case 'in':
      return 'in';
    case 'out':
      return 'out';
    case 'internal':
      return 'internal';
    default:
      return null;
  }
}

// Badge levels for a tx's role in its gold user transaction's lifecycle (see
// `fmo_core/src/gold.rs`). Unknown/future roles fall back to 'info'. Shared
// by the explorer's item-label column and the user-transaction page's
// member-tx rows.
export const ROLE_BADGE_LEVELS: Record<string, BadgeLevel> = {
  offer: 'info',
  fund: 'success',
  claim: 'success',
  cancel: 'error',
  refund: 'warning',
  self: 'info',
};

export function roleBadgeLevel(role: string): BadgeLevel {
  return ROLE_BADGE_LEVELS[role] ?? 'info';
}

function capitalize(text: string): string {
  return text.length === 0 ? text : text[0].toUpperCase() + text.slice(1);
}

// Fixed-width label-column content for one `SessionItem`: the gold-kind
// classification (+ role, when present and not 'self') for a transaction, or
// the CI kind (+ guardian) for a consensus item. Stacked (`flex-col`) so
// rows of varying badge counts still line up, and so bodies rendered
// alongside via `renderItem` don't have to carry this themselves.
export function itemLabel(item: SessionItem): ReactNode {
  if (item.item_type === 'transaction') {
    const badge = classificationBadge(item.user_tx_kind);
    const direction = directionHint(item.direction);
    return (
      <div className="flex flex-col gap-1">
        <Badge level={badge.level}>{badge.label}</Badge>
        {item.role && item.role !== 'self' && (
          <Badge level={roleBadgeLevel(item.role)}>{capitalize(item.role)}</Badge>
        )}
        {direction && (
          <span className="text-xs text-gray-500 dark:text-gray-400 capitalize">{direction}</span>
        )}
      </div>
    );
  }
  return (
    <div className="flex flex-col gap-1">
      <Badge level="info">{item.kind ?? 'ci'}</Badge>
      {item.peer_id !== null && item.peer_id !== undefined && (
        <span className="text-xs text-gray-500 dark:text-gray-400">Guardian {item.peer_id}</span>
      )}
    </div>
  );
}

// On-demand loader for a fedimint transaction's structured detail, shared by
// the consensus/session item rows and the user-transaction page's member-tx
// rows. Fetches once, the first time the row is expanded.
export function useTxDetailToggle(federationId: string | undefined, txid: string | null) {
  const [expanded, setExpanded] = useState(false);
  const [detail, setDetail] = useState<TxDetail | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const toggle = () => {
    const next = !expanded;
    setExpanded(next);
    if (next && !detail && !loading && federationId && txid) {
      setLoading(true);
      setError(null);
      api
        .getTxDetail(federationId, txid)
        .then(setDetail)
        .catch((err: unknown) => {
          setError(err instanceof Error ? err.message : 'Failed to load transaction details');
        })
        .finally(() => setLoading(false));
    }
  };

  return { expanded, toggle, detail, loading, error };
}

function TransactionRow({ item }: { item: SessionItem }) {
  const { id: federationId } = useParams<{ id: string }>();
  const {
    expanded,
    toggle: handleToggle,
    detail,
    loading: loadingDetail,
    error: detailError,
  } = useTxDetailToggle(federationId, item.txid);

  return (
    <div>
      <div className="flex flex-col sm:flex-row sm:items-center gap-2">
        {item.txid && federationId ? (
          <span className="inline-flex items-center gap-1 min-w-0">
            <Link
              to={`/federations/${federationId}/tx/${item.txid}`}
              className="font-mono text-xs text-blue-600 dark:text-blue-400 hover:underline truncate"
              title={item.txid}
            >
              {shorten(item.txid)}
            </Link>
            <CopyButton text={item.txid} />
          </span>
        ) : (
          <span
            className="inline-flex items-center gap-1 min-w-0 font-mono text-xs text-gray-700 dark:text-gray-300 truncate"
            title={item.txid ?? undefined}
          >
            {item.txid ? shorten(item.txid) : 'unknown txid'}
            {item.txid && <CopyButton text={item.txid} />}
          </span>
        )}
        {item.ecash_anon_bits != null && (
          <span
            className="text-xs text-gray-400 dark:text-gray-500 shrink-0"
            title="Upper-bound ecash anonymity set — the transaction hides among ~this many spenders of its scarcest spent denomination (2^⌊bits⌋)."
          >
            ≈{formatAnonSetCount(item.ecash_anon_bits)}
          </span>
        )}
        {item.ecash_issuance_bits != null && (
          <span
            className="text-xs text-indigo-400 dark:text-indigo-400/80 shrink-0"
            title="Issuance-side estimate — the crowd of same-denomination notes this transaction's freshly-minted notes join. Forward-looking; weaker than the spend-side figure."
          >
            mint ≈{formatAnonSetCount(item.ecash_issuance_bits)}
          </span>
        )}
        {item.user_tx_key && federationId && (
          <Link
            to={`/federations/${federationId}/user-transactions/${item.user_tx_key}`}
            className="text-xs text-blue-600 dark:text-blue-400 hover:underline"
          >
            Part of user transaction →
          </Link>
        )}
        {item.txid && (
          <button
            type="button"
            onClick={handleToggle}
            className="text-xs text-gray-500 dark:text-gray-400 hover:underline sm:ml-auto text-left"
          >
            {expanded ? 'Hide details' : 'Show details'}
          </button>
        )}
      </div>
      {expanded && (
        <TxDetailBody detail={detail} loading={loadingDetail} error={detailError} />
      )}
    </div>
  );
}

// Sums the amounts of a tx's inputs or outputs. `complete` is false when any
// part's amount is unknown (NULL), so totals/fee can be shown as approximate
// rather than silently wrong.
function partsTotal(parts: TxItemPart[]): { total: number; complete: boolean } {
  let total = 0;
  let complete = true;
  for (const part of parts) {
    if (part.amount_msat === null) {
      complete = false;
    } else {
      total += part.amount_msat;
    }
  }
  return { total, complete };
}

// Structured body of a fedimint transaction: its inputs/outputs plus a
// total-in / total-out / fee summary (fee = inputs − outputs). Shared by the
// item-row expander and the user-transaction member-tx rows. Renders its own
// loading/error states so callers just pass the async result through.
export function TxDetailBody({
  detail,
  loading = false,
  error = null,
}: {
  detail: TxDetail | null;
  loading?: boolean;
  error?: string | null;
}) {
  const totalIn = detail ? partsTotal(detail.inputs) : null;
  const totalOut = detail ? partsTotal(detail.outputs) : null;
  const feeKnown = totalIn?.complete && totalOut?.complete;
  const fee = totalIn && totalOut ? totalIn.total - totalOut.total : 0;

  return (
    <div className="mt-2 text-xs text-gray-700 dark:text-gray-300">
      {loading && <span>Loading…</span>}
      {error && <span className="text-red-500 dark:text-red-400">{error}</span>}
      {detail && totalIn && totalOut && (
        <>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
            <TxPartsList title="Inputs" parts={detail.inputs} />
            <TxPartsList title="Outputs" parts={detail.outputs} />
          </div>
          <dl className="mt-3 pt-2 border-t border-gray-200 dark:border-gray-700 grid grid-cols-3 gap-2 text-xs">
            <TotalCell label="Total in" value={totalIn.total} approximate={!totalIn.complete} />
            <TotalCell label="Total out" value={totalOut.total} approximate={!totalOut.complete} />
            {feeKnown ? (
              <TotalCell label="Fee" value={fee} />
            ) : (
              <div>
                <dt className="uppercase text-gray-400 dark:text-gray-500">Fee</dt>
                <dd className="font-mono text-gray-400 dark:text-gray-500">unknown</dd>
              </div>
            )}
          </dl>
        </>
      )}
    </div>
  );
}

function TotalCell({
  label,
  value,
  approximate = false,
}: {
  label: string;
  value: number;
  approximate?: boolean;
}) {
  return (
    <div>
      <dt className="uppercase text-gray-400 dark:text-gray-500">{label}</dt>
      <dd className="font-mono text-gray-900 dark:text-white tabular-nums">
        {approximate ? '≥ ' : ''}
        {asSats(value)}
      </dd>
    </div>
  );
}

function TxPartsList({ title, parts }: { title: string; parts: TxDetail['inputs'] }) {
  if (parts.length === 0) {
    return (
      <div>
        <div className="uppercase text-gray-400 dark:text-gray-500 mb-1">{title}</div>
        <div className="text-gray-400 dark:text-gray-500">none</div>
      </div>
    );
  }
  return (
    <div>
      <div className="uppercase text-gray-400 dark:text-gray-500 mb-1">{title}</div>
      <ul className="space-y-0.5">
        {parts.map((part) => (
          <li key={part.index}>
            {part.kind}
            {part.amount_msat !== null ? ` — ${asSats(part.amount_msat)}` : ''}
          </li>
        ))}
      </ul>
    </div>
  );
}

// Renders a contract id, cross-linked to the gold-layer user-transaction page
// (LN's dedup grain is `contract_id`, see `fmo_core/src/gold.rs`) when a
// federation id is available from the route; otherwise falls back to plain
// mono text so this still works outside a `/federations/:id/*` route.
export function ContractLink({ id }: { id: string }) {
  const { id: federationId } = useParams<{ id: string }>();
  if (!federationId) {
    return (
      <span className="font-mono text-xs" title={id}>
        {shorten(id)}
      </span>
    );
  }
  return (
    <Link
      to={`/federations/${federationId}/user-transactions/${id}`}
      className="font-mono text-xs text-blue-600 dark:text-blue-400 hover:underline"
      title={id}
    >
      {shorten(id)}
    </Link>
  );
}

// ---- Consensus item row -----------------------------------------------

function ConsensusItemRow({ item }: { item: SessionItem }) {
  return <div className="text-sm text-gray-900 dark:text-white">{safeCiBody(item)}</div>;
}

function safeCiBody(item: SessionItem): ReactNode {
  try {
    return renderCiBody(item);
  } catch {
    return <RawFallback details={item.details} />;
  }
}

function renderCiBody(item: SessionItem): ReactNode {
  switch (item.kind) {
    case 'ln': {
      const rendered = renderLn(item.details);
      if (rendered !== null) return rendered;
      break;
    }
    case 'lnv2': {
      const rendered = renderLnv2(item.details);
      if (rendered !== null) return rendered;
      break;
    }
    case 'wallet':
    case 'walletv2': {
      const rendered = renderWallet(item.details);
      if (rendered !== null) return rendered;
      break;
    }
    case 'meta':
      return renderMeta(item.details, item.peer_id);
    default:
      break;
  }
  return <RawFallback details={item.details} />;
}

function RawFallback({ details }: { details: unknown }) {
  const variant = singleVariant(details);
  return (
    <>
      {variant && (
        <span className="mr-2 text-xs text-gray-500 dark:text-gray-400">{variant.tag}</span>
      )}
      <details className="inline-block align-middle">
        <summary className="cursor-pointer text-xs text-gray-500 dark:text-gray-400">
          Raw details
        </summary>
        <pre className="mt-1 text-xs overflow-x-auto whitespace-pre-wrap break-words bg-gray-50 dark:bg-gray-900 p-2 rounded">
          {JSON.stringify(details, null, 2)}
        </pre>
      </details>
    </>
  );
}

// ---- Per-kind detail decoding ------------------------------------------
//
// `details` is the JSON-serialized module consensus-item enum (externally
// tagged: `{ "<Variant>": <payload> }`). We only recognize the handful of
// variants the observer actually emits/needs; anything else (unexpected
// shape, new variant, decoding disabled) returns `null` so the caller falls
// back to the raw-JSON view.

function asObj(value: unknown): Record<string, unknown> | null {
  if (value && typeof value === 'object' && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  return null;
}

function objField(value: unknown, key: string): unknown {
  const obj = asObj(value);
  return obj ? obj[key] : undefined;
}

function singleVariant(details: unknown): { tag: string; value: unknown } | null {
  const obj = asObj(details);
  if (!obj) return null;
  const keys = Object.keys(obj);
  if (keys.length !== 1) return null;
  return { tag: keys[0], value: obj[keys[0]] };
}

function renderLn(details: unknown): ReactNode | null {
  const variant = singleVariant(details);
  if (!variant) return null;
  switch (variant.tag) {
    case 'DecryptPreimage': {
      const arr = variant.value;
      if (!Array.isArray(arr)) return null;
      const contractId = arr[0];
      if (typeof contractId !== 'string') return null;
      return (
        <>
          Preimage decryption share for contract{' '}
          <span className="inline-flex items-center gap-1">
            <ContractLink id={contractId} />
            <CopyButton text={contractId} />
          </span>
        </>
      );
    }
    case 'BlockCount': {
      const blockCount = variant.value;
      if (typeof blockCount !== 'number') return null;
      return (
        <>
          Block count vote: <span className="font-mono">{formatNumber(blockCount)}</span>
        </>
      );
    }
    default:
      return null;
  }
}

function renderLnv2(details: unknown): ReactNode | null {
  const variant = singleVariant(details);
  if (!variant) return null;
  switch (variant.tag) {
    case 'UnixTimeVote': {
      const unixTime = variant.value;
      if (typeof unixTime !== 'number') return null;
      return (
        <>
          Unix time vote:{' '}
          <span className="font-mono">{new Date(unixTime * 1000).toLocaleString()}</span>
        </>
      );
    }
    case 'BlockCountVote': {
      const blockCount = variant.value;
      if (typeof blockCount !== 'number') return null;
      return (
        <>
          Block count vote: <span className="font-mono">{formatNumber(blockCount)}</span>
        </>
      );
    }
    default:
      return null;
  }
}

function renderWallet(details: unknown): ReactNode | null {
  const variant = singleVariant(details);
  if (!variant) return null;
  switch (variant.tag) {
    case 'BlockCount': {
      const blockCount = variant.value;
      if (typeof blockCount !== 'number') return null;
      return (
        <>
          Block height vote: <span className="font-mono">{formatNumber(blockCount)}</span>
        </>
      );
    }
    case 'Feerate': {
      const satsPerKvb = objField(variant.value, 'sats_per_kvb');
      if (typeof satsPerKvb !== 'number') return null;
      return (
        <>
          Fee rate vote: <span className="font-mono">{formatNumber(satsPerKvb)} sats/kvB</span>
        </>
      );
    }
    case 'PegOutSignature': {
      const txid = objField(variant.value, 'txid');
      if (typeof txid !== 'string') return null;
      return (
        <>
          Peg-out signature for tx{' '}
          <span className="font-mono text-xs" title={txid}>
            {shorten(txid)}
          </span>
        </>
      );
    }
    case 'ModuleConsensusVersion': {
      const major = objField(variant.value, 'major');
      const minor = objField(variant.value, 'minor');
      if (typeof major !== 'number') return null;
      return (
        <>
          Module consensus version vote:{' '}
          <span className="font-mono">
            {major}.{typeof minor === 'number' ? minor : 0}
          </span>
        </>
      );
    }
    case 'Signatures': {
      // walletv2: [txid, signatures[]]
      const arr = variant.value;
      if (!Array.isArray(arr)) return null;
      const txid = arr[0];
      if (typeof txid !== 'string') return null;
      return (
        <>
          Peg-out signatures for tx{' '}
          <span className="font-mono text-xs" title={txid}>
            {shorten(txid)}
          </span>
        </>
      );
    }
    default:
      return null;
  }
}

// The observer has no `meta` module wired in (see `fmo_server::builder`), so
// `details` for `kind === 'meta'` items is currently always `null` (raw
// consensus items are recorded structurally, but only a registered
// `ObserverModule` decodes `details`). We still render a friendly summary
// when a payload *is* present (forwards-compatible if a meta module is added
// later), and a graceful placeholder otherwise — never the raw-JSON
// fallback, since there's nothing raw to show.
function renderMeta(details: unknown, peerId: number | null): ReactNode {
  const obj = asObj(details);
  if (!obj) {
    return <>Meta vote{peerId !== null && peerId !== undefined ? ` by guardian ${peerId}` : ''} (no decoded payload)</>;
  }
  const key = obj.key;
  const value = obj.value;
  if (key === undefined) {
    return <RawFallback details={details} />;
  }
  const keyText = typeof key === 'object' && key !== null ? JSON.stringify(key) : String(key);
  return (
    <>
      Meta vote: key <span className="font-mono">{keyText}</span>
      {value !== undefined && value !== null && (
        <>
          {' '}
          = <span className="font-mono text-xs break-all">{typeof value === 'string' ? value : JSON.stringify(value)}</span>
        </>
      )}
    </>
  );
}
