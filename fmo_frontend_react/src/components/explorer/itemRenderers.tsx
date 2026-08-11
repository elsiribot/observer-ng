/* eslint-disable react-refresh/only-export-components --
 * This module's public surface is `renderItem`, a plain dispatch function
 * (not a component) that composes several small internal-only row
 * components below. That mix is intentional here (it's a renderer registry,
 * not a page), so Fast Refresh boundary purity doesn't apply. */
import { useState } from 'react';
import type { ReactNode } from 'react';
import { Link, useParams } from 'react-router-dom';
import { Badge } from '../Badge';
import { api } from '../../services/api';
import { formatNumber } from '../../utils/format';
import type { SessionItem, TxDetail } from '../../types/api';

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

function shorten(hex: string, chars = 8): string {
  if (hex.length <= chars * 2 + 1) {
    return hex;
  }
  return `${hex.slice(0, chars)}…${hex.slice(-chars)}`;
}

// ---- Transaction row -------------------------------------------------

function TransactionRow({ item }: { item: SessionItem }) {
  const { id: federationId } = useParams<{ id: string }>();
  const [expanded, setExpanded] = useState(false);
  const [detail, setDetail] = useState<TxDetail | null>(null);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [detailError, setDetailError] = useState<string | null>(null);

  const handleToggle = () => {
    const next = !expanded;
    setExpanded(next);
    if (next && !detail && !loadingDetail && federationId && item.txid) {
      setLoadingDetail(true);
      setDetailError(null);
      api
        .getTxDetail(federationId, item.txid)
        .then(setDetail)
        .catch((err: unknown) => {
          setDetailError(err instanceof Error ? err.message : 'Failed to load transaction details');
        })
        .finally(() => setLoadingDetail(false));
    }
  };

  return (
    <div className="py-3">
      <div className="flex flex-col sm:flex-row sm:items-center gap-2">
        <Badge level="success">Transaction</Badge>
        <span
          className="font-mono text-xs text-gray-700 dark:text-gray-300 truncate"
          title={item.txid ?? undefined}
        >
          {item.txid ? shorten(item.txid) : 'unknown txid'}
        </span>
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
        <div className="mt-2 text-xs text-gray-700 dark:text-gray-300">
          {loadingDetail && <span>Loading…</span>}
          {detailError && <span className="text-red-500 dark:text-red-400">{detailError}</span>}
          {detail && (
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
              <TxPartsList title="Inputs" parts={detail.inputs} />
              <TxPartsList title="Outputs" parts={detail.outputs} />
            </div>
          )}
        </div>
      )}
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
            {part.amount_msat !== null ? ` — ${formatNumber(part.amount_msat)} msat` : ''}
          </li>
        ))}
      </ul>
    </div>
  );
}

// ---- Consensus item row -----------------------------------------------

function ConsensusItemRow({ item }: { item: SessionItem }) {
  return (
    <div className="flex flex-col sm:flex-row sm:items-center gap-1 sm:gap-3 py-3">
      <div className="flex items-center gap-2 shrink-0">
        <Badge level="info">{item.kind ?? 'ci'}</Badge>
        {item.peer_id !== null && item.peer_id !== undefined && (
          <span className="text-xs text-gray-500 dark:text-gray-400">Guardian {item.peer_id}</span>
        )}
      </div>
      <div className="flex-1 min-w-0 text-sm text-gray-900 dark:text-white">{safeCiBody(item)}</div>
    </div>
  );
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
  return (
    <details>
      <summary className="cursor-pointer text-xs text-gray-500 dark:text-gray-400">
        Raw details
      </summary>
      <pre className="mt-1 text-xs overflow-x-auto whitespace-pre-wrap break-words bg-gray-50 dark:bg-gray-900 p-2 rounded">
        {JSON.stringify(details, null, 2)}
      </pre>
    </details>
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
          <span className="font-mono text-xs" title={contractId}>
            {shorten(contractId)}
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
