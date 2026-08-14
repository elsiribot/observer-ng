import type { FederationSummary } from '../types/api';
import { ratingIndex } from './format';

export type SortKey = 'reputation' | 'uptime' | 'volume' | 'balance' | 'tx_count' | 'name';
export type SortDirection = 'asc' | 'desc';

// Default sort matches what the home page has always shown: federations ranked
// by nostr reputation, best first.
export const DEFAULT_SORT_KEY: SortKey = 'reputation';

// Sort options in display order, each with the direction that "best first"
// implies for that metric (biggest numbers first, names A->Z).
export const SORT_OPTIONS: {
  key: SortKey;
  label: string;
  defaultDirection: SortDirection;
}[] = [
  { key: 'reputation', label: 'Reputation', defaultDirection: 'desc' },
  { key: 'uptime', label: 'Uptime', defaultDirection: 'desc' },
  { key: 'volume', label: 'Volume', defaultDirection: 'desc' },
  { key: 'balance', label: 'Balance', defaultDirection: 'desc' },
  { key: 'tx_count', label: 'Tx Count', defaultDirection: 'desc' },
  { key: 'name', label: 'Name', defaultDirection: 'asc' },
];

export function defaultDirectionFor(key: SortKey): SortDirection {
  return SORT_OPTIONS.find((o) => o.key === key)?.defaultDirection ?? 'desc';
}

// Reputation score used for ranking; mirrors the historical home-page sort
// (`ratingIndex`, which weights the average by vote count). `null` for
// federations with no votes, which always sort last (see `compareFederations`).
function reputationScore(fed: FederationSummary): number | null {
  return fed.nostr_votes.avg === null
    ? null
    : ratingIndex(fed.nostr_votes.count, fed.nostr_votes.avg);
}

function numericMetric(fed: FederationSummary, key: SortKey): number {
  switch (key) {
    case 'volume':
      return fed.total_volume;
    case 'balance':
      return fed.deposits;
    case 'tx_count':
      return fed.total_tx_count;
    default:
      return 0;
  }
}

// Comparator for two federations under a given sort key/direction.
// Numeric metrics compare by magnitude; `name` compares case-insensitively.
// For `reputation`, federations with no votes (avg === null) always sort LAST,
// regardless of direction.
export function compareFederations(
  a: FederationSummary,
  b: FederationSummary,
  key: SortKey,
  direction: SortDirection
): number {
  const dir = direction === 'asc' ? 1 : -1;

  if (key === 'name') {
    const an = (a.name ?? '').toLowerCase();
    const bn = (b.name ?? '').toLowerCase();
    return an.localeCompare(bn) * dir;
  }

  if (key === 'reputation') {
    const as = reputationScore(a);
    const bs = reputationScore(b);
    // No-vote federations sink to the bottom no matter the direction.
    if (as === null && bs === null) return 0;
    if (as === null) return 1;
    if (bs === null) return -1;
    return (as - bs) * dir;
  }

  if (key === 'uptime') {
    const au = a.uptime_pct;
    const bu = b.uptime_pct;
    // Federations with no health samples yet sink to the bottom no matter the
    // direction, like no-vote federations under `reputation`.
    if (au === null && bu === null) return 0;
    if (au === null) return 1;
    if (bu === null) return -1;
    return (au - bu) * dir;
  }

  return (numericMetric(a, key) - numericMetric(b, key)) * dir;
}

// Returns a new, sorted array (does not mutate the input).
export function sortFederations(
  feds: FederationSummary[],
  key: SortKey,
  direction: SortDirection
): FederationSummary[] {
  return [...feds].sort((a, b) => compareFederations(a, b, key, direction));
}
