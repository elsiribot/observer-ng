// Convert millisatoshis to Bitcoin with specified decimal places
export function asBitcoin(msats: number, decimals: number = 6): string {
  const btc = msats / 100_000_000_000;
  return `${btc.toFixed(decimals)} BTC`;
}

// Convert millisatoshis to Bitcoin number only (no BTC suffix)
export function toBitcoin(msats: number, decimals: number = 6): string {
  const btc = msats / 100_000_000_000;
  return btc.toFixed(decimals);
}

// Convert millisatoshis to satoshis for display (whole sats, thousand-separated).
// Sub-satoshi remainders (msat) are dropped — amounts on-chain/in-Fedimint are
// effectively sat-granular for display purposes.
export function asSats(msats: number): string {
  const sats = Math.round(msats / 1000);
  return `${sats.toLocaleString('en-US')} sats`;
}

// Format numbers with thousand separators
export function formatNumber(num: number): string {
  return num.toLocaleString('en-US');
}

// Format a unix-epoch-seconds timestamp for display, or "unknown" when the
// value is unavailable (e.g. a session that hasn't received a time vote yet).
export function formatTimestamp(unixSeconds: number | null): string {
  if (unixSeconds === null) {
    return 'unknown';
  }
  return new Date(unixSeconds * 1000).toLocaleString();
}

// Format a unix-epoch-seconds timestamp as a compact relative age (e.g. for
// the consensus explorer's session dividers). '' when unavailable.
export function timeAgo(unixSeconds: number | null): string {
  if (unixSeconds === null) {
    return '';
  }
  const diffSeconds = Math.max(0, Math.round(Date.now() / 1000 - unixSeconds));
  if (diffSeconds < 30) {
    return 'just now';
  }
  if (diffSeconds < 60) {
    return `${diffSeconds}s ago`;
  }
  const diffMinutes = Math.floor(diffSeconds / 60);
  if (diffMinutes < 60) {
    return `${diffMinutes}m ago`;
  }
  const diffHours = Math.floor(diffMinutes / 60);
  if (diffHours < 24) {
    return `${diffHours}h ago`;
  }
  const diffDays = Math.floor(diffHours / 24);
  return `${diffDays}d ago`;
}

// Calculate rating index for sorting
export function ratingIndex(count: number, avg: number | null): number {
  return (avg || 0) * Math.log10((count || 0) + 1);
}
