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

// Render a duration in seconds as a compact spread label: `s`/`m`/`h`/`d`,
// picking the largest unit that keeps the number small (e.g. 240 -> "4m").
export function humanizeSpread(seconds: number): string {
  const s = Math.abs(seconds);
  if (s < 60) {
    return `${Math.round(s)}s`;
  }
  if (s < 3600) {
    return `${Math.round(s / 60)}m`;
  }
  if (s < 86400) {
    return `${Math.round(s / 3600)}h`;
  }
  return `${Math.round(s / 86400)}d`;
}

// Display for an item's estimated time with its uncertainty, given the
// resolved fields from the API (`estimated_time`/`time_lower`/`time_upper`/
// `time_source`). Returns a compact `text` for inline rendering plus an
// optional `title` (the full range, for a hover tooltip).
//
// - "observed" or a zero-width interval (lower == upper): the exact time, no ±.
// - "interpolated" with both bounds: `≈ <midpoint> ·±<spread>` where the
//   spread is half the interval width; `title` is the full `lower – upper`
//   range.
// - "interpolated" with an unbounded upper (null): `≳ <lower>` ("after").
// - no info: "unknown".
export interface EstimatedTimeDisplay {
  text: string;
  title: string | null;
}

export function formatEstimatedTime(item: {
  estimated_time: number | null;
  time_lower: number | null;
  time_upper: number | null;
  time_source: string | null;
}): EstimatedTimeDisplay {
  const { estimated_time, time_lower, time_upper, time_source } = item;

  // No time information at all.
  if (time_source === null || estimated_time === null) {
    return { text: 'unknown', title: null };
  }

  // Exact: live-observed, or a zero-width (directly-voted) interval.
  if (
    time_source === 'observed' ||
    (time_lower !== null && time_lower === time_upper)
  ) {
    return { text: formatTimestamp(estimated_time), title: null };
  }

  // Interpolated between two votes: midpoint estimate with a ± spread.
  if (time_lower !== null && time_upper !== null) {
    const spread = (time_upper - time_lower) / 2;
    return {
      text: `≈ ${formatTimestamp(estimated_time)} ·±${humanizeSpread(spread)}`,
      title: `${formatTimestamp(time_lower)} – ${formatTimestamp(time_upper)}`,
    };
  }

  // Interpolated with an unbounded upper (no later vote yet).
  if (time_lower !== null) {
    return {
      text: `≳ ${formatTimestamp(time_lower)}`,
      title: `after ${formatTimestamp(time_lower)}`,
    };
  }

  // Defensive fallback.
  return { text: 'unknown', title: null };
}

// Calculate rating index for sorting
export function ratingIndex(count: number, avg: number | null): number {
  return (avg || 0) * Math.log10((count || 0) + 1);
}
