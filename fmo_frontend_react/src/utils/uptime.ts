// Shared formatting for the threshold-aware federation-uptime badge, used both
// on the federation-detail page (guardians panel header) and in the home-page
// federation list row.

// Tailwind classes for the federation-uptime badge, keyed on the operable
// percentage. Semantic (green good / amber warning / red critical), separate
// from the accent hue.
export function uptimeBadgeClasses(pct: number): string {
  if (pct >= 99.9) {
    return 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300';
  }
  if (pct >= 99) {
    return 'bg-green-100 text-green-700 dark:bg-green-900/40 dark:text-green-300';
  }
  if (pct >= 95) {
    return 'bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300';
  }
  return 'bg-red-100 text-red-700 dark:bg-red-900/40 dark:text-red-300';
}

// Show more precision as uptime approaches 100% (99.97% reads better than
// 100%), but avoid rounding a real outage up to a clean "100%".
export function formatUptimePct(pct: number): string {
  if (pct >= 99.995) {
    return '100%';
  }
  if (pct >= 99) {
    return `${pct.toFixed(2)}%`;
  }
  return `${pct.toFixed(1)}%`;
}
