// Formats the upper-bound ecash anonymity-set estimate: "≈ X bits (≥ N notes)".
// `bits` is log2 of the weakest-link in-circulation pool; N = 2^bits (a lower
// bound on the crowd the spent note hides in). Null when not applicable.
// Used on the tx-detail page (the full, precise figure).
export function formatAnonSet(bits: number | null): string | null {
  if (bits === null || bits === undefined) {
    return null;
  }
  const notes = Math.round(2 ** bits);
  return `≈ ${bits.toFixed(1)} bits (≥ ${notes.toLocaleString()} notes)`;
}

// Formats a number with an SI suffix (k, M, G, …) and 3 significant figures.
// Values below 1000 are shown as-is (integers).
export function formatSi(n: number): string {
  if (n < 1000) {
    return `${n}`;
  }
  const units = ['', 'k', 'M', 'G', 'T', 'P', 'E'];
  const tier = Math.min(units.length - 1, Math.floor(Math.log10(n) / 3));
  const scaled = n / 1000 ** tier;
  // 3 significant figures; strip trailing zeros only after a decimal point
  // (so "2.50"→"2.5", "2.00"→"2", but "150" stays "150").
  let s = scaled.toPrecision(3);
  if (s.includes('.')) {
    s = s.replace(/0+$/, '').replace(/\.$/, '');
  }
  return `${s}${units[tier]}`;
}

// Compact anon-set SIZE for the transaction list: round the bits DOWN to the
// next whole bit and show the resulting crowd size (2^floor(bits)) with 3
// significant figures + SI suffix, e.g. 11.87 bits → 2048 → "2.05k". A
// conservative lower bound (the true set is ≥ this). Null when not applicable.
export function formatAnonSetCount(bits: number | null): string | null {
  if (bits === null || bits === undefined) {
    return null;
  }
  return formatSi(2 ** Math.floor(bits));
}
