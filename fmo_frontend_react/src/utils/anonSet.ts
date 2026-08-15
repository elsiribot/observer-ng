// Formats the upper-bound ecash anonymity-set estimate: "≈ X bits (≥ N notes)".
// `bits` is log2 of the weakest-link in-circulation pool; N = 2^bits (a lower
// bound on the crowd the spent note hides in). Null when not applicable.
export function formatAnonSet(bits: number | null): string | null {
  if (bits === null || bits === undefined) {
    return null;
  }
  const notes = Math.round(2 ** bits);
  return `≈ ${bits.toFixed(1)} bits (≥ ${notes.toLocaleString()} notes)`;
}
