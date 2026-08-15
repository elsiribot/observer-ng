# Ecash Anonymity-Set Estimate — Design

**Status:** design (not yet planned/implemented)
**Date:** 2026-08-14
**Branch:** `modularization`
**Depends on:** the shipped ecash denomination tracking (`fmo_mint.note_denominations`, `fmo_mintv2.note_denominations`) and the gold layer (`fmo_core/src/gold.rs`, `user_transactions`).

## Goal

Attach to every ecash-spending gold transaction a defensible, cheap-to-compute
**upper bound on its anonymity set**, in bits, and surface it in the UI. The
bound must be the *tightest* (lowest) ceiling justified by real fedimint client
behavior, computable per transaction in the gold layer with indexed lookups and
closed-form arithmetic — no enumeration, subset-sum, or global flow solving.

## Background: the metric

Fedimint ecash privacy is Zerocoin-style blind-signature unlinkability: the
observer records issuance outputs (blind nonces) and spend inputs (revealed
nonces) but cannot link them. A spent note of denomination `D` is hidden among
the notes of that same denomination *in circulation at the moment it is spent*.

**Client behavior (grounded in fedimint source) makes the bound tight.**
- Denominations are powers of two msat from 1 (`fedimint-core` `tiered.rs`,
  server base 2).
- Note selection for spending is **deterministic greedy, no randomness**
  (`fedimint-mint-client` `select_notes_from_stream`); issuance/change is a
  **deterministic canonical (binary) representation** (`represent_amount`),
  typically 0-or-1 note per denomination.
- **Every input note of a fedimint transaction belongs to a single wallet /
  owner** — there is no coinjoin-style multi-owner transaction.

**The bound: weakest link (min), not sum.** The naive ceiling sums independent
per-denomination pools:

```
H_naive = Σ_D log₂( N_D! / (N_D − q_D)! )  ≈  Σ_D q_D · log₂ N_D
```

where, for the spending transaction, `q_D` = number of spent notes of
denomination `D` and `N_D` = that denomination's in-circulation pool at spend
time. But the spent notes are **not** independent — they share one owner, and
the whole spent bundle `{q_D}` is public. Deanonymizing the transaction means
pinning that one owner, who must be one of the ≤ `N_{D*}` holders of the
**rarest** spent denomination `D* = argmin_D N_D`. Once pinned there, every
other (abundant-denomination) note in the transaction is forced onto the same
owner, so the large pools add no independent entropy — they collapse onto the
rarest. Hence:

```
ecash_anon_bits(t) = MIN over spent denominations D of  log₂(N_D)
```

This is a **valid upper bound** (candidate owners ⊆ rarest-denomination
holders, so it never over-claims privacy) and strictly **tighter** than the
naive sum (`min ≤ each term ≤ Σ`), often by tens of bits when a large amount
spends one rare high denomination plus a tail of common small ones. The honest
reading: *a transaction is only as private as its scarcest spent denomination.*

**Note (revised after deploy):** `N_D` is the *crowd of possible spenders* (the
in-circulation pool of denomination D), so the per-denomination term is
`log₂(N_D)` — independent of how many notes `q_D` of that denomination the
transaction spends. An earlier version used the falling factorial
`Σ_{j<q_D} log₂(N_D − j) ≈ q_D · log₂ N_D`, which measures "which specific
notes" (a combinatorial count) rather than "which spender," and blew up to
nonsensical values (e.g. 1545 bits → `2^1545` "notes") for consolidation spends
of many notes of one denomination. Spending a large fraction of a pool in fact
*reduces* real privacy; that (holdings-based) tightening needs per-owner state
the observer cannot see and is out of scope, so we report the plain crowd size
`log₂(N_D)`.

Scope: the bound is over **spent (input) notes**. Freshly issued change/outputs
have no backward anonymity set at creation → not scored. A transaction that
spends no ecash notes (e.g. a pure peg-in) → `NULL`.

## Data model

### 1. `note_circulation` — the time-resolved pool curve (new, core/gold)

`note_denominations` (per mint module) stores only the **current cumulative**
issued/spent totals. The anon-set needs the pool **as of each historical
spend**, so we add a time-resolved curve, owned by core (it is cross-cutting and
consumed by gold):

```sql
CREATE TABLE note_circulation (
    federation_id     BYTEA  NOT NULL REFERENCES federations (federation_id),
    kind              TEXT   NOT NULL,      -- 'mint' | 'mintv2' (disjoint pools)
    denomination_msat BIGINT NOT NULL,
    session_index     INTEGER NOT NULL,     -- change point
    in_circulation    BIGINT NOT NULL,      -- cumulative (issued − spent) through this session
    PRIMARY KEY (federation_id, kind, denomination_msat, session_index)
);
```

- **Keyed by `kind`** because v1 `mint` and v2 `mintv2` notes are disjoint pools
  — a spent v1 note hides only among v1 notes of its denomination. (In practice
  a federation runs one mint module, but keying by kind keeps it correct.)
- **Change points only.** One row per session in which a `(kind, denom)` pool
  changes, storing the running `in_circulation` after that session. A lookup for
  session `s` reads the latest row with `session_index < s` (strictly before;
  see semantics below). This keeps the table sparse and the lookup a PK-prefix
  `ORDER BY session_index DESC LIMIT 1`.

### 2. `transaction_privacy` — per-fedimint-transaction score (new table)

The score is a property of a *fedimint transaction that spends ecash*, keyed by
`txid` — NOT of the deduplicated gold user-transaction. Keying by `txid` covers
every ecash spend uniformly: ecash transfers, on-chain withdrawals (peg-outs
burn notes), and **Lightning `fund` legs**. (A `user_transactions`-keyed column
misses Lightning entirely, because LN user-transactions are grained by
`contract_id`, not the fund `txid`.)

```sql
CREATE TABLE transaction_privacy (
    federation_id   BYTEA            NOT NULL,
    txid            BYTEA            NOT NULL,
    ecash_anon_bits DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (federation_id, txid),
    FOREIGN KEY (federation_id, txid) REFERENCES transactions (federation_id, txid)
);
```

A row exists only for a scored transaction; **absence = not an ecash spend** (or
no pool data before it). The score is surfaced on the transaction list rows and
the tx-detail page (join `transaction_privacy` by `(federation_id, txid)`).

## Semantics: which pool is "at spend time"

`N_D` for a spend at session `s` is the in-circulation count of `(kind, D)` **as
of the session strictly before `s`** — i.e. the latest `note_circulation` row
with `session_index < s`. Strictly-before is the conservative, unambiguous
choice: the note must have been issued before it was spent, and the
transaction's own session (its own issuance/spends and intra-session ordering,
which is not consensus-meaningful) neither inflates nor deflates its own anon
set. A pool with no data before `s` (cold start) → that denomination
contributes no term; if no denomination has a pool → `ecash_anon_bits = NULL`.

## Computing it

### Forward (incremental) — in the gold fold

`note_circulation` is maintained in the same cursor'd `fold_sessions`
(`gold.rs`) that already produces `user_transactions`. For each processed
session range, per `(federation_id, kind, denomination_msat)`:

```
Δ = (# mint/mintv2 OUTPUTS of this denom in the range)   -- issued
  − (# mint/mintv2 INPUTS  of this denom in the range)   -- spent
```

added to the running total and written as a change point at each session where
the pool changes. Because the fold processes sessions in order under a monotone
cursor, the running total when the fold reaches session `s` is exactly the pool
"at spend time"; new user-transactions get `ecash_anon_bits` computed inline via
the query below against the just-updated curve. All writes idempotent
(`ON CONFLICT DO NOTHING` / deterministic recompute), consistent with the rest
of gold.

### The per-transaction query

```sql
-- for user_transactions in the fold's [start, end) range that spend ecash
WITH tx_denoms AS (          -- q_D per spent (kind, denomination)
  SELECT ti.federation_id, ti.txid, t.session_index, ti.kind,
         ti.amount_msat AS denom, COUNT(*) AS q
  FROM transaction_inputs ti
  JOIN transactions t USING (federation_id, txid)
  WHERE ti.federation_id = $1 AND t.session_index >= $2 AND t.session_index < $3
    AND ti.kind IN ('mint','mintv2') AND ti.amount_msat IS NOT NULL
  GROUP BY ti.federation_id, ti.txid, t.session_index, ti.kind, ti.amount_msat
),
pool AS (                    -- N_D: latest change point STRICTLY BEFORE the tx's session
  SELECT d.*, (
      SELECT nc.in_circulation FROM note_circulation nc
      WHERE nc.federation_id = d.federation_id AND nc.kind = d.kind
        AND nc.denomination_msat = d.denom AND nc.session_index < d.session_index
      ORDER BY nc.session_index DESC LIMIT 1) AS n
  FROM tx_denoms d
),
bits AS (                    -- crowd of spenders per denomination: log2(pool)
  SELECT federation_id, txid, log(2.0, n) AS bits_d
  FROM pool WHERE n IS NOT NULL AND n > 0
),
scored AS (SELECT federation_id, txid, MIN(bits_d) AS min_bits FROM bits
           GROUP BY federation_id, txid)
INSERT INTO transaction_privacy (federation_id, txid, ecash_anon_bits)
SELECT federation_id, txid, min_bits FROM scored
ON CONFLICT (federation_id, txid) DO UPDATE
    SET ecash_anon_bits = EXCLUDED.ecash_anon_bits;
```

The per-denomination term is `log2(N_D)` — the crowd of possible spenders,
independent of how many notes of D the transaction spends (an earlier
falling-factorial `Σ log2(N-j)` exploded on consolidation spends; see §Metric).
Per-tx cost: group ≤ ~30 input denominations, one index lookup per denomination,
take the min. No enumeration. The upsert is keyed by `txid`, so it scores every
ecash-spending fedimint tx (incl. Lightning fund legs), not just the txid-grained
gold user-transactions.

### Backfilling existing gold transactions — reset and replay (the crux)

Old `user_transactions` already in the gold table predate this feature and must
be scored retroactively. **The live cumulative counts (`note_denominations`, or
the current tail of `note_circulation`) are "now", not "at spend time", so they
cannot be used to backfill.** The pool for a transaction from six months ago is
the pool as it was *then*, which is smaller than today's cumulative total.

Therefore the backfill must **reconstruct the time-resolved curve from zero** —
reset a per-`(federation, kind, denomination)` running accumulator to zero and
replay every session in order from the append-only core tables
(`transaction_inputs`/`transaction_outputs` + `transactions.session_index`),
emitting `note_circulation` change points as it goes. The running value when the
replay reaches session `s` is the count at spend time for transactions in `s`.

Concretely the one-time backfill:
1. **Truncate/reset `note_circulation`** and rebuild it from session 0 with a
   single windowed running-sum pass over the core tables (one `Δ` per
   `(fed, kind, denom, session)`, `SUM(...) OVER (PARTITION BY fed, kind, denom
   ORDER BY session_index)`), inserting change points. This is the "reset the
   counts table to get the count at spend time" step.
2. **Score every ecash-spending fedimint transaction into `transaction_privacy`**
   by running the per-transaction upsert above with `session_index < s`
   (strictly-before) against the now-complete historical curve — a set-based
   `INSERT … ON CONFLICT` over all history, keyed by `txid`.

Because `note_circulation` is derived purely from the append-only core tables,
the reset-and-rebuild is deterministic and idempotent (re-running produces the
identical curve); it can run inside a `note_circulation` migration or a one-off
gold heal step, before the forward fold takes over. This is the same pattern as
the denomination-table backfill, but it must produce the *whole session-indexed
curve*, not just the final totals — that is the difference the anon-set feature
forces.

## API + frontend

- Expose `ecash_anon_bits` on the **transaction list items** (`SessionItem`, via
  a `LEFT JOIN transaction_privacy` on the tx branch of the session-items,
  consensus-stream, and live-SSE queries) and on `TxDetail` — `null` for
  consensus items and non-ecash transactions.
- The **consensus / session transaction list** renders a compact badge per tx
  row; the tx-detail (`/tx/:txid`) page shows the full figure. Surface as
  "≈ N bits (≥ M notes)" with a tooltip explaining it is an *upper bound* set by
  the scarcest spent denomination and that real privacy is ≤ this.
- The score is NOT on `user_transactions` (the dedup'd gold grain) — it lives on
  the fedimint transaction (`txid`), so Lightning payments (contract_id-grained
  in gold) are covered too. A federation-level distribution histogram is a
  possible follow-up.

## Edge cases & safety

- **`min`, not `sum`, is the headline.** It is a valid upper bound; if wrong it
  under-claims privacy (safe for a privacy tool). Optionally show the sum as a
  labeled "loose ceiling."
- **`N_D` must be in-circulation (issued − spent), never issued-only.** An
  issued-only curve over-counts the pool and would *over*-claim privacy — the one
  genuinely unsafe error. Prefer a conservatively small pool when uncertain.
- **Per-kind pools.** Score a spent note against its own module's pool (`mint`
  vs `mintv2`); never merge the two.
- **Single-owner assumption is load-bearing.** The weakest-link collapse holds
  only because all inputs share one owner. Guard the metric on module kind; if a
  future fedimint version introduces multi-party transactions, revert to the
  subadditive sum for those.
- **Strictly-before session** avoids a transaction inflating/deflating its own
  anon set.
- **Non-canonical / patched clients** that spend a non-minimal note set only
  ever raise their real privacy above the bound → still safe.
- **Small pools / cold start:** when `N_D` is 1–2 for a scarce denomination the
  bits correctly drop toward 0 — a real privacy warning, report it; `NULL` only
  when no ecash notes are spent or no pool data exists before the session.
- **Undecoded (NULL `amount_msat`) notes** are excluded symmetrically from both
  the curve and the `q_D` grouping, matching every other aggregate over these
  tables.

## Out of scope (noted, not built)

- Holdings-dependent tightening (buffer target 2–3/tier, consolidate >8 keep 4)
  — needs per-owner state the observer cannot see; would lower the base below
  `N_D` but is not safely computable.
- Out-of-band (`spend_notes_oob`) cross-transaction linkage; each reissue is
  bounded correctly on its own.
- Amount/change recovery from canonical representation, and offline
  Sinkhorn/Bethe-permanent marginals for a single principled number between the
  loose and tight ceilings — research-grade, never in the gold hot path.

## Testing

- **`note_circulation` build:** seed core mint/mintv2 inputs/outputs across
  several sessions and denominations; assert the change-point curve matches a
  hand-computed running `issued − spent`, and that a strictly-before lookup
  returns the pool as of the prior session.
- **Bound:** a transaction spending one rare high-denomination note + several
  common low ones asserts `ecash_anon_bits == log₂(N_rare)` (the min), not the
  sum; a `q_D > 1` case exercises the falling-factorial term; a no-ecash tx →
  `NULL`; a cold-start spend with no prior pool → `NULL`.
- **Backfill idempotence:** running the reset-and-replay twice yields identical
  `note_circulation` and identical `ecash_anon_bits`.
