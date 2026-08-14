# Ecash Denomination Tracking + Histogram Tab — Design

**Status:** approved for implementation
**Date:** 2026-08-14
**Branch:** `modularization` (no PR; deploy to runner-01 per repo convention)

## Goal

Track, per federation, how many ecash notes of each denomination have ever been
issued and how many are currently in circulation, and surface it as a new
"Ecash" tab on the federation detail page: a histogram of note denominations
with two series — *ever issued* and *in circulation* — on separate Y-axes.

## Background: the data is already there

In Fedimint, ecash notes come in fixed power-of-2 denominations (in msat). In
the observer's core structural tables:

- Each **mint output** (`transaction_outputs` where `kind='mint'`) is exactly
  one note minted at denomination `amount_msat` → "issued".
- Each **mint input** (`transaction_inputs` where `kind='mint'`) is exactly one
  note spent at denomination `amount_msat` → "spent".

Confirmed one-note-per-item by `fmo_module_mint/src/lib.rs`: `process_output`
reads a single `output.maybe_v0_ref().amount`, `process_input` a single
`input.maybe_v0_ref().amount`, and the existing nonce lookup treats one input as
one note (`details -> 'V0' -> 'note' -> 'nonce'`).

Therefore, per denomination:
- `issued`  = count of mint outputs at that denomination
- `spent`   = count of mint inputs at that denomination
- `in_circulation` = `issued - spent` (clamped at ≥ 0; see edge cases)

## Approach: incremental counts, not a rescan

The mint module currently owns no tables (its amounts/details live in the core
tables). We give it its first schema — a small **counts table**, maintained
incrementally, seeded once from history.

### Why incremental (not a materialized view refreshed on the cycle)

A matview would re-aggregate the largest tables in the database on every ~60s
refresh cycle, needing a supporting index to stay cheap. Instead we fold the
counts into the module's per-item processing, exactly as `fmo_module_walletv2`
maintains its tables. This is safe because `dispatch::process_module_batch`
commits the module writes **and** the `module_progress` cursor advance in one
transaction (`fmo_core/src/dispatch.rs`), so each item is processed
exactly-once: a crash rolls back both the increment and the cursor, and resume
re-counts once. Steady state is then O(1) per item with no periodic scan and no
extra index on the core tables.

## Backend

### 1. Counts table + one-time history backfill

New file `fmo_modules/fmo_module_mint/schema/v0.sql`:

```sql
CREATE TABLE note_denominations
(
    federation_id     BYTEA  NOT NULL REFERENCES public.federations (federation_id),
    denomination_msat BIGINT NOT NULL,
    issued            BIGINT NOT NULL DEFAULT 0,
    spent             BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (federation_id, denomination_msat)
);

-- One-time backfill from the core tables so historical notes are counted
-- without a module version bump / full replay. Runs once, at deploy, inside
-- the migration (before the per-federation processors spawn), so it counts
-- everything up to the current module cursor; incremental increments then take
-- over from the cursor forward -- no gap, no double-count.
INSERT INTO note_denominations (federation_id, denomination_msat, issued, spent)
SELECT COALESCE(o.federation_id, i.federation_id),
       COALESCE(o.denom, i.denom),
       COALESCE(o.n, 0),
       COALESCE(i.n, 0)
FROM (SELECT federation_id, amount_msat AS denom, COUNT(*) AS n
      FROM transaction_outputs
      WHERE kind = 'mint' AND amount_msat IS NOT NULL
      GROUP BY federation_id, amount_msat) o
FULL OUTER JOIN (SELECT federation_id, amount_msat AS denom, COUNT(*) AS n
                 FROM transaction_inputs
                 WHERE kind = 'mint' AND amount_msat IS NOT NULL
                 GROUP BY federation_id, amount_msat) i
  ON o.federation_id = i.federation_id AND o.denom = i.denom;
```

`schema_name` sets `search_path TO fmo_mint, public`, so the table lands in
`fmo_mint` and the backfill reads the unqualified core tables.

### 2. Module wiring (`fmo_module_mint/src/lib.rs`)

- `version()` stays **1** (no replay; the migration applies via the
  per-migration `schema_version` cursor in `setup_module_schema`).
- `migrations()` returns one `Migration { sql: include_str!(".../schema/v0.sql") }`.
- `process_output`: when `amount` is `Some`, before returning, upsert issued:

  ```rust
  ctx.dbtx.execute(
      "INSERT INTO note_denominations (federation_id, denomination_msat, issued, spent)
       VALUES ($1, $2, 1, 0)
       ON CONFLICT (federation_id, denomination_msat)
       DO UPDATE SET issued = note_denominations.issued + 1",
      &[&meta.federation_id.consensus_encode_to_vec(), &(amount.msats as i64)],
  ).await?;
  ```
  (`meta` is currently `_meta`; rename to use it. `amount` is
  `fedimint_core::Amount`; `amount.msats` is the denomination.)
- `process_input`: same, incrementing `spent` (`VALUES ($1,$2,0,1)` /
  `DO UPDATE SET spent = note_denominations.spent + 1`).
- No `matviews()` override.

### 3. Endpoint

Add to the module `api_router`: `GET /denominations` → mounted at
`/federations/:federation_id/modules/mint/denominations`.

```rust
Router::new()
    .route("/nonces/spend", post(get_nonces_spend_info))
    .route("/denominations", get(get_denominations))
```

Handler reads the counts table for the federation and returns denominations
sorted ascending:

```sql
SELECT denomination_msat,
       issued,
       GREATEST(issued - spent, 0) AS in_circulation
FROM note_denominations
WHERE federation_id = $1
ORDER BY denomination_msat
```

Returns `Vec<MintDenomination>`. A federation with no mint notes (or no mint
module) yields an empty vec (HTTP 200), which the frontend renders as an empty
state.

### 4. API type (`fmo_api_types/src/lib.rs`)

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MintDenomination {
    /// Note denomination in millisatoshis (a power of two).
    pub denomination_msat: u64,
    /// Total notes of this denomination ever minted.
    pub issued: u64,
    /// Notes of this denomination currently unspent (issued - spent, >= 0).
    pub in_circulation: u64,
}
```

## Frontend

### Tab

Add `'ecash'` to the `activeTab` union in `pages/FederationDetail.tsx`, a tab
button labelled **"Ecash"** (between "Gateways" and "Config"), and a render
branch `{activeTab === 'ecash' && id && <EcashTab federationId={id} />}`.

### `services/api.ts`

```ts
getMintDenominations: (federationId: string) =>
  request<MintDenomination[]>(`/federations/${federationId}/modules/mint/denominations`),
```
(with `MintDenomination` added to `types/api.ts`).

### Components

- `components/EcashTab.tsx` — fetches denominations, handles loading/error/empty,
  renders a compact summary line (total notes issued, total in circulation, and
  total value of each = Σ count×denomination) plus the chart.
- `components/EcashDenominationsChart.tsx` — ECharts (`echarts-for-react`,
  matching `GuardianLatencyChart`) grouped **bar** chart:
  - X axis (category): denomination, formatted (msat/sat) ascending.
  - Two Y axes: left = *ever issued*, right = *in circulation*; two bar series,
    each bound to its own axis so the magnitude gap stays readable.
  - Tooltip shows denomination + both counts.
- The ECharts option construction is a pure exported function
  `buildDenominationChartOption(data)` so it can be unit-tested without a DOM.
- A denomination formatter (`formatDenomination(msat)`) — reuse `utils/format`
  where possible; powers of 2 in msat, shown as sats when a whole/near-whole sat
  and msat otherwise.

## Testing

- **Backend** (`fmo_modules/fmo_module_mint/tests/` — new): seed
  `transaction_outputs`/`transaction_inputs` with `kind='mint'` at known
  denominations (e.g. 1000 msat ×3 issued, ×1 spent; 2000 msat ×2 issued),
  apply the mint schema, and assert the denominations query returns the expected
  `issued` / `in_circulation` per denomination, ordered. Follow the harness in
  `fmo_module_walletv2/tests/process.rs`.
- **Frontend**: unit-test `buildDenominationChartOption` (maps N denominations →
  category axis + two series bound to axis 0/1, issued on left, in_circulation on
  right) and `formatDenomination` (msat vs sat rendering), in the style of
  `utils/sortFederations.test.ts`.

## Edge cases & notes

- **`in_circulation` never negative**: clamped with `GREATEST(..., 0)`. It could
  in principle go slightly negative for a denomination if a note's issuance
  output has a NULL (undecoded) amount while its spend input is decoded; both the
  backfill (`WHERE amount_msat IS NOT NULL`) and the increments (only when
  `amount` is `Some`) skip NULL-amount items symmetrically, so this is a
  negligible edge, same as any aggregate over these tables faces.
- **Deploy cost**: the one-time backfill is a single fleet-wide aggregate over
  the mint rows, run inside the migration at startup (tens of seconds,
  comparable to the existing consensus index build). After that, steady state is
  O(1) per processed item; reads are a PK-range lookup.
- **No version bump / no replay**: the cursor is untouched; the backfill counts
  everything the module has already processed (present in the core tables up to
  the cursor), and increments handle everything from the cursor forward.
- **Fresh import**: on a database built via `fmo_server import` + full replay,
  the mint schema is created before processing starts, so the backfill counts
  zero rows and every note is counted incrementally as it is processed — same
  final result.

## Out of scope

- Time-resolved (per-session) in-circulation history and anonymity-set analysis
  (being researched separately). This design stores only cumulative totals.
