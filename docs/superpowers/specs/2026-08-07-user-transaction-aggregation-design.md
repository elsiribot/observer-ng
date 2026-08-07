# User-Transaction Deduplication & Aggregation — Design Spec

**Date:** 2026-08-07
**Status:** approved design (pending implementation plan)

## Goal

Estimate the *actual* user-facing transaction count and volume for each
federation by collapsing the multiple fedimint transactions a single user
action generates (most notably LN payments, which span 2–3 fedimint txs).
Expose the result as **incrementally-maintained, directly-queryable tables**.

## Why: one user action ≠ one fedimint transaction

Measured tx-shape distribution (input kinds → output kinds) over the live
data confirms the decomposition:

| Raw fedimint tx shape | ~count | Role |
|---|--:|---|
| `mint → ln` / `mint → ln,mint` | ~442k | LN **fund** leg (+change) |
| `ln → mint` / `ln,mint → mint` | ~439k | LN **claim** leg |
| `(no inputs) → ln` | ~336k | LN **offer** leg (0 value) |
| `mint → mint` | ~83k | ecash transfer |
| `wallet → mint` | ~1.1k | peg-in (deposit) |
| `mint → wallet` | ~1.8k | peg-out (withdrawal) |
| `mintv2 → lnv2,mintv2` etc. | ~4k | lnv2 fund/claim |
| `mint → *stability_pool*` | ~1.5k | stability-pool in/out |

So **one LN receive = up to 3 fedimint txs** (offer + fund + claim), **one LN
send = 2** (fund + claim). Counting raw txs double/triple-counts LN and, if
both input and output sides are summed, double-counts volume.

## Definitions

- **User transaction** — one economic action a user initiated, regardless of
  how many fedimint txs realized it.
- **`amount_msat`** — the primary value of that action, counted **once**
  (never input-side + output-side, never fund-leg + claim-leg). Fees are
  tracked separately, not folded into `amount_msat`.

## Taxonomy & deduplication rules

Two-pass classification of every raw fedimint tx:

1. **LN-leg txs** (txid appears in `fmo_ln`/`fmo_lnv2` contract-link tables)
   → grouped by **`contract_id`** into one user tx. Offer/fund/claim/cancel/
   refund all fold in.
2. **All other txs** → 1:1 user tx, classified by input/output **kind
   signature**.

| Type | Dedup key | Legs folded | `amount_msat` | Direction | `status` |
|---|---|---|---|---|---|
| `ln_send` | `(fed, ln contract_id)`, type=outgoing | fund+claim(+cancel/refund) | contract amount | out | see below |
| `ln_receive` | `(fed, ln contract_id)`, type=incoming | offer+fund+claim | contract amount | in | see below |
| `lnv2_send` / `lnv2_receive` | `(fed, lnv2 contract_id)` | fund+claim | contract amount | out/in | see below |
| `peg_in` | `txid` | — | wallet **input** amount | in | completed |
| `peg_out` | `txid` | — | wallet **output** amount | out | completed |
| `ecash_transfer` | `txid` | — | mint input amount | internal | completed |
| `stability_pool` | `txid` | — | amount (inferred) | in/out | completed |
| v2 mint/wallet analogues | `txid` | — | per above | — | completed |

### Decisions folded in (from review)

- **`mint → mint` is treated as external `ecash_transfer`** (a real user
  payment), **not** internal housekeeping. Caveat retained below: a fraction
  are note-refreshes with no external counterparty; we accept the over-count
  because the majority are real transfers in these federations.
- **Stranded LN contracts are `in_flight`, not failed.** A funded-but-unclaimed
  contract is a payment still in progress (could still complete), so:
  - `status` ∈ { `completed` (claimed/refunded), `in_flight` (funded, no spend
    yet), `cancelled` (CancelOutgoing present, not yet refunded) }.
  - `in_flight` volume = value currently locked in unclaimed contracts.

### Fees (two separate columns)

- **`fedimint_fee_msat` — exact.** A fedimint tx balances, so the federation's
  fee = `Σ input amount_msat − Σ output amount_msat` per leg, summed across the
  user tx's legs.
- **`gateway_fee_msat` — estimate, LN only.** The gateway's LN markup is
  **off-ledger**: `OutgoingContract` carries no invoice amount, only the
  contract amount the user locked. Estimated from the paying gateway's
  advertised fee schedule (`fmo_ln.gateways.raw → fees.base_msat +
  proportional_millionths`, matched by `gateway_key`):
  `gw_fee ≈ base + ppm·invoice/1e6`, solving `contract = invoice + gw_fee`.
  NULL for non-LN and when the gateway/fee schedule is unknown.
  **Caveat:** advertised ≠ actually-charged, and we only poll *current*
  schedules, so historical estimates drift. This column is explicitly an
  estimate; `fedimint_fee_msat` is not.

## Architecture: incremental gold layer

The gold layer is inherently cross-module (needs core `transactions` +
`fmo_ln`/`fmo_lnv2`/`fmo_wallet` silver together), so it is the
denormalization/"gold" tier of the issue-#8 medallion model.

**Incrementally maintained** (per review decision — not a full-recompute
matview):

- A new core **`user_tx` processor** with its own per-federation cursor
  (`gold_progress`, mirroring `module_progress`) that **trails the min of the
  relevant module cursors**, so a session is only folded into the gold layer
  after every module has written its silver rows for it.
- Per session, for each tx:
  - **LN/LNv2 leg** → *recompute-and-upsert the whole user tx for that
    `contract_id` from silver* (idempotent regardless of leg order or replay).
  - **standalone** → upsert a user tx keyed by `txid`, classified by kind
    signature.
- Idempotent (`ON CONFLICT`), crash/replay-safe (same as module dispatch).
- **Graceful degradation:** LN grouping only runs where `fmo_ln`/`fmo_lnv2`
  exist; absent modules just mean those types don't appear.

Recompute-per-contract (rather than incremental status flags) is what buys
idempotency: reprocessing any leg reproduces the identical final row.

## Tables (the deliverable — query these directly)

1. **`user_transactions`** (grain, one row per deduped user tx):
   `federation_id, user_tx_key, kind (ln_send/…), direction, amount_msat,
   fedimint_fee_msat, gateway_fee_msat, num_fedimint_txs, first_session_index,
   first_timestamp, last_timestamp, status`.
   Key = `contract_id` for LN, `txid` otherwise; `(federation_id, user_tx_key)`
   unique.
2. **`user_tx_daily`** (rollup for dashboards):
   `federation_id, date, kind, direction, status, tx_count, volume_msat,
   fedimint_fee_msat, gateway_fee_msat`. Maintained from `user_transactions`.
3. *(optional)* **`user_tx_federation_totals`** — all-time count + volume +
   fees per federation per kind.

"Actual volume" = `SUM(amount_msat)`; "actual count" = row count — both against
`user_transactions`, ~2–3× below raw fedimint tx counts for LN-heavy feds.

## Known limitations (documented, not blockers)

1. `ecash_transfer` over-counts real payments by the share that are automatic
   note-refreshes — no consensus signal separates them (accepted per decision).
2. `gateway_fee_msat` is an estimate from advertised fees (see Fees).
3. A rare atomic multi-module tx (e.g. fund + peg-out) is attributed to each
   linked user tx and documented where it occurs.
4. Double-funded contracts (recurringd resets) collapse to one user tx per
   `contract_id`; the second funding is not a separate user tx (rare: 9 total).

## Implementation phases

1. **Schema + gold cursor + processor skeleton** — `gold_progress`,
   `user_transactions` table, standalone (non-LN) 1:1 classification by kind
   signature, `fedimint_fee_msat`. Tests on peg-in/out/ecash shapes.
2. **LN/LNv2 contract grouping** — recompute-per-contract, `status`,
   `num_fedimint_txs`, timestamps. Tests on offer/fund/claim collapse.
3. **`gateway_fee_msat` estimate** — join to gateway fee schedule.
4. **Rollups** (`user_tx_daily`, totals) + optional API endpoints.
