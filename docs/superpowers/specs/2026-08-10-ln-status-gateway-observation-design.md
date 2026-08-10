# LN Per-Payment Status + Gateway Observation Design

**Date:** 2026-08-10
**Status:** Approved, ready for planning
**Branch:** modularization (no PR — local branch only, per standing constraint)

## Where this fits

This is **SP‑3**, the first of three sub-projects decomposed from a larger feature request.
The other two get their own spec → plan → implementation cycles later and are **out of scope
here**:

- **SP‑1 — Session & Consensus Explorer** (paginated/filterable session + consensus-item views).
- **SP‑2 — Live consensus view** (subscribe to the pending session via `get_session_status`,
  stream items to the browser). Depends on SP‑1.

SP‑3 is independent backend/module work and can proceed in parallel with the others.

## Goal

Two tightly-related module-layer capabilities for Lightning:

1. **Per-payment status** — an authoritative terminal status for every LN contract (LNv1 and
   LNv2), owned and computed *by the LN modules*, updated incrementally in Rust as legs are
   processed, and exposed on the module's own data.
2. **Gateway observation for LNv2** — bring LNv2 to parity with LNv1's gateway monitoring, and
   in doing so add **real gateway-API reachability probing** (not just registry presence) to
   both, via a shared, extracted polling harness.

## Context / Current State

- **LN modules own contract lifecycle primitives, but no status.**
  - `fmo_ln.contracts(federation_id, contract_id, type∈{incoming,outgoing}, payment_hash)`;
    `output_contracts.interaction_kind ∈ {fund, cancel, offer}`; `input_contracts` (claims /
    refunds, by outpoint); `decryption_shares` + the `contract_decryption` matview (preimage
    decryption progress per incoming contract).
  - `fmo_lnv2.contracts(federation_id, contract_id, type, amount_msat, txid, out_index)`;
    `input_outpoints(type∈{incoming,outgoing}, outpoint_txid, outpoint_out_index)` — note `type`
    is the *contract* type, **not** claim-vs-refund; the input variant is not currently recorded.
- **Status today lives only in the gold tier, coarse.** `gold.rs` (`fold_ln_v1`/`fold_lnv2`,
  core, non-modular) re-derives `completed / in_flight / cancelled` from the contract lifecycle
  and writes it to `user_transactions.status`; `user_tx_daily` rolls up by it. Current values:
  ln_receive completed 163,788 / in_flight 212; ln_send completed 278,937 / in_flight 20 /
  cancelled 15; lnv2 similar. This is a smell — LN lifecycle semantics living in core.
- **Gateway observation exists for LNv1 only** (`fmo_module_ln/gateways.rs`, ported from upstream
  PR #109): a 5-minute poller hits the federation's `LIST_GATEWAYS_ENDPOINT`, upserts
  `fmo_ln.gateways` (gateway_id, node_pub_key, api_endpoint, lightning_alias, vetted, raw, first/
  last_seen), and writes one `gateway_poll_snapshots(federation_id, gateway_id, poll_time,
  is_seen)` row per gateway per poll. **`is_seen` = "present in the federation registry"** — the
  poller does NOT contact the gateway's own API, so today's "uptime" is registration presence,
  not reachability. Rich `GatewayInfo`/`GatewayUptimeMetrics`/`GatewayActivityMetrics` API types
  already exist in `fmo_api_types`.
- **LNv2 gateways are pollable.** The lnv2 module exposes a `GATEWAYS_ENDPOINT ("gateways")`
  federation registry (plus `add_gateway`/`remove_gateway`), but it returns thinner data than
  LNv1's `LightningGatewayAnnouncement` (registered gateway API URLs, not fees/node-key/vetting).
  `fmo_module_lnv2` currently has no gateway table, poller, or route.
- **Live-fetch note (for SP‑2, not here):** the fetcher ingests only *signed* sessions via
  `await_block`; `get_session_status` (present in fedimint-api-client 0.11.1, returns
  `Pending(Vec<AcceptedItem>)`) is the live path — recorded so SP‑2 has it.

## Decisions (locked)

1. **Status is authoritative in the module tables, not gold.** Each LN module owns its status.
2. **Incremental Rust-updated column, not a matview.** A matview over hundreds of thousands of
   contracts refreshed every cycle is the global-recompute trap; the module updates an in-place
   `status` column as it processes each leg.
3. **Gold carries no status.** Delete `user_transactions.status`, strip status derivation from
   `fold_ln_v1`/`fold_lnv2`, drop the `status` dimension from `user_tx_daily`. Consumers read
   status from the module via `user_transaction_txs → contract_id`.
4. **Rich taxonomy** (LNv1): succeeded / refunded / pending, plus incoming distinguishes
   decrypted (= "unclaimed" while it persists) from claimed (= succeeded). LNv2 is thinner:
   succeeded / refunded / pending.
5. **Gateway observation extends to LNv2 AND adds real gateway-API pinging** to both modules
   (reachable + latency), beyond the existing registry-presence `is_seen`.
6. **Extract the shared gateway-poller harness**; refactor LNv1 onto it, build LNv2 on it.
7. **Per-payment only — no rollups/reliability metrics** (aggregate success-rate dashboards are a
   possible later feature, explicitly not built now).

## Architecture

### A. Per-payment status — incremental, module-owned

Add a `status TEXT NOT NULL` column to `fmo_ln.contracts` and `fmo_lnv2.contracts` (schema
migration + `version()` bump in each module → schema drop + replay from raw sessions, no
refetch). The module sets/advances it **in place** inside `process_output` / `process_input` /
`process_ci` as each leg is processed. Because legs arrive in causal consensus order (fund
before claim/cancel/refund; decrypt before claim), transitions are **monotonic** and the
in-place `UPDATE` is idempotent and replay-stable — a re-processed leg re-asserts the same
terminal state; a weaker event never arrives after a stronger one for the same contract.

**LNv1 (`fmo_ln`)** status per `contract_id`:
- *Outgoing:* `fund` output → **pending**; claim input (gateway reveals preimage) →
  **succeeded**; `cancel` output followed by refund input → **refunded**.
- *Incoming:* `fund` output → **pending**; `DecryptPreimage` consensus item → **decrypted**;
  claim input → **succeeded**. The persistent **decrypted** state *is* "decrypted-but-unclaimed"
  (the 212 stranded receives) — it simply never advances to succeeded; **no time-sweep**.
- *Not modeled:* a distinct time-based **expired**. Expiry is not an on-ledger event; a reclaimed
  offer shows **refunded** (event), an abandoned one stays **pending**/**decrypted**. A read-time
  "expired" view could be added later if wanted; not now.

**LNv2 (`fmo_lnv2`)** status per `contract_id`:
- `fund` → **pending**; claim → **succeeded**; refund → **refunded**.
- Requires distinguishing claim-vs-refund inputs, which the schema doesn't record today →
  **add the input variant** to `fmo_lnv2` (a column on `input_outpoints`, or equivalent) so the
  status update can tell them apart. Covered by the same `version()` bump/replay.

**Gold changes (core):** remove the `status` column from `user_transactions`, delete the status
derivation from `fold_ln_v1`/`fold_lnv2` (thinning core of LN semantics it should never have
owned), and remove `status` from the `user_tx_daily` matview definition and its group-by. This
is a schema migration on the gold tier. Nothing in gold needs the module status; consumers join
`user_transaction_txs → contract_id → fmo_ln/fmo_lnv2.contracts.status`.

### B. Gateway observation — registry poll + gateway-API ping, LNv1 & LNv2

Every poll cycle, per federation, the harness:
1. **Polls the federation registry** (`LIST_GATEWAYS_ENDPOINT` for LNv1, `GATEWAYS_ENDPOINT` for
   LNv2), upserting the module's `gateways` table and recording registry presence (`is_seen`) —
   existing LNv1 behavior, generalized.
2. **Pings each registered gateway's own API** (e.g. LNv1 `GET_GATEWAY_ID_ENDPOINT` `/id`, or the
   equivalent lightweight endpoint) with a per-gateway timeout, recording **reachable** (bool)
   and **latency_ms** in the snapshot. A dead/slow gateway cannot stall the loop (bounded
   timeout, per-gateway isolation).

`gateway_poll_snapshots` gains `reachable BOOLEAN` and `latency_ms INTEGER NULL` columns
(migration on `fmo_ln`; native in the new `fmo_lnv2` table). Uptime metrics can then reflect real
reachability, not just registry presence (the existing `is_seen`-based computation stays valid
and is complemented). `fmo_lnv2` gets its own `gateways` + `gateway_poll_snapshots` tables
(thinner `gateways` columns matching what the lnv2 registry returns) and a `/gateways` API route
parallel to LNv1's.

### C. Shared gateway-poller harness

Extract the generic scaffolding out of `fmo_module_ln/gateways.rs` — the poll loop + interval,
snapshot-retention pruning (90-day), the upsert-and-snapshot transaction shape, and the
per-gateway API ping (timeout, reachable/latency) — into a shared location (`fmo_core`, or a
small shared module). Each LN module supplies: (a) its registry endpoint + a parser from the
registry response into gateway rows, (b) the per-gateway ping target. LNv1 is refactored onto the
harness (behavior-preserving for the registry half, gaining the ping); LNv2 is implemented on it.

## Data Model Summary

- `fmo_ln`: `contracts += status`; `gateway_poll_snapshots += reachable, latency_ms`. `version()`
  bump → replay.
- `fmo_lnv2`: `contracts += status`; `input_outpoints` gains claim/refund variant;
  new `gateways` + `gateway_poll_snapshots` tables. `version()` bump → replay.
- Core gold: `user_transactions -= status`; `fold_ln_v1`/`fold_lnv2` status derivation removed;
  `user_tx_daily` redefined without `status`. Gold-tier migration.
- Shared harness code lands in `fmo_core` (or a shared module); both LN modules depend on it.

## API / Surfacing

- Contract/transaction responses from each LN module include the contract `status`.
- `fmo_lnv2` gains a `/gateways` route (and compat alias consistent with LNv1's) returning the
  LNv2 `GatewayInfo`-shaped data (thinner) with reachability/uptime from snapshots.
- No new aggregate/rollup endpoints (per-payment only; reliability dashboards are out of scope).
- Frontend rendering of status/gateways is **not** in this spec — the Session/Consensus Explorer
  (SP‑1) and any gateway UI are separate. This spec delivers the data + module APIs.

## Testing

- **Status (TDD, per module, DB-gated harness that already exists):** fixtures that feed the
  legs of each terminal state and assert the incremental `status` column lands correctly and is
  **replay-stable** (process legs, assert; re-process from scratch, assert identical) —
  LNv1 outgoing succeeded/refunded/pending, LNv1 incoming pending/decrypted(unclaimed)/succeeded,
  LNv2 succeeded/refunded/pending.
- **Gateway harness:** a test with a stub registry response and a stub gateway API exercising
  reachable / unreachable / timeout, asserting the snapshot rows (`is_seen`, `reachable`,
  `latency_ms`) and the upsert/retention behavior. LNv1 refactor keeps its existing tests green.
- **Gold:** confirm the `status` removal leaves `fold_ln_v1`/`fold_lnv2` and `user_tx_daily`
  correct (existing gold tests updated to drop status assertions).

## Global Constraints

- Status is authoritative **in the module tables**; gold carries no status.
- Status is maintained by **incremental in-place Rust updates** during item processing — no
  matview, no global recompute. Transitions are monotonic and replay-stable.
- Unknown/undecodable data must never panic a module or stall a federation (existing invariant).
- Gateway pinging must be bounded (per-gateway timeout) so one gateway can't stall the poll loop.
- The shared harness must keep LNv1's existing registry/snapshot behavior working (its tests stay
  green) while adding the ping.
- Work stays on the `modularization` branch; no PR, nothing pushed.

## Out of Scope / Non-Goals

- Aggregate success-rate / gateway-reliability metrics, dashboards, or rollup tables (per-payment
  status only).
- A distinct time-based **expired** status (no expiry sweep; read-time derivation is a later
  option).
- Active probing of gateway *liquidity/fees* beyond what the registry announcement and a
  lightweight reachability ping provide.
- Any frontend work, and SP‑1 (explorer) / SP‑2 (live view), which are separate specs.
