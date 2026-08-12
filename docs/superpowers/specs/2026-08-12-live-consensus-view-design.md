# Live Consensus View Design (SP‑2)

**Date:** 2026-08-12
**Status:** Approved, ready for planning
**Branch:** modularization (no PR — local branch only, per standing constraint)

## Where this fits

SP‑2, the last of three sub-projects. SP‑3 (gateway + LN status) and SP‑1 (Session & Consensus
Explorer) are done and deployed. SP‑2 builds directly on SP‑1's item model, consensus query, and
`ItemList`/renderer registry.

## Goal

Show a federation's **in-progress (unsigned) consensus session live** — items appear as guardians
accept them, fully processed (raw item + module silver + gold classification), streamed to the
browser over SSE and rendered with SP‑1's item components. The pending session becomes real,
queryable data in the DB (the explorer shows it live too); when it signs, it's finalized and
reconciled against the authoritative outcome.

## Context / Current State

- **The fetcher only ingests *signed* sessions.** `run_fetcher` (`fmo_core/src/fetch.rs`) loops
  `api.await_block(session_index)`, which blocks until a session is signed, then `ingest_session`
  writes its structural rows and `sessions.data = SessionOutcome` bytes. Dispatch
  (`fmo_core/src/dispatch.rs`) later decodes `sessions.data` per session and runs the module
  `process_input/output/ci` hooks, advancing `module_progress.next_session_index` per session.
- **The live primitive exists.** `DynGlobalApi::get_session_status(idx)` returns
  `SessionStatus::Initial | Pending(Vec<AcceptedItem>) | Complete(SignedSessionOutcome)`;
  `AcceptedItem { item: ConsensusItem, peer }` is the exact shape ingest/dispatch already handle.
  `Pending` returns the full accumulated accepted-item list each call, appended to over time.
  **Accepted items are final and append-only** — the signed outcome is the same ordered list plus
  signatures; the pending list is always a prefix of it.
- **The whole pipeline is idempotent.** Structural ingest and every module `process_*` use
  `ON CONFLICT DO NOTHING`; write-backs are `UPDATE`; LN status is an incremental per-contract
  recompute; gold folds recompute per-contract (LN) / per-txid (else). So the same item can be
  processed more than once safely.
- **Per-item seam.** `dispatch_session_to_module` (dispatch.rs:275) is a
  `for (item_index, accepted_item) in session.items.iter().enumerate()` loop over the module hooks
  — refactorable to process a suffix `items[start..]`.
- **SP‑1 gives us the item model + query.** `SessionItem` (session_index, item_index, item_type,
  kind, peer_id, txid, user_tx_key, **user_tx_kind, direction**, details) and the consensus-stream
  query already assemble the *fully enriched* item (including the gold classification via a LATERAL
  join). The frontend `ItemList` + renderer registry render it. `session_stats` is written once,
  immutably (`ON CONFLICT DO NOTHING`).
- **`sessions.data` is `BYTEA NOT NULL`** — assumes a session is stored only when complete.

## Decisions (locked)

1. **Everything live, then reconcile.** For the in-progress session, raw items *and* module dispatch
   *and* gold all process incrementally as items arrive; on completion the session is finalized and
   reconciled against the authoritative signed outcome.
2. **Incremental, not re-run.** Because `Pending` returns the full list each poll, the live poller
   keeps an in-memory `next_item_index` per federation and processes **only `items[next_item_index..]`**
   each poll — never re-processing what it already did. The dispatch/ingest item loops are refactored
   to accept a start index.
3. **Catch-up → live transition.** Historical sessions (behind the tip) keep using bulk `await_block`
   + whole-session dispatch. The live per-item path engages only once a federation reaches the tip.
4. **Cursors advance on completion only.** `module_progress`/`gold_progress` do NOT advance while a
   session is open — the live path is the authoritative processor for the tip session. On `Complete`,
   reconcile + finalize, then advance one session.
5. **Transport = SSE with a `watch`-of-index, per-client DB tail.** A per-federation
   `tokio::sync::watch<(session_index, item_index)>` high-water mark; the poller just bumps it. Each
   SSE client keeps its own cursor and, on each tick, reads the keyset delta (SP‑1's consensus query)
   from its cursor to the watermark and streams those enriched items. Reuses SP‑1's query (so live
   items carry gold classification), and is correct by construction (own cursor → no dup/gap, handles
   slow/reconnecting/late clients with no broadcast lag/join edge cases).

## Architecture

### Fetcher: catch-up → live

`run_fetcher` gains a live phase. While `next_session` is behind the federation's current session,
keep the existing `await_block` bulk loop. When it reaches the tip, switch to the **live loop**: poll
`get_session_status(current_session_index)` every ~1s (configurable, `FO_LIVE_POLL_SECS`).

### Live poll of `Pending(items)` (all bounded to *new* items)

Let `start = next_item_index` (0 on the first poll of a session). For `items[start..]`, in one
transaction:
1. **Structurally ingest** the new items (a `start`-aware variant of `ingest_session`'s per-item
   inserts) — creating the `sessions` row on the first poll (with `data = NULL`, not yet complete).
2. **Dispatch** the new items across all modules (the `start`-aware `dispatch_*` loop) → silver
   tables, amounts, LN status, decryption shares update live.
3. **Gold-fold** the current session directly (bounded to this session's touched contracts/txids;
   NOT via `gold_progress`, which stays behind) → `user_transactions` classification/direction live.
4. **Update `session_stats`** for the open session with the new running counts (see below).
5. Set `next_item_index = items.len()` and **bump the watch watermark** to `(session_index,
   items.len()-1)`.

### Completion `Complete(SignedSessionOutcome)`

The signed outcome's items are authoritative and a superset (prefix-extension) of what we live-
collected. So: process any tail `items[next_item_index..]` (the reconcile/backfill — usually empty),
**assert/log** `final_count == next_item_index` after backfill, write the authoritative
`sessions.data` (and the signature — see schema), mark the session complete + freeze its
`session_stats`, then advance `module_progress` (all modules) and `gold_progress` past this session.
Reset `next_item_index = 0`, emit a rollover signal on the watch, and advance to the next session.
No full re-process on completion.

### Schema touches (core, append-only migration v4)

- `sessions.data` → **nullable** (open sessions have `data = NULL`; set to the authoritative bytes on
  completion). Optionally add `signature BYTEA` to persist the session signature (the "backfill the
  signature" intent; the observer doesn't otherwise use it — provenance only).
- Dispatch's catch-up query filters to complete sessions (`WHERE data IS NOT NULL`) so it never tries
  to decode an open session's null blob.
- **`session_stats` becomes mutable while open:** the live path `INSERT … ON CONFLICT DO UPDATE`
  (running counts) for the open session; on completion it's frozen (its final counts written once
  more, then never touched — sessions are immutable after signing). This is the one SP‑1 change.

### SSE endpoint + watch

- Per-federation `LiveState { watch: watch::Receiver<Watermark>, … }` held on the observer, published
  to by the live poller. `Watermark = { session_index, item_index, rolled_over: bool }`.
- `GET /federations/:id/live` (SSE): on connect the handler reads the **current open session's items
  so far** from the DB (SP‑1 consensus query scoped to that session) and streams them, recording its
  cursor. Then it awaits watch changes; on each, it reads the keyset delta from its cursor to the
  watermark (SP‑1 query) and streams the new enriched `SessionItem`s, advancing its cursor. A rollover
  watermark tells the client the session completed and a new one opened (it resets to the new
  session). Standard SSE keep-alive/heartbeat; the handler cleans up on client disconnect.

### Frontend (reuse SP‑1)

A **Live** view (a tab on `FederationDetail`, and/or a banner on the Consensus tab): opens an
`EventSource` to `/federations/:id/live`, appends incoming `SessionItem`s to an SP‑1 `ItemList`
(same renderer registry → same rich tx/CI rendering, gold classification badges), shows a "● LIVE"
indicator and the current session index, and on a rollover event starts a fresh list for the new
session. Reconnect on drop (EventSource does this natively). No new rendering code — only the live
data source.

## Data Flow

federation guardians → `get_session_status(Pending)` → live poller processes `items[new..]`
(ingest → dispatch → gold, all incremental) → DB + watch watermark bump → SSE handlers read the
delta (SP‑1 query) → browser `ItemList` appends. On sign → finalize + reconcile → cursors advance →
rollover → clients reset to the next session.

## Error Handling

- **Poll failure / federation unreachable:** log + retry with backoff (like the existing fetcher);
  the watch simply doesn't advance; SSE clients idle until it resumes.
- **Reconcile mismatch** (final count ≠ live count after tail backfill): log loudly with the indices;
  the authoritative `Complete` items are the source of truth, so backfilling the suffix repairs it.
  A true divergence (an item changed, not just appended) violates the append-only invariant and is a
  logged error, not silently ignored.
- **SSE client slow/disconnect/reconnect:** handled by the own-cursor design — a reconnecting client
  re-reads from its last position; no server-side per-client buffering.
- **Restart mid-open-session:** on startup the live poller resumes; `next_item_index` starts at 0 and
  the first poll re-ingests the open session's items idempotently, then continues. The open session's
  `data` is still NULL (correct); dispatch's catch-up ignores it.

## Testing

- **Refactor (start-aware ingest/dispatch):** unit-test that processing `items[0..k]` then
  `items[k..]` yields the identical DB state as processing `items[0..]` in one pass (idempotent,
  order-preserving), via the existing DB-gated harness.
- **Live processing:** a test driving a fabricated `SessionOutcome`'s items in two "polls" (a prefix
  then the rest) asserts silver + gold rows appear incrementally and the watermark advances; a
  completion step asserts finalize (data set, session_stats frozen, cursors advanced) and that a
  missing-tail case is backfilled.
- **`sessions.data` nullable + dispatch skip:** assert dispatch's catch-up ignores a null-data (open)
  session and processes it once finalized.
- **SSE handler:** a test (mock/live watch) that a client connecting mid-session gets the items-so-far
  then the delta on a watermark bump, with no duplication across the connect/tick boundary.
- **Frontend:** a Live-view test (mock `EventSource`) that streamed items append via `ItemList` and a
  rollover event resets the list.

## Global Constraints

- Incremental only — the live path processes **new items** each poll (`items[next..]`), never the
  whole session again; per-poll work is proportional to arrivals, completion is a count-check + cursor
  bump.
- All live processing stays idempotent (the design relies on it for restart/reconcile safety).
- `module_progress`/`gold_progress` advance **only** on session completion.
- The SSE channel carries only the `(session_index, item_index)` watermark; enriched items come from
  the DB via SP‑1's consensus query (reuse, not re-assembly).
- Accepted items are append-only/final; the pending list is a prefix of the signed outcome.
- Core schema changes are append-only migrations; frontend reuses SP‑1's `ItemList`/renderers.
- Read-only for clients (SSE is server→client only).
- Public-ready but deployed to the private instance only; nothing may assume private-only.
- Work stays on the `modularization` branch; no PR, nothing pushed.

## Out of Scope / Non-Goals

- A broadcast/WebSocket transport (watch + DB tail chosen; broadcast is a later optimization behind
  the same interface if the delta reads ever profile hot).
- Live mempool/on-chain overlays; SP‑2 is consensus items only.
- Historical "replay a past session live" playback — SP‑2 is the *current* session only.
- Persisting or verifying the guardian signature beyond optional storage for provenance (the
  reconcile uses item comparison, not signature verification).
- Any change to how already-signed/historical sessions are fetched or dispatched (catch-up path
  unchanged apart from the `WHERE data IS NOT NULL` filter).
