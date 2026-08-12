# Live Consensus View Implementation Plan (SP‑2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stream a federation's in-progress (unsigned) consensus session live — items fully processed (raw + module silver + gold) the moment guardians accept them — to the browser over SSE, rendered with SP‑1's item components; finalize + reconcile when the session signs.

**Architecture:** The fetcher bulk-catches-up history via `await_block`, then at the tip polls `get_session_status`; each poll processes only the *new* items (`items[next..]`) through a shared start-aware ingest+dispatch+gold path and bumps a per-federation `watch` watermark. `module_progress` advances only on completion (atomic finalize + reconcile). SSE clients tail the DB delta (SP‑1's consensus query) from their own cursor on each watermark tick.

**Tech Stack:** Rust (axum incl. `Sse`, tokio `watch`, deadpool/tokio-postgres), fedimint 0.11.1 `get_session_status`; React 19 + TS `EventSource`; Nix `just`.

## Global Constraints

- Incremental only — each poll processes `items[next_item_index..]`, never the whole session again; completion is a count-check + cursor bump, not a reprocess.
- All live processing stays idempotent (ingest/`process_*` `ON CONFLICT DO NOTHING`, LN status per-contract recompute, gold per-contract/txid) — relied on for restart/reconcile safety.
- `module_progress`/`gold_progress` advance **only** on session completion; the finalize (set `sessions.data` + advance `module_progress` + freeze `session_stats`) is one atomic transaction.
- The SSE channel carries only the `(session_index, item_index)` watermark; enriched items come from the DB via SP‑1's consensus query (reuse, not re-assembly).
- Accepted items are append-only/final; the pending list is a prefix of the signed outcome.
- Keyset only (no OFFSET); read-only for clients (SSE server→client only).
- Core schema changes are append-only migrations; frontend reuses SP‑1's `ItemList`/renderer registry.
- Public-ready, deployed private-only for now.
- Repo pre-commit hook (typos + `cargo fmt --all`) stays green; commit without `--no-verify`.
- DB-gated tests: `export FMO_TEST_DATABASE='postgres://user@/fmo_test?host=/home/user/projects/fedimint-observer/.pg_dev&port=5432'` before `nix develop -c just test_package <pkg>`. Frontend: `cd fmo_frontend_react && npm test`. Use `FederationObserver::new_without_tasks` in observer-method tests (SP‑1).
- Work stays on the `modularization` branch; no PR, nothing pushed.

---

## Task 1: Start-aware ingest + dispatch (refactor)

Make the two per-item loops process a suffix `items[start..]` of a session, so the live path can reuse them. Pure refactor — behavior identical for `start = 0`.

**Files:**
- Modify: `fmo_core/src/ingest.rs` (extract a start-aware item ingest)
- Modify: `fmo_core/src/dispatch.rs` (`dispatch_session_to_module` takes a start index)
- Test: `fmo_core/tests/incremental.rs`

**Interfaces:**
- Produces: `pub async fn fmo_core::ingest::ingest_items(dbtx, config, federation_id, session_index: u64, items: &[fedimint_core::session_outcome::AcceptedItem], start: usize) -> anyhow::Result<()>` — inserts the `sessions` row (data NULL) if absent, then the structural rows for `items[start..]`, and upserts `session_stats` running counts with `ON CONFLICT DO UPDATE`. `ingest_session` becomes a thin wrapper: create the session row with the final `data`, then `ingest_items(.., items, 0)`.
- Produces: `pub(crate) async fn dispatch_items_to_module(dbtx, module, services, federation_id, config, session_index: u64, items: &[AcceptedItem], start: usize) -> anyhow::Result<()>` — the current `dispatch_session_to_module` body, iterating `items[start..]` with absolute `item_index = start + rel`.

- [ ] **Step 1: Refactor `dispatch_session_to_module`.** Change its item loop from `session.items.iter().enumerate()` to iterate a slice with an absolute index, and expose `dispatch_items_to_module(.., items: &[AcceptedItem], start: usize)`. Keep `dispatch_session_to_module(.., session)` as `dispatch_items_to_module(.., &session.items, 0)`.

```rust
for (rel, accepted_item) in items[start..].iter().enumerate() {
    let item_index = start + rel;
    // ... existing body, using `item_index` (was the enumerate index) ...
}
```

- [ ] **Step 2: Refactor `ingest_session` into `ingest_items`.** Move the per-item structural inserts into `ingest_items(.., items, start)`. The `sessions` row insert becomes `INSERT INTO sessions (federation_id, session_index, data) VALUES ($1,$2,NULL) ON CONFLICT DO NOTHING` in `ingest_items` (create-if-absent, data stays NULL for open sessions); `ingest_session` (the historical/complete path) additionally sets `data` — see Task 2. Change the `session_stats` write in `ingest_items` to `ON CONFLICT (federation_id, session_index) DO UPDATE SET tx_count=EXCLUDED.tx_count, ci_count=EXCLUDED.ci_count, items_by_kind=EXCLUDED.items_by_kind` and compute the counts over the WHOLE items list (0..len) so running totals are correct each call (the counts are cheap; the structural inserts are the `start..` slice).

- [ ] **Step 3: Write the equivalence test** `fmo_core/tests/incremental.rs`: build a fabricated `Vec<AcceptedItem>` (2 transactions + 2 module CIs of two kinds — reuse the fixture style from `fmo_core/tests/gold.rs`/`sessions_api.rs` for constructing items, or seed via a minimal `SessionOutcome`). Process it two ways in separate federations: (a) `ingest_items(items, 0)` then dispatch all; (b) `ingest_items(items[0..k], 0)` + `ingest_items(items, k)` then dispatch in two slices. Assert the resulting `transactions`/`consensus_items`/`session_stats` rows are identical, and `session_stats` reflects the full counts in both.

- [ ] **Step 4: Run tests** (`export FMO_TEST_DATABASE=…; nix develop -c just test_package fmo_core`) — confirm the new test ran (not "skipping") + the existing ingest/dispatch/gold/session_stats tests still pass. `just clippy`. **Commit.**

---

## Task 2: Nullable `sessions.data` + dispatch skips open sessions

**Files:**
- Create: `fmo_core/schema/core/v4.sql`
- Modify: `fmo_core/src/db/migrations.rs` (append v4) + `fmo_core/tests/schema.rs` (count assertion → 4)
- Modify: `fmo_core/src/dispatch.rs` (`process_pending` filters complete sessions)
- Modify: `fmo_core/src/ingest.rs` (`ingest_session` sets `data` on the complete path)
- Test: `fmo_core/tests/incremental.rs` (extend)

- [ ] **Step 1: Migration** `fmo_core/schema/core/v4.sql`:

```sql
-- Open (unsigned) sessions are ingested live with NULL data; the authoritative
-- SessionOutcome bytes are written when the session signs. Optionally the
-- guardian signature is stored for provenance (observer doesn't verify it).
ALTER TABLE sessions ALTER COLUMN data DROP NOT NULL;
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS signature BYTEA;
```

Append the v4 `Migration{}` to `CORE_MIGRATIONS` (after v3); bump the `tests/schema.rs` count assertion from 3 to 4.

- [ ] **Step 2: `process_pending` skips open sessions.** In `dispatch.rs`, the `SELECT session_index, data FROM sessions WHERE …` query (and the `MAX(session_index)` fetched query) must exclude open sessions — add `AND data IS NOT NULL`. So dispatch only ever processes complete sessions (the live path owns open ones).

- [ ] **Step 3: `ingest_session` sets data.** `ingest_session(dbtx, config, fed, idx, session)` (the historical/complete path used by the bulk `await_block` fetcher and the import tool) becomes: `INSERT INTO sessions (…, data) VALUES ($1,$2,$3) ON CONFLICT (federation_id, session_index) DO UPDATE SET data = EXCLUDED.data` (so it fills `data` even if the row was pre-created NULL by a live poll), then `ingest_items(.., &session.items, 0)`.

- [ ] **Step 4: Test** (extend `incremental.rs`): insert a session row with `data = NULL` + some items, run `process_pending`, assert it did NOT process that session (module tables empty for it); set `data`, run again, assert it now processed. **Run tests + clippy. Commit.**

---

## Task 3: Live processing functions (DB-testable core)

The pure per-poll processing + the completion finalize, as functions the live loop (Task 4) calls. No network, no watch, no SSE here.

**Files:**
- Create: `fmo_core/src/live.rs` (+ `pub mod live;` in `fmo_core/src/lib.rs`)
- Test: `fmo_core/tests/live.rs`

**Interfaces:**
- Consumes: `ingest::ingest_items`, `dispatch::dispatch_items_to_module`, `gold::fold_sessions` (Task 1), registry, `CoreServices`.
- Produces:
  - `pub async fn live_process(pool, registry: &ModuleRegistry, services: &Arc<CoreServices>, federation_id, config: &ClientConfig, session_index: u64, items: &[AcceptedItem], start: usize) -> anyhow::Result<()>` — in ONE transaction: `ingest_items(items, start)`, then for each module `dispatch_items_to_module(items, start)`, then `gold::fold_sessions` scoped to `[session_index, session_index+1)` (bounded to this session's touched contracts). Idempotent. Used per live poll with `start = next_item_index`.
  - `pub async fn finalize_live_session(pool, registry, services, federation_id, config, session_index: u64, final_items: &[AcceptedItem], processed_count: usize, data: &[u8], signature: Option<&[u8]>) -> anyhow::Result<()>` — reconcile + finalize atomically: `live_process(final_items, processed_count)` for any tail, assert `final_items.len() == processed_count_after` (log a warning with indices on mismatch), then in one txn `UPDATE sessions SET data=$data, signature=$sig` and advance every module's cursor **conditionally**: `UPDATE module_progress SET next_session_index = $session_index+1 WHERE federation_id=$1 AND module_kind=$k AND next_session_index = $session_index` (only when the separate processor has already caught up to this session — otherwise it affects 0 rows and the normal `run_processor` dispatches the now-complete session from its stored `data` and advances the cursor itself, idempotently). **Rationale:** the fetcher can go live before `module_progress` reaches the tip; an unconditional advance to `session_index+1` would skip the un-dispatched middle sessions. Gold advances via the normal `run_gold_processor` once `module_progress` moves.

- [ ] **Step 1: Failing test** `fmo_core/tests/live.rs`: fabricate a session's `Vec<AcceptedItem>` (a transaction with an ln/wallet input+output + a module CI). Seed the federation. Call `live_process(items, 0..? )` in two slices (`start=0` on `items[0..1]`, then `start=1` on all) and assert: structural rows for all items exist, module silver rows exist, `session_stats` shows running counts, and `module_progress` has NOT advanced (still 0). Then `finalize_live_session(final_items, processed_count, data_bytes, None)` and assert: `sessions.data` is set, `module_progress.next_session_index == session_index+1` for every module, and a deliberately-short `processed_count` triggers the tail backfill (all items processed) without error.

- [ ] **Step 2: Implement `live.rs`.** `live_process` opens a txn, runs `ingest_items` then the per-module `dispatch_items_to_module` loop then `gold::fold_sessions(&txn-conn, federation_id, session_index, session_index+1)` (match `fold_sessions`'s real signature — read gold.rs:447), commits. `finalize_live_session` calls `live_process` for the tail, verifies the count, then a finalize txn (`UPDATE sessions SET data/signature`, per-module `module_progress` upsert to `session_index+1`). Keep both idempotent.

- [ ] **Step 3: Run tests + clippy. Commit.**

---

## Task 4: Live poll loop + watch + fetcher transition

Wire the fetcher: catch-up (`await_block`) → live (`get_session_status`); publish a per-federation watermark.

**Files:**
- Modify: `fmo_core/src/live.rs` (the loop + `Watermark`/`LiveState`)
- Modify: `fmo_core/src/fetch.rs` (`run_fetcher` transitions to live at the tip)
- Modify: `fmo_core/src/observer.rs` (hold `live_states: Arc<Mutex<HashMap<FederationId, watch::Receiver<Watermark>>>>`; pass the sender into the fetcher)

**Interfaces:**
- Produces: `pub struct Watermark { pub session_index: i64, pub item_index: i64, pub rolled_over: bool }` (Default = all zero / no data); `FederationObserver::live_watch(&self, federation_id) -> Option<watch::Receiver<Watermark>>` (for the SSE handler in Task 5).
- Produces: the live loop `pub async fn run_live(pool, registry, services, federation_id, config, api: DynGlobalApi, wm: watch::Sender<Watermark>, from_session: u64) -> anyhow::Result<()>`.

- [ ] **Step 1: `run_live`.** Loop over session indices from `from_session`: for the current index, poll `api.get_session_status(idx, &decoders, core_api_version, broadcast_public_keys)` every `FO_LIVE_POLL_SECS` (default 1). On `Pending(items)`: if `items.len() > next_item_index`, `live_process(.., items, next_item_index)`, set `next_item_index = items.len()`, and `wm.send(Watermark{ session_index: idx as i64, item_index: items.len() as i64 - 1, rolled_over:false })`. On `Complete(signed)`: `finalize_live_session(.., signed.items(), next_item_index, &signed_data_bytes, Some(&sig_bytes))`, send a `rolled_over: true` watermark for `idx`, reset `next_item_index = 0`, advance to `idx+1`. On `Initial`: wait a poll. Log+backoff on poll errors (mirror the fetcher's `background_backoff`). (Read `SessionStatus`/`SignedSessionOutcome` accessors in fedimint-core `session_outcome.rs` for the exact item/signature getters and how to get the encoded `data` bytes.)

- [ ] **Step 2: `run_fetcher` transition.** After the bulk `await_block` loop reaches the current tip (`session_index >= api.session_count() - 1`), hand off to `run_live` starting at that session, publishing to the federation's watch sender. (Keep bulk `await_block` for the catch-up phase; only switch at the tip. `run_processor`/`run_gold_processor` keep handling `< tip` complete sessions — they skip open sessions via Task 2's `data IS NOT NULL` filter, and find `module_progress` already advanced past a session the live path finalized.)

- [ ] **Step 3: Observer wiring.** In `observer.rs`, add the `live_states` map; in `spawn_federation`, create a `watch::channel(Watermark::default())`, store the receiver in `live_states`, and pass the sender to `run_fetcher`. Add `live_watch(&self, fed)`.

- [ ] **Step 4:** This task is network-facing (`get_session_status`), so it stays integration-tested in deployment like the fetcher; the DB-mutating functions are covered by Task 3. Gate: `just clippy` clean, `just test_package fmo_core` still green (no regressions). **Commit.**

---

## Task 5: SSE `/live` endpoint

**Files:**
- Create: `fmo_core/src/api/live.rs` (+ `pub mod live;` in `api/mod.rs`) + route in `federations.rs`
- Modify: `fmo_core/src/api/mod.rs` (expose the observer's `live_watch` via `AppState`/`ModuleApiState` as needed)
- Test: `fmo_core/tests/live_api.rs`

**Interfaces:**
- Consumes: `FederationObserver::live_watch` (Task 4), SP‑1's consensus query.
- Produces: `pub async fn federation_live_items(&self, federation_id, after: Option<(i64,i64)>, up_to: (i64,i64)) -> anyhow::Result<Vec<SessionItem>>` — the keyset delta strictly greater than `after` and ≤ `up_to`, ascending (oldest-first, so the client appends in order), reusing SP‑1's enriched item SQL (the `USER_TX_LATERAL` fragment + the tx⊔CI union), scoped to `after < (session,item) <= up_to`.

- [ ] **Step 1: Failing test** `fmo_core/tests/live_api.rs`: seed a federation with a couple of sessions of items (+ a gold row for one tx). Assert `federation_live_items(fed, None, (s,i))` returns the items up to `(s,i)` ascending with enriched `user_tx_kind`; then `federation_live_items(fed, Some(cursor), (s2,i2))` returns only the delta after `cursor` — no overlap, no gap.

- [ ] **Step 2: Implement `federation_live_items`** (ascending keyset delta over the tx⊔CI union, reusing `sql_fragments::USER_TX_LATERAL`; predicate `(session,item) > after AND (session,item) <= up_to`, `ORDER BY session ASC, item ASC`).

- [ ] **Step 3: SSE handler** `federation_live(Path(fed), State) -> Sse<...>` in `api/live.rs`: get `live_watch(fed)`; build an async stream that (a) on start reads `federation_live_items(fed, None, current_watermark)` and yields each as an SSE `data:` event (JSON `SessionItem`), tracking `cursor`; (b) loops `wm.changed().await`, on each read the new watermark — if `rolled_over`, yield a `event: rollover` with the new session index and reset `cursor` to before that session; else `federation_live_items(fed, Some(cursor), watermark)` and yield the delta, advancing `cursor`. Include SSE keep-alive (`axum::response::sse::KeepAlive`). Add the route `.route("/:federation_id/live", get(super::live::federation_live))` in `federations.rs`.

- [ ] **Step 4:** Run `just test_package fmo_core` (the `live_api` delta test ran+passed) + `just clippy`. The SSE streaming wiring is exercised by compile + the delta function's test. **Commit.**

---

## Task 6: Frontend Live view

**Files:**
- Modify: `fmo_frontend_react/src/services/api.ts` (a helper to build the SSE URL; auth note below) + `src/types/api.ts` (reuse `SessionItem`)
- Create: `src/components/explorer/LiveView.tsx`
- Modify: `src/pages/FederationDetail.tsx` (a "Live" tab)
- Test: `src/components/explorer/LiveView.test.tsx`

- [ ] **Step 1: `LiveView` component.** Opens an `EventSource` to `${BASE_URL}/federations/${id}/live`. Note: `EventSource` can't set an `Authorization` header, so on the bearer-gated private instance the token must go via query string — add `?token=<bearerToken>` from `auth.getToken()` and have the SSE route accept it (OR document that `/live` is served same-origin behind the existing cookie/gate; simplest: append the token as a query param and read it in the handler as an alternative to the header — implement that in Task 5's handler if the token is present). Append incoming `SessionItem`s (parsed from `event.data`) to a list state and render via SP‑1's `<ItemList items scope="consensus" hasMore={false} …>`; show a "● LIVE" indicator + the current session index; on a `rollover` event, clear the list and update the session index; reconnect is native to `EventSource`.
- [ ] **Step 2: Wire a "Live" tab** into `FederationDetail` (extend the `activeTab` union with `'live'`, tab button matching the existing styling, render `<LiveView federationId={id}/>`).
- [ ] **Step 3: Test** `LiveView.test.tsx` (mock `EventSource`): dispatched `message` events append items rendered by `ItemList`; a `rollover` event clears the list. `npm test` + `npm run build`.
- [ ] **Step 4: Commit.**

---

## Final verification

- `just clippy` + `just test_package fmo_core` (with `FMO_TEST_DATABASE`) green; `cd fmo_frontend_react && npm test && npm run build` green; `just final-check`.
- **Deployment (user-gated):** the v4 migration is instant (`ALTER … DROP NOT NULL` + add nullable column). No replay (no module `version()` bump). On restart the fetchers reach the tip and begin live-polling; the `/live` SSE + Live tab become active. Re-lock infra + colmena as with SP‑1.

## Notes on ordering & dependencies

- Task 1 (refactor) underpins Tasks 3–5; Task 2 (nullable data) is needed before the live path creates open sessions. Do 1→2→3→4→5 in order; Task 6 (frontend) needs Task 5's endpoint.
- Tasks 4/5 are network/SSE-facing (integration-tested in deployment, like SP‑1's poller); the DB-mutating and delta-query logic they call is unit-tested in Tasks 3/5.
- The live path and the existing `run_processor`/`run_gold_processor` coexist via Task 2's `data IS NOT NULL` filter + the atomic finalize (set data + advance `module_progress` together) — no double-processing or cursor races at the tip.
