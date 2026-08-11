# Session & Consensus Explorer Implementation Plan (SP‑1)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A polished per-federation explorer — paginated session list (precomputed stats), federation-wide filterable consensus-item stream, rich transaction detail, and a gold user-transaction drill-in — API + React UI, over the existing structural + gold tables.

**Architecture:** One item model (transaction ⊔ module-CI, addressed by `(session_index, item_index)`) at two scopes (one session | whole federation), keyset-paginated. A precomputed immutable `session_stats` table makes the session list O(1)/row. New read-only endpoints join the `/federations` API and expose the gold layer; a shared React item-list component + a per-kind renderer registry power the UI.

**Tech Stack:** Rust (axum, deadpool/tokio-postgres), PostgreSQL, fedimint 0.11.1; React 19 + TS + Vitest; Nix `just`.

## Global Constraints

- Pagination is **keyset only** (cursor `(session_index, item_index)` for items, `session_index` for sessions); never `OFFSET` — `consensus_items` is ~127M rows.
- `session_stats` is written **once at ingest** and never mutated; population must be idempotent (`ON CONFLICT DO NOTHING`) for crash-resume/replay safety.
- Consensus-item `details` are served as pre-decoded JSON; the frontend owns per-kind rendering and must degrade to a raw-JSON view for undecoded kinds, never error.
- New API types live in `fmo_api_types` (shared FE/BE).
- Read-only: no endpoint mutates.
- Core schema changes are append-only migrations; the composite index on the 127M-row `consensus_items` must be built with `CREATE INDEX CONCURRENTLY IF NOT EXISTS` **outside** any migration transaction (CONCURRENTLY can't run inside one).
- Public-ready but deployed to the private instance only for now.
- Repo pre-commit hook (typos + `cargo fmt --all`) stays green; commit without `--no-verify`.
- DB-gated tests: `export FMO_TEST_DATABASE='postgres://user@/fmo_test?host=/home/user/projects/fedimint-observer/.pg_dev&port=5432'` before `nix develop -c just test_package <pkg>` (else they silently skip). Frontend: `cd fmo_frontend_react && npm test`.
- Work stays on the `modularization` branch; no PR, nothing pushed.

---

## Task 1: `session_stats` table + ingest population + backfill

**Files:**
- Create: `fmo_core/schema/core/v3.sql`
- Modify: `fmo_core/src/db/migrations.rs` (append v3)
- Modify: `fmo_core/src/ingest.rs` (populate at ingest)
- Modify: `fmo_core/src/observer.rs` (spawn backfill task) + Create `fmo_core/src/session_stats.rs` (backfill)
- Test: `fmo_core/tests/session_stats.rs`

**Interfaces:**
- Produces: table `session_stats(federation_id BYTEA, session_index INT, tx_count INT NOT NULL, ci_count INT NOT NULL, items_by_kind JSONB NOT NULL, PK(federation_id, session_index))`; `pub async fn fmo_core::session_stats::backfill_session_stats(pool: &deadpool_postgres::Pool, federation_id: &[u8]) -> anyhow::Result<()>`.

- [ ] **Step 1: Migration.** Create `fmo_core/schema/core/v3.sql`:

```sql
CREATE TABLE session_stats
(
    federation_id BYTEA   NOT NULL REFERENCES federations (federation_id),
    session_index INTEGER NOT NULL,
    tx_count      INTEGER NOT NULL,
    ci_count      INTEGER NOT NULL,
    items_by_kind JSONB   NOT NULL,
    PRIMARY KEY (federation_id, session_index),
    FOREIGN KEY (federation_id, session_index) REFERENCES sessions (federation_id, session_index)
);
```

Append to `CORE_MIGRATIONS` in `fmo_core/src/db/migrations.rs` (after v2):

```rust
    Migration {
        sql: include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/schema/core/v3.sql")),
    },
```

Update the migration-count assertion in `fmo_core/tests/schema.rs` (search for the `assert_eq!(..., 2)` from the SP‑3 task; it becomes `3`).

- [ ] **Step 2: Populate at ingest.** In `fmo_core/src/ingest.rs`, tally during the existing item loop and insert after it. Before the `for (item_index, accepted_item)` loop add counters; in the `Transaction` arm `tx_count += 1;`, in the `Module` arm `*ci_by_kind.entry(kind.clone()).or_insert(0) += 1; ci_count += 1;`. After the loop, insert:

```rust
    let mut tx_count: i32 = 0;
    let mut ci_count: i32 = 0;
    let mut ci_by_kind: std::collections::BTreeMap<String, i64> = std::collections::BTreeMap::new();
    // ... in the Transaction arm: tx_count += 1;
    // ... in the Module arm (after computing `kind`):
    //     ci_count += 1;
    //     *ci_by_kind.entry(kind.to_string()).or_insert(0) += 1;

    dbtx.execute(
        "INSERT INTO session_stats (federation_id, session_index, tx_count, ci_count, items_by_kind)
         VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING",
        &[
            &federation_id_bytes,
            &(session_index as i32),
            &tx_count,
            &ci_count,
            &serde_json::to_value(&ci_by_kind)?,
        ],
    )
    .await?;
```

(Note: `kind` in the `Module` arm is a `String` from `instance_to_kind`; adapt `.clone()`/`.to_string()` to its actual type. The `Transaction` arm's inputs/outputs also have kinds but those are tx-level, not counted here — only whole transactions count toward `tx_count`.)

- [ ] **Step 3: Backfill for pre-existing sessions.** Create `fmo_core/src/session_stats.rs`:

```rust
//! One-time backfill of `session_stats` for sessions ingested before the table
//! existed. Batched by session range and resumable (re-run fills only gaps),
//! so it can run as a background task without a blocking migration.
use deadpool_postgres::Pool;

const BATCH: i64 = 2000;

pub async fn backfill_session_stats(pool: &Pool, federation_id: &[u8]) -> anyhow::Result<()> {
    loop {
        let conn = pool.get().await?;
        // Next contiguous window of sessions missing stats.
        let n = conn
            .execute(
                "INSERT INTO session_stats (federation_id, session_index, tx_count, ci_count, items_by_kind)
                 SELECT s.federation_id, s.session_index,
                        COALESCE(t.c, 0)::int,
                        COALESCE(c.total, 0)::int,
                        COALESCE(c.by_kind, '{}'::jsonb)
                 FROM (
                     SELECT federation_id, session_index FROM sessions
                     WHERE federation_id = $1
                       AND NOT EXISTS (SELECT 1 FROM session_stats ss
                                       WHERE ss.federation_id = sessions.federation_id
                                         AND ss.session_index = sessions.session_index)
                     ORDER BY session_index
                     LIMIT $2
                 ) s
                 LEFT JOIN (
                     SELECT federation_id, session_index, count(*) c
                     FROM transactions WHERE federation_id = $1 GROUP BY 1, 2
                 ) t ON t.federation_id = s.federation_id AND t.session_index = s.session_index
                 LEFT JOIN (
                     SELECT federation_id, session_index, count(*) total,
                            jsonb_object_agg(kind, k) AS by_kind
                     FROM (SELECT federation_id, session_index, kind, count(*) k
                           FROM consensus_items WHERE federation_id = $1 GROUP BY 1, 2, 3) x
                     GROUP BY federation_id, session_index
                 ) c ON c.federation_id = s.federation_id AND c.session_index = s.session_index
                 ON CONFLICT DO NOTHING",
                &[&federation_id, &BATCH],
            )
            .await?;
        if n == 0 {
            return Ok(());
        }
    }
}
```

Spawn it per federation in `fmo_core/src/observer.rs` alongside the other per-federation tasks (mirror how the gold/fetcher tasks are spawned in `spawn_federation`), with a loop-restart backoff like the gold task. It is self-terminating once no gaps remain, then idles.

- [ ] **Step 4: Write the test** `fmo_core/tests/session_stats.rs`: seed a session with 2 transactions + CIs of two kinds via SQL, call `backfill_session_stats`, assert the `session_stats` row has `tx_count=2`, correct `ci_count`, and `items_by_kind` equal to the per-kind map; re-run and assert idempotent (no change, still one row). (Use `fmo_core::test_util` like `tests/gold.rs`.)

- [ ] **Step 5: Run tests** (`export FMO_TEST_DATABASE=...; nix develop -c just test_package fmo_core`), confirm they ran (not skipped). Lint `just clippy`.

- [ ] **Step 6: Commit** (`git add` the schema, migrations.rs, ingest.rs, session_stats.rs, observer.rs, tests/session_stats.rs, tests/schema.rs).

---

## Task 2: Session list + session detail endpoints

**Files:**
- Modify: `fmo_core/src/api/sessions.rs` (rewrite list; add detail)
- Modify: `fmo_core/src/api/federations.rs` (add `/:federation_id/sessions/:session_index` route)
- Modify: `fmo_api_types/src/lib.rs` (session types)
- Test: `fmo_core/tests/sessions_api.rs`

**Interfaces:**
- Consumes: `session_stats` (Task 1), `session_times`, `transactions`, `consensus_items`.
- Produces: `SessionSummary { session_index: i64, estimated_time: Option<i64>, tx_count: i64, items_by_kind: serde_json::Value }`; `SessionItem { session_index: i64, item_index: i64, item_type: String /* "transaction"|"ci" */, kind: Option<String>, peer_id: Option<i32>, txid: Option<String>, user_tx_key: Option<String>, details: Option<serde_json::Value> }` in `fmo_api_types`. `SessionItem.session_index` is always populated (redundant in the session-scope view, needed for the federation-wide stream) — one type serves both scopes.

- [ ] **Step 1: Types.** Add `SessionSummary` and `SessionItem` (serde `Serialize`/`Deserialize`, above shapes) to `fmo_api_types/src/lib.rs`.

- [ ] **Step 2: Failing test** `fmo_core/tests/sessions_api.rs`: seed 2 sessions (with session_stats + session_times + a tx and a CI each), call the new `federation_session_page(federation_id, before, limit)` and `federation_session_items(federation_id, session_index)` observer methods; assert the page returns rows newest-first with `tx_count`/`items_by_kind` from `session_stats`, and the item list unions the tx (`item_type="transaction"`, `txid` set) and CI (`item_type="ci"`, `kind`+`peer_id`+`details`) ordered by `item_index`.

- [ ] **Step 3: Implement** in `fmo_core/src/api/sessions.rs`:
  - `pub async fn federation_session_page(&self, federation_id, before: Option<i64>, limit: i64) -> anyhow::Result<Vec<SessionSummary>>` — keyset:

```sql
SELECT ss.session_index, EXTRACT(EPOCH FROM st.estimated_session_timestamp)::bigint AS estimated_time,
       ss.tx_count::bigint, ss.items_by_kind
FROM session_stats ss
LEFT JOIN session_times st ON st.federation_id = ss.federation_id AND st.session_index = ss.session_index
WHERE ss.federation_id = $1 AND ($2::int IS NULL OR ss.session_index < $2)
ORDER BY ss.session_index DESC
LIMIT $3
```

  - `pub async fn federation_session_items(&self, federation_id, session_index: i64) -> anyhow::Result<Vec<SessionItem>>` — union transactions + consensus_items for the session, ordered by `item_index`, left-joining `user_transaction_txs` for the tx's `user_tx_key`:

```sql
SELECT t.item_index::bigint, 'transaction' AS item_type, NULL::text AS kind, NULL::int AS peer_id,
       encode(t.txid,'hex') AS txid,
       (SELECT encode(utt.user_tx_key,'hex') FROM user_transaction_txs utt
        WHERE utt.federation_id=t.federation_id AND utt.txid=t.txid LIMIT 1) AS user_tx_key,
       NULL::jsonb AS details
FROM transactions t WHERE t.federation_id=$1 AND t.session_index=$2
UNION ALL
SELECT ci.item_index::bigint, 'ci', ci.kind, ci.peer_id, NULL, NULL, ci.details
FROM consensus_items ci WHERE ci.federation_id=$1 AND ci.session_index=$2
ORDER BY 1
```

  - Rewrite `list_sessions` to call `federation_session_page` (parse `before`/`limit` query params, default limit 50) and return `Json<Vec<SessionSummary>>`; add `session_items` axum handler calling `federation_session_items`.
- [ ] **Step 4: Route.** In `fmo_core/src/api/federations.rs` `get_federations_routes`, add `.route("/:federation_id/sessions/:session_index", get(super::sessions::session_items))`. (Keep `/sessions` and `/sessions/count`.)
- [ ] **Step 5: Run tests + clippy; Commit.**

---

## Task 3: Consensus item stream endpoint (keyset, filters, indexes)

**Files:**
- Create: `fmo_core/src/api/consensus.rs`
- Modify: `fmo_core/src/api/mod.rs` (`pub mod consensus;`) + `federations.rs` (route)
- Modify: `fmo_core/src/observer.rs` (an `ensure_explorer_indexes` startup step using CONCURRENTLY)
- Modify: `fmo_api_types/src/lib.rs` (reuse `SessionItem` + a cursor type)
- Test: `fmo_core/tests/consensus_api.rs`

**Interfaces:**
- Consumes: `transactions`, `consensus_items`, `user_transaction_txs`, `SessionItem` (from Task 2, which carries `session_index`).
- Produces: `ConsensusPage { items: Vec<SessionItem>, next: Option<(i64,i64)> }` (`next` = the last item's `(session_index, item_index)`, or `None` when fewer than `limit` returned). `pub async fn federation_consensus_page(&self, federation_id, filter: &str, before: Option<(i64,i64)>, limit: i64)`.

- [ ] **Step 1: Indexes (CONCURRENTLY, outside a tx).** Add `ensure_explorer_indexes(pool)` (run once at startup, before spawning federation tasks, idempotent) that executes — each as its own statement, not in a transaction:

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS transactions_by_session
    ON transactions (federation_id, session_index, item_index);
CREATE INDEX CONCURRENTLY IF NOT EXISTS consensus_items_stream
    ON consensus_items (federation_id, kind, session_index, item_index);
```

Run via `pool.get().await?.batch_execute` is NOT allowed for CONCURRENTLY-in-a-multi-statement; issue each with a separate `execute`/`simple_query` on a dedicated connection with no open transaction. Log start/finish (the consensus_items index build is minutes on 127M rows; it is one-time and non-blocking to writers).

- [ ] **Step 2: Failing test** `fmo_core/tests/consensus_api.rs`: seed a federation with several sessions' worth of txs and CIs of two kinds; assert `federation_consensus_page(fed, "all", None, 3)` returns the 3 newest items by `(session_index, item_index)` desc with a correct `next` cursor; `filter="transaction"` returns only tx items; `filter="ln"` returns only that kind; paging with the returned cursor yields the next distinct page (no overlap/gap).

- [ ] **Step 3: Implement** `federation_consensus_page` in `consensus.rs`. Keyset predicate `(session_index, item_index) < (before.0, before.1)` via row-compare. For `filter`:
  - `"transaction"`: query `transactions` only (item_type transaction, join user_tx_key).
  - `"all"`: `UNION ALL` of transactions + consensus_items, then `ORDER BY session_index DESC, item_index DESC LIMIT`.
  - else (a kind): `consensus_items WHERE kind = filter`.
  Compute `next` from the last row (or `None` if `< limit` returned). Add the axum handler `consensus_stream` parsing `filter` (default `"all"`), `before_session`/`before_item`, `limit` (default 50).
- [ ] **Step 4: Route + startup.** Add `.route("/:federation_id/consensus", get(super::consensus::consensus_stream))`; call `ensure_explorer_indexes` in the observer startup (once, before the per-federation task spawns).
- [ ] **Step 5: Tests + clippy; Commit.**

---

## Task 4: Rich transaction detail + gold user-transaction endpoint

**Files:**
- Modify: `fmo_core/src/api/transactions.rs` (structured detail + user_tx_key)
- Create: `fmo_core/src/api/user_transactions.rs` + `mod.rs`/`federations.rs` route
- Modify: `fmo_api_types/src/lib.rs`
- Test: `fmo_core/tests/user_tx_api.rs`

**Interfaces:**
- Produces: `TxDetail { txid, session_index, item_index, inputs: Vec<TxItemPart>, outputs: Vec<TxItemPart>, user_tx_key: Option<String> }`, `TxItemPart { index, kind, amount_msat: Option<i64>, details: Option<serde_json::Value> }`; `UserTransaction { kind, direction, amount_msat, fedimint_fee_msat, gateway_fee_estimate_msat, num_fedimint_txs, first_timestamp, last_timestamp, member_txs: Vec<MemberTx> }`, `MemberTx { txid, role, session_index }`.

- [ ] **Step 1: Types** in `fmo_api_types`.
- [ ] **Step 2: Failing tests** `fmo_core/tests/user_tx_api.rs`: seed a gold `user_transactions` row + 3 `user_transaction_txs` (roles offer/fund/claim) + the underlying transactions with `transaction_inputs/outputs` (kinds+amounts). Assert `federation_transaction_detail(fed, txid)` returns structured inputs/outputs (kind+amount from the tables) and the correct `user_tx_key`; assert `federation_user_transaction(fed, user_tx_key)` returns the summary + 3 member txs with their roles.
- [ ] **Step 3: Implement.**
  - `federation_transaction_detail`: read `transactions` row (session_index, item_index) + `transaction_inputs`/`transaction_outputs` (index, kind, amount_msat, details) ordered by index, + the `user_tx_key` via `user_transaction_txs`. (This is the structured replacement for the Debug-string `transaction_details`; keep the old one if referenced elsewhere, else repurpose.)
  - `federation_user_transaction`: read the `user_transactions` row by `(federation_id, user_tx_key)` + its `user_transaction_txs` ordered by `session_index, role`.
  - Add axum handlers + routes `/:federation_id/tx/:txid` and `/:federation_id/user-transactions/:user_tx_key` (hex-decode the key param to bytea).
- [ ] **Step 4: Tests + clippy; Commit.**

---

## Task 5: Frontend — API client + shared item-list + renderer registry

**Files:**
- Modify: `fmo_frontend_react/src/services/api.ts` (new methods via the existing `request`/`authedFetch`)
- Modify: `fmo_frontend_react/src/types/api.ts` (mirror the new types)
- Create: `src/components/explorer/ItemList.tsx`, `src/components/explorer/itemRenderers.tsx`
- Test: `src/components/explorer/itemRenderers.test.tsx`

**Interfaces:**
- Produces: `api.getSessionPage`, `api.getSessionItems`, `api.getConsensusPage`, `api.getTxDetail`, `api.getUserTransaction`; `<ItemList items scope onLoadMore/>`; `renderItem(item): ReactNode` registry.

- [ ] **Step 1: API methods** on the `api` object routing through the existing `request<T>()` (keyset params as query string). Mirror the Rust types in `types/api.ts`.
- [ ] **Step 2: Renderer registry** `itemRenderers.tsx`: `renderItem(item: SessionItem): ReactNode` switching on `item_type`/`kind`.
  - Provide the **full** transaction renderer (badge for the tx's classification, list of input/output kinds+amounts via the tx-detail lazy-load or the item summary, and the "Part of user transaction →" link when `user_tx_key` set).
  - Provide **full** CI renderers for `ln` (preimage decryption / block-count), `lnv2` (unix-time / block-count vote), `wallet`/`walletv2` (block-height vote), and `meta`, each reading `details` and showing guardian (peer) + key fields.
  - Provide the **raw-JSON fallback** (`<details><pre>{JSON.stringify(details,null,2)}</pre></details>`) for any unhandled/undecoded kind. Add new kinds by extending the switch — enumerate the kinds above as the required set; anything else uses the fallback.
- [ ] **Step 3: `ItemList` component** — renders a list via `renderItem`, an IntersectionObserver "load more" sentinel calling `onLoadMore` with the last cursor, loading/empty/error states. Used by both the session detail and consensus tab.
- [ ] **Step 4: Test** `itemRenderers.test.tsx`: a transaction item renders its classification + user-tx link; an `ln` CI renders a friendly summary; an unknown-kind CI renders the raw-JSON fallback (assert the `<pre>` is present). Run `npm test`, `npm run build`.
- [ ] **Step 5: Commit.**

---

## Task 6: Frontend — Sessions tab + session detail

**Files:**
- Modify: `fmo_frontend_react/src/pages/FederationDetail.tsx` (add "Sessions" tab)
- Create: `src/components/explorer/SessionsTab.tsx`, `src/pages/SessionDetail.tsx`
- Modify: `src/App.tsx` (route `/federations/:id/session/:idx`)
- Test: `src/components/explorer/SessionsTab.test.tsx`

- [ ] **Step 1: `SessionsTab`** — infinite-scroll session list (via `api.getSessionPage`), each row: session_index, formatted `estimated_time`, `tx_count`, and per-kind badges from `items_by_kind` (map to `<Badge>`); row links to `/federations/:id/session/:idx`.
- [ ] **Step 2: `SessionDetail` page** — loads `api.getSessionItems`, renders via `<ItemList>` (session scope), shows session header (index, time, counts).
- [ ] **Step 3: Wire** the "Sessions" tab into `FederationDetail`'s tab set (extend the `activeTab` union + tab list) and add the route in `App.tsx`.
- [ ] **Step 4: Test** `SessionsTab.test.tsx` (mock `api`): renders rows with badges, clicking loads more. `npm test` + `npm run build`. Commit.

---

## Task 7: Frontend — Consensus tab (filters + infinite scroll)

**Files:**
- Create: `src/components/explorer/ConsensusTab.tsx`
- Modify: `src/pages/FederationDetail.tsx` (add "Consensus" tab)
- Test: `src/components/explorer/ConsensusTab.test.tsx`

- [ ] **Step 1: `ConsensusTab`** — filter chips (All / Transactions / one per module kind present in the federation, derived from a session-stats aggregate or a static kind list), calling `api.getConsensusPage(filter, cursor)`; renders via `<ItemList>` (federation scope) with keyset "load more"; switching a filter resets the cursor.
- [ ] **Step 2: Wire** the tab into `FederationDetail`.
- [ ] **Step 3: Test** (mock `api`): switching filter re-queries with the right `filter` and resets; load-more passes the cursor. `npm test` + `npm run build`. Commit.

---

## Task 8: Frontend — Transaction detail + User-transaction page

**Files:**
- Create: `src/pages/TransactionDetail.tsx`, `src/pages/UserTransaction.tsx`
- Modify: `src/App.tsx` (routes `/federations/:id/tx/:txid`, `/federations/:id/user-tx/:key`)
- Test: `src/pages/UserTransaction.test.tsx`

- [ ] **Step 1: `TransactionDetail`** — `api.getTxDetail`, renders the rich tx (inputs/outputs by kind+amount) + "Part of user transaction: [kind · amount] →" link to `/federations/:id/user-tx/:user_tx_key` when set.
- [ ] **Step 2: `UserTransaction` page** — `api.getUserTransaction`, renders the gold summary (kind, direction, amount, fees, timestamps, num txs) + the member-tx list with role badges, each linking to `/federations/:id/tx/:txid`.
- [ ] **Step 3: Routes** in `App.tsx`; ensure the item/session renderers link into these pages (close the navigation graph).
- [ ] **Step 4: Test** `UserTransaction.test.tsx` (mock `api`): summary + member-tx rows with roles render and link correctly. `npm test` + `npm run build`. Commit.

---

## Final verification

- `just clippy` + `just test_package fmo_core` (with `FMO_TEST_DATABASE`) green; `cd fmo_frontend_react && npm test && npm run build` green.
- `just final-check` before done.
- **Deployment (user-gated, after the plan):** the v3 migration + the `session_stats` backfill run on deploy (the backfill is a background catch-up over ~8.4M sessions; the two explorer indexes build CONCURRENTLY once — minutes on the 127M-row `consensus_items`, non-blocking). Frontend served from the private vhost. Not part of the automated run.

## Notes on ordering & dependencies

- Backend Tasks 1→4 are largely independent but share `fmo_api_types` + routing; do them in order (1 first — `session_stats` underpins Task 2). Frontend Tasks 5→8 depend on the backend endpoints and on Task 5's shared component/registry. 5 before 6/7; 8 after 4 (needs the tx/user-tx endpoints).
- Task 3's CONCURRENTLY index build is the one operation that can't run in a migration transaction — keep it in the dedicated `ensure_explorer_indexes` startup step.
