# Session & Consensus Explorer Design (SP‑1)

**Date:** 2026-08-12
**Status:** Approved, ready for planning
**Branch:** modularization (no PR — local branch only, per standing constraint)

## Where this fits

This is **SP‑1**, the second of three sub-projects decomposed from a feature request
(SP‑3, gateway observation + LN status, is done and deployed). The remaining sibling is
out of scope here:

- **SP‑2 — Live consensus view** (subscribe to the *pending* session via `get_session_status`
  and stream items to the browser). Builds on SP‑1's item-list component and gets its own
  spec/plan later.

## Goal

A polished, public-facing explorer for a federation's raw consensus, in three linked views:

1. **Session explorer** — a paginated list of consensus sessions (best-effort timestamp, tx
   count, per-module item counts), each drilling into that session's full ordered item list.
2. **Consensus item explorer** — a federation-wide, infinite-scroll stream of consensus items,
   filterable by transactions or by module kind.
3. **User-transaction drill-in** — from any fedimint transaction, navigate to the deduplicated
   gold-layer *user* transaction it belongs to, and see all of that user transaction's member
   fedimint txs (with their roles). This exposes the gold layer via API for the first time.

## Context / Current State

- **A session's items span two tables, sharing `item_index`.** The ingest (`fmo_core/src/ingest.rs`)
  splits each session's `AcceptedItem`s: `ConsensusItem::Transaction` → the `transactions` table
  (+ `transaction_inputs`/`transaction_outputs`), `ConsensusItem::Module` → `consensus_items`.
  So a session's full ordered item list is the UNION of both by `item_index`.
- **`consensus_items`** = `(federation_id, session_index, item_index, peer_id, kind, details JSONB)`,
  PK `(federation_id, session_index, item_index)`, index `(federation_id, kind)`. `kind` is the
  **module kind** (lnv2/wallet/ln/stability_pool/multi_sig_stability_pool/walletv2/meta). `details`
  is pre-decoded JSON written by module dispatch (`process_ci`). It is **huge** — ~127M rows on the
  private instance (lnv2 alone ~91M), so pagination must be keyset, never `OFFSET`. Undecoded kinds
  (`stability_pool`, `multi_sig_stability_pool` — 8M+ rows, no observer module) have null/minimal
  `details`.
- **`transactions`** = `(federation_id, txid, session_index, item_index, data BYTEA)`, PK
  `(federation_id, txid)`; `data` is the raw consensus-encoded tx. The existing
  `transaction_details` handler decodes it into inputs/outputs with module-kind labels.
- **`session_times`** matview gives `estimated_session_timestamp` per `(federation_id, session_index)`.
- **Gold layer** (built earlier, never API-exposed): `user_transactions(federation_id, user_tx_key,
  kind, direction∈{in,out,internal}, amount_msat, fedimint_fee_msat, gateway_fee_estimate_msat,
  num_fedimint_txs, first_session_index, first_timestamp, last_timestamp)` PK `(federation_id,
  user_tx_key)`; `user_transaction_txs(federation_id, txid, user_tx_key, role, session_index)`,
  PK `(federation_id, txid, user_tx_key)`, index `(federation_id, user_tx_key)`. `role` ∈
  offer/fund/claim/cancel/refund/self. `user_tx_key` = contract_id for LN, txid otherwise. So a
  fedimint tx resolves to its user tx via `user_transaction_txs` by txid, and a user tx lists its
  member txs via the by-user-key index.
- **Current API is thin**: session list returns only `{session_index: {transactions: n}}`, no
  timestamps or per-module counts — and it computes those tx counts with an **unbounded**
  `LEFT JOIN transactions … GROUP BY session_index` over *all* sessions (no pagination), which is
  O(all sessions) per call (271k+ on a busy federation). The transaction list also has **no
  pagination**; there is **no consensus-item endpoint** and **no gold-layer endpoint**.
- **Sessions are immutable once ingested** — a finalized session's items never change — so any
  per-session aggregate can be computed exactly once and read back O(1).
- **Frontend** (`fmo_frontend_react`): per-federation `FederationDetail` page already uses a tab
  pattern (`activity | utxos | config`) and a `Tabs` component; routes are `/`, `/nostr`,
  `/federations/:id`. Central `api.ts` funnels all calls (now bearer-auth-aware from SP‑bearer work).

## Decisions (locked)

1. **Polished, public-facing** rendering — not a raw dump. Designed to eventually ship on
   observer.fedimint.org; **deployed to the private instance only for now**.
2. **Rich transactions, functional consensus items.** Transactions get first-class human-readable
   rendering; module CIs get a clean, consistent presentation (kind, guardian, a friendly summary
   where a decoder exists, raw-JSON fallback for undecoded kinds).
3. **One item model, two scopes.** A single item-list API (scope = whole federation | one session;
   filter = transaction | module kind) and a single frontend item-list component power both the
   session drill-in and the consensus explorer. Keyset pagination throughout.
4. **Gold-layer drill-in.** Fedimint tx → its user transaction → all member fedimint txs. Exposes
   `user_transactions`/`user_transaction_txs` read-only via API.
5. **Full UI now** (not API-only), on `FederationDetail` tabs + deep-linkable detail routes.
6. **Precomputed per-session stats.** A core `session_stats` table holds each session's `tx_count`
   and per-kind consensus-item counts, written once at ingest (immutable thereafter). The session
   list reads it directly — O(1) per row, keyset-paginated — instead of grouped counts or the
   current unbounded join.

## Architecture

**Shared item model.** An "item" is either a transaction or a module consensus item, addressed by
`(session_index, item_index)`. Two scopes over the same model:
- *Session scope* (`GET /sessions/:idx`): all items of one session, tx ⊔ CI, ordered by `item_index`
  — small, no pagination.
- *Federation scope* (`GET /consensus`): the whole federation's item stream, keyset-paginated by
  `(session_index desc, item_index desc)`, with a filter.

**Keyset pagination.** Cursor is `(session_index, item_index)`; each page returns the next cursor.
No `OFFSET`. Filter handling:
- `filter=transaction` → keyset over `transactions` (needs an index `(federation_id, session_index,
  item_index)` — **add it**, a core append-only migration).
- `filter=<kind>` → keyset over `consensus_items WHERE kind=$` (needs an index `(federation_id, kind,
  session_index, item_index)` for the filtered order over the 127M rows — **add it**).
- `filter=all` → a keyset UNION of both tables under the same `(session,item)` cursor.

**Backend decoding.** CIs: serve the pre-decoded `details` JSON as-is (the frontend renders per
kind). Transactions: reuse/extend `transaction_details` to return decoded inputs/outputs with
module-kind labels; join `user_transaction_txs` to include the item's `user_tx_key` so the UI links
to the gold layer without a second call.

**Precomputed session stats.** A new core table `session_stats(federation_id, session_index,
tx_count INT, ci_count INT, items_by_kind JSONB)`, PK `(federation_id, session_index)`, FK to
`sessions`. Populated **at ingest**: `ingest_session` already iterates a session's items, so it
tallies `tx_count` and the per-kind CI counts in Rust and `INSERT … ON CONFLICT DO NOTHING`
(idempotent for crash-resume/replay). Because sessions are immutable, the row is written once and
never updated. Pre-existing sessions are filled by a **one-time background backfill** (a batched
`INSERT INTO session_stats SELECT … GROUP BY session_index` over `transactions`+`consensus_items`,
by session range, resumable via a cursor — analogous to a lightweight replay, not a blocking
migration). The `GET /sessions` list then reads `session_stats` directly (keyset by
`session_index`), and `items_by_kind` is served straight from the JSONB — no per-page grouped
counts, and the current unbounded tx-count join is removed.

## API (new/extended, all under `/federations/:id`)

- `GET /sessions?before=<session_index>&limit=N` — keyset-paginated session list; each row
  `{session_index, estimated_time, tx_count, items_by_kind:{ln:3,wallet:2,…}, next_cursor}`, read
  directly from `session_stats` joined to `session_times` (no grouped counts at request time).
- `GET /sessions/:idx` — one session's ordered items: `[{item_index, type:'transaction'|'ci',
  kind, peer_id?, summary, txid?, user_tx_key?}]` (tx items carry `txid`+`user_tx_key`; CI items
  carry `kind`+`peer_id`+`details`).
- `GET /consensus?filter=<all|transaction|kind>&before=<session,item>&limit=N` — federation-wide
  filtered keyset stream of the same item shape.
- `GET /transactions/:txid` — extend the existing detail: decoded inputs/outputs + module kinds +
  the tx's `user_tx_key` (nullable).
- `GET /user-transactions/:key` — gold user-tx: `{kind, direction, amount_msat, fedimint_fee_msat,
  gateway_fee_estimate_msat, num_fedimint_txs, first/last_timestamp, member_txs:[{txid, role,
  session_index}]}`.

All shapes are added to `fmo_api_types` (shared FE/BE types). Endpoints join the (unstable)
`/federations` API; the config API is untouched.

## Frontend

New tabs on `FederationDetail` + deep-linkable detail routes (via `react-router`, so drill-ins are
shareable URLs — unlike the current local-state tabs):
- **Sessions tab** — infinite-scroll list: time · tx count · per-module item badges; row → session
  detail.
- **Session detail** — the shared item-list component scoped to one session.
- **Consensus tab** — the shared item-list component scoped to the federation, with filter chips
  (All / Transactions / per module kind); infinite scroll.
- **Item renderer registry** — a lookup from `(type, kind)` to a renderer:
  - *transaction* → rich card (peg-in/out, LN send/receive, ecash, using the decoded inputs/outputs
    and, where available, the gold `kind`), with a **"Part of user transaction: [kind · amount] →"**
    link when `user_tx_key` is set.
  - *ci* by kind → friendly summary for decodable kinds (LN preimage decryption, wallet/lnv2 block
    & time votes, meta), showing guardian (peer) and key fields; **raw-JSON `<details>` fallback**
    for undecoded/unknown kinds.
- **Transaction detail route** `/federations/:id/tx/:txid` — rich tx view + the user-tx link.
- **User-transaction page** `/federations/:id/user-tx/:key` — gold summary (kind/direction/amount/
  fees/timestamps/count) + the member fedimint txs with roles (offer/fund/claim/…), each linking
  back to its tx detail / session.
- Navigation graph (all deep-linkable): `session → item → (tx) tx-detail → user-tx → member txs`.

Rendering reuses existing primitives (`Tabs`, `Badge`, `Alert`, `Copyable`, formatting utils) and
the theme; new pieces are the item-list component, the renderer registry, and the two detail pages.

## Performance & Testing

- Keyset pagination end-to-end; the two new composite indexes above make the filtered streams cheap
  on the 127M-row table. The session list reads precomputed `session_stats` (O(1) per row), so it
  does no counting at request time.
- **Backend tests** (DB-gated, existing harness): `session_stats` populated correctly at ingest
  (tx_count + items_by_kind) and idempotent on re-ingest; the backfill fills a pre-existing session;
  session list shape reads from `session_stats`; session item union ordering; consensus keyset
  pagination + each filter (transaction / kind / all) incl. cursor correctness; tx detail
  `user_tx_key` join; user-tx assembly (summary + member roles). Seed via SQL like the gold tests.
- **Frontend tests** (vitest + testing-library, from SP‑bearer): the item-list component
  (pagination/scroll, filter switching), the renderer registry (a rich-tx render, a functional-CI
  render, the raw-JSON fallback), and the user-tx page (member-tx list + links).

## Global Constraints

- Pagination is **keyset only** (cursor `(session_index, item_index)`); never `OFFSET` — the
  `consensus_items` table is ~127M rows.
- Consensus-item `details` are served as pre-decoded JSON; the frontend owns per-kind rendering.
  Undecoded kinds must degrade to a raw-JSON view, never error.
- New API types live in `fmo_api_types` and are shared by FE and BE.
- Public-ready but **deployed to the private instance only** for now; nothing about the design may
  assume private-only (it must be safe to ship publicly later).
- Core schema additions are append-only migrations; module schemas untouched (this is core + FE).
- `session_stats` is written once at ingest and never mutated (sessions are immutable); its ingest
  population must be idempotent (`ON CONFLICT DO NOTHING`) so crash-resume and replay are safe.
- Work stays on the `modularization` branch; no PR, nothing pushed.

## Out of Scope / Non-Goals

- SP‑2 live/pending-session view (`get_session_status` streaming).
- Bespoke friendly rendering of *every* CI variant, or of undecoded kinds (raw-JSON fallback only).
- Public deployment (design is public-ready; only the private instance runs it now).
- Writes of any kind — the explorer and gold endpoints are read-only.
