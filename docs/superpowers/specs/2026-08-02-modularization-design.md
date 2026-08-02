# Fedimint Observer Modularization — Design

**Date:** 2026-08-02
**Issue:** [#8 Modularize Observer](https://github.com/fedimint/fedimint-observer/issues/8)
**Status:** Approved by elsirion (2026-08-02)

## Context

`fmo_server` currently mixes three responsibilities (per issue #8):

1. Fetching raw session data from federations
2. Decoding session data into a normalized, queryable form
3. Making data available via the API (denormalizing for efficiency)

Module-specific decoding (mint/wallet/ln/stability_pool) is hardcoded in
`observer.rs` as `match kind.as_str()` blocks with `.expect("Not v0")` panics;
stability_pool is a Cargo feature flag. Adding a module requires forking FMO.

**Goals (in order):**

1. Fix the architecture: separate the three responsibilities, introduce a
   module interface so third parties can build a custom FMO with a ~10-line
   `main.rs` (`FedimintObserver::builder().with_module(...)`).
2. Implement support for the next-generation fedimint v2 modules (lnv2 now;
   walletv2/mintv2 as upstream stabilizes them).

**Non-goals:**

- In-place migration of an existing v8-schema database (replaced by an import
  path; see below).
- Modularizing the denormalization/API layer beyond mounting module
  sub-routers (issue #8 considers this low RoI).
- DB privilege isolation between modules (modules are compiled into one
  binary; per-module schemas are for namespacing only).

## Decisions

| Question | Decision |
|---|---|
| "v2 module support" means | New fedimint v2 modules (lnv2 first, walletv2/mintv2 later) |
| Base | Fedimint 0.10, frontend removed (PR #115) — merged into the rewrite branch, **not** master first |
| Module interface scope | Full: decoding + own migrations + background tasks + API sub-routers |
| Old data | Fresh DB + import tool reading a v8 database; no in-place migration |
| DB layout | One Postgres schema per module (`fmo_<kind>`), namespacing only |
| Denormalization sync | Rust-side, in-transaction; matview refresh loop for the rest |
| Workspace | `fmo_core` library + one crate per module + thin `fmo_server` binary |
| HTTP API | Existing `/federations/*` endpoints keep working via compat shims |
| Staging | Single rewrite branch (approach A); reviewable stacked commits/sub-PRs inside it; master frozen until merge |

Resolutions to inconsistencies in issue #8 as written:

1. **Amounts cannot be filled by the fetch layer** (amount extraction is
   module-specific; ln/stability_pool have zero-amount cases). The global
   `amount_msat` columns are nullable and filled by module dispatch via the
   `ProcessedItem` return value; NULL for uninstalled kinds.
2. **Fetching and processing get separate cursors.** Fetch cursor per
   federation (`MAX(session_index)` over bronze `sessions`); processing cursor
   per `(module_kind, federation)`. This enables adding a module later and
   replaying history without refetching, and makes import nearly free.
3. **Decoders are supplied by modules**, with fedimint's raw fallback for
   unknown kinds. Nothing panics on unknown data; later-installed modules
   decode retroactively via replay.

## Section 1: Workspace layout & module interface

```
fmo_api_types/            shared API types (kept)
fmo_core/                 NEW: library — everything module-agnostic
  ├─ db/                  pool, core migrations, cursor management
  ├─ fetch/               session fetcher (per-federation task)
  ├─ dispatch/            decode + dispatch to modules, replay engine
  ├─ api/                 axum router assembly + core endpoints
  ├─ module/              ObserverModule trait + registry + ProcessCtx
  └─ services/            block times, guardian health, nostr, meta cache
fmo_modules/
  ├─ fmo_module_mint/
  ├─ fmo_module_wallet/
  ├─ fmo_module_ln/
  ├─ fmo_module_lnv2/     the "v2 support" deliverable
  └─ fmo_module_stability_pool/   in-tree, NOT in the default binary;
                                  reference example of a custom build
fmo_server/               thin binary: builder call + compat API shims
```

### ObserverModule trait (sketch)

```rust
#[async_trait]
pub trait ObserverModule: Send + Sync + 'static {
    fn kind(&self) -> ModuleKind;
    fn decoder(&self) -> Decoder;
    /// Bump to force: drop module schema, reset cursor, replay from raw sessions
    fn version(&self) -> u32;
    /// Run inside the module's own PG schema (search_path pre-set)
    fn migrations(&self) -> Vec<Migration>;

    async fn process_input(&self, ctx: &mut ProcessCtx<'_>, input: &DynInput, meta: &ItemMeta)
        -> Result<ProcessedItem>;   // { amount: Option<Amount>, details: Option<Json> }
    async fn process_output(&self, ctx: &mut ProcessCtx<'_>, output: &DynOutput, meta: &ItemMeta)
        -> Result<ProcessedItem>;
    async fn process_ci(&self, ctx: &mut ProcessCtx<'_>, ci: &DynModuleConsensusItem, meta: &CiMeta)
        -> Result<Option<Json>>;

    fn background_tasks(&self) -> Vec<ModuleTask> { vec![] }   // e.g. LN gateway polling
    fn api_router(&self) -> Option<Router<ModuleApiState>> { None }
}
```

- `ProcessCtx` provides: a DB transaction scoped to the module's PG schema,
  federation id + the module's config section, and shared core services
  (esplora client, block-time lookup, `session_time_votes` helper).
- `ItemMeta`/`CiMeta` carry federation id, txid, session/item/input/output
  indices, proposing peer for CIs.
- Core (not the module) writes returned `amount`/`details` into the global
  `transaction_inputs/outputs`/`consensus_items` columns — modules never touch
  core tables directly.
- Builder API per elsirion's issue comment:

```rust
FedimintObserver::builder()
    .with_module(MintObserver)
    .with_module(WalletObserver)
    .with_module(LnObserver)
    .with_module(LnV2Observer)
    .serve(opts)
    .await
```

- stability_pool drops its feature flag; its crate ships a tiny example binary
  demonstrating a custom FMO build.

## Section 2: Database layout & processing pipeline

### Core schema (bronze + structural silver, module-agnostic)

- `federations(federation_id, config, ...)`
- `sessions(federation_id, session_index, data)` — raw bytes, append-only;
  fetch cursor = `MAX(session_index)` (today's `COUNT(*)` assumption goes away)
- `transactions(federation_id, txid, session_index, item_index, data)`
- `transaction_inputs(federation_id, txid, in_index, kind, amount_msat NULL, details JSONB NULL)`
- `transaction_outputs(federation_id, txid, out_index, kind, amount_msat NULL, details JSONB NULL)`
- `consensus_items(federation_id, session_index, item_index, peer, kind, details JSONB NULL)`
- `module_progress(module_kind, federation_id, last_session_index, module_version)`
- `session_time_votes(federation_id, session_index, source_kind, timestamp)`
- Core services tables: `block_times`, `guardian_health*`, `nostr_*`;
  `session_times` materialized view.

### session_time_votes

`session_times` (used by nearly every API query) is currently derived from
`block_height_votes` — wallet module data — which would make core depend on
one module's tables. Instead, core owns `session_time_votes`, and any module
may contribute rows via a `ProcessCtx` helper: the wallet module converts
height votes to timestamps using the core block-times service; lnv2
contributes its consensus time votes directly. Core aggregates and
forward-fills into the `session_times` matview; the system degrades
gracefully with no time-aware module installed.

### Per-module schemas

`fmo_wallet` (peg_ins, withdrawal_*, block_height_votes, utxos matview),
`fmo_ln` (contracts, gateways), `fmo_mint`, `fmo_lnv2`. Each has its own
`schema_version` table and migration lineage. Module reprocessing =
`DROP SCHEMA ... CASCADE` + cursor reset + replay. Foreign keys from module
tables to core tables are allowed.

### Pipeline

- **Fetch task** (per federation): writes bronze `sessions` + structural
  tables only. No module decoding — fetch works even for kinds nobody
  installed.
- **Processor task** (per federation): reads sessions above the minimum
  module cursor, decodes once with the combined decoder registry (raw
  fallback for unknown kinds), dispatches each item to its module.
- **Per-session, per-module transactions**: a module's writes and its cursor
  advance commit atomically. Modules progress independently; one broken
  module logs errors and stalls only itself (today a single panic kills the
  whole observer loop).
- **Replay = catch-up**: live processing and historical replay are the same
  code path; a newly added module starts at cursor 0.
- **Idempotency**: all inserts remain `ON CONFLICT DO NOTHING` so
  crash-resume and replay are safe.
- **Denormalization**: Rust-side, in the same transaction as processing;
  remaining matviews refreshed by the existing loop with a configurable
  interval (aligns with PR #117).

## Section 3: API compatibility & import

### API

- Core router keeps today's paths: `/federations`, `/:id`, `/:id/config`,
  `/:id/meta`, `/:id/health`, `/:id/transactions*`, `/:id/sessions*`,
  `/:id/backfill`, `/totals`, nostr routes, and the stable `/config/*` API.
- Module routers mount under `/federations/:id/modules/:kind/*`.
- Module-flavored legacy endpoints become thin compat shims in `fmo_server`
  forwarding to the owning module: `/:id/utxos` → wallet,
  `/:id/nonces/spend` → mint. The React frontend keeps working unmodified.
- New features (gateways, lnv2 data) appear only under module routes, with
  `/federations/:id/gateways` as a compat alias for Ayus's #109 API.

### Import tool

`fmo_server import --from <old-db-url>`:

1. Copies `federations` (configs), raw `sessions`, and `block_times` (saves
   re-hitting esplora) from a v8-schema database into the new core schema.
2. Normal replay machinery rebuilds all derived data — works for dead
   federations since no network access is needed.
3. Verification: session counts per federation match the source; spot-check
   txids/totals against the old DB.

Consensus encoding of `SessionOutcome` is consensus-critical and therefore
stable across fedimint versions, so 0.8-era bytes decode under 0.10. A
fixture-based round-trip test (snapshot of real session data) proves this
before we rely on it.

## Section 4: Sequencing, v2 modules, open PRs, testing

### Sequencing (single rewrite branch; master frozen until merge)

1. Create the rewrite branch; merge PR #115 (fedimint 0.10, frontend removal)
   into it — **not** into master first. Cherry-pick small fixes (#112, perf
   PRs #117/#118/#119) into the branch as needed.
2. On the branch, as stacked reviewable commits/sub-PRs:
   crate split → core schema + fetch layer → dispatch/replay engine →
   port mint/wallet/ln/stability_pool logic into module crates (existing
   `match` arms become module impls near-mechanically) → API + compat shims →
   import tool.
3. `fmo_module_lnv2` (contracts, gateway registration CIs, time votes) — the
   first validation that a new module needs zero core changes.
4. walletv2/mintv2: new crates whenever upstream stabilizes them; no core
   changes expected.

### Ayus's (bansalayush247) PRs

- #109 gateway monitoring → ported into `fmo_module_ln` as a background task
  + module API route + compat alias. Coordinate with Ayus: he rebases onto
  the module interface (good validation of its docs), or we port and credit
  him. Not a blocker ("can also wait").
- #113 guardian version tracking → core service; port into the branch.
- #116 UTXO vs guardian claims → wallet-module feature, afterwards.

### Testing

- Unit tests per module processor against fixture item bytes.
- End-to-end test: process fixture raw sessions, assert normalized +
  denormalized table contents.
- Import-tool test against an old-schema fixture dump.
- Replay-idempotency test: process twice, assert identical state.
- CI remains `just final-check`.

### Deployment

Stand up new DB → run import from production DB → let replay complete →
verify counts/totals against the old instance → switch traffic → keep the
old DB as backup.
