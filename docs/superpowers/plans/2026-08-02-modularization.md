# Fedimint Observer Modularization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restructure fedimint-observer into a module-agnostic core (`fmo_core`) plus per-module crates with a builder API, per-module DB schemas and replay cursors, an import path for v8 databases, and a new `lnv2` observer module.

**Architecture:** Three layers per issue #8: (1) fetch writes raw sessions + structural tables using only fallback decoding; (2) a processor dispatches decoded items to `ObserverModule` implementations, each writing its own PG schema and progressing on its own cursor; (3) Rust-side denormalization + matviews serve the API, with old endpoint paths kept as compat shims. Spec: `docs/superpowers/specs/2026-08-02-modularization-design.md`.

**Tech Stack:** Rust, axum 0.7, tokio-postgres/deadpool, fedimint 0.10 (tag v0.10.0), PostgreSQL.

## Global Constraints

- Work happens directly on the `modularization` branch. **No PRs, no pushes** unless the user asks.
- fedimint deps: `{ version = "0.10.0", git = "https://github.com/fedimint/fedimint", tag = "v0.10.0" }` (arrives via merge of PR #115).
- Master is frozen: never merge to or rebase onto master as part of this plan.
- New DB schema is a fresh lineage (core `v0.sql`); no in-place migration from the old v8 schema. Old data arrives via `fmo_server import`.
- All processing inserts idempotent (`ON CONFLICT DO NOTHING`); replay must be safe.
- Modules never write core tables directly; amounts/details flow through `ProcessedItem` return values.
- Per-module PG schema named `fmo_<kind>` (e.g. `fmo_wallet`); module SQL runs with `search_path = fmo_<kind>, public`.
- Existing HTTP paths under `/federations/*` and `/config/*` must keep responding with the same shapes (React frontend unmodified).
- Dev DB: `just pg_start`; DSN `postgres://user@/postgres?host=<repo>/.pg_dev&port=5432`. DB-touching tests are gated: skip unless `FMO_TEST_DATABASE` env var is set.
- Verification per task: `just clippy` clean + `cargo test --workspace` green (DB tests run when `FMO_TEST_DATABASE` set).
- Commit after every task (small, descriptive commits). Pre-commit hook is fixed after Task 1; until then use `--no-verify` only for the frontend-breakage false positive.

---

## Phase 0 — Branch setup

### Task 1: Merge PR #115 into the `modularization` branch

**Files:**
- Modify: whole tree (merge); no hand-edits except conflict resolution

**Interfaces:**
- Produces: workspace on fedimint 0.10, `fmo_frontend/` deleted, `fedimint-connectors` available. All later tasks assume this state.

- [ ] **Step 1: Fetch and merge the PR head**

```bash
git fetch origin pull/115/head
git merge --no-ff FETCH_HEAD -m "Merge PR #115: remove fmo_frontend, upgrade fedimint to 0.10"
```

Expected conflicts: `CLAUDE.md`, possibly `Cargo.lock`. Resolve by taking #115's side for build files; for `CLAUDE.md` keep #115's version. The design/plan docs under `docs/superpowers/` only exist on our side — keep ours.

- [ ] **Step 2: Verify the workspace builds**

Run: `just clippy`
Expected: clean (warnings-as-errors per justfile). If `stability_pool_v1` feature breaks the build, it is only compiled with `--features stability_pool_v1` — default build must pass.

- [ ] **Step 3: Verify tests pass**

Run: `cargo test --workspace`
Expected: PASS (only `last_n_day_iter` unit test + doc tests exist).

- [ ] **Step 4: Commit conflict resolution if any**

```bash
git add -A && git commit -m "chore: resolve #115 merge conflicts" || echo "clean merge, nothing to do"
```

---

## Phase 1 — fmo_core skeleton

### Task 2: Create `fmo_core` crate; move shared plumbing

**Files:**
- Create: `fmo_core/Cargo.toml`, `fmo_core/src/lib.rs`
- Move: `fmo_server/src/util.rs` → `fmo_core/src/db/query.rs` (only the five query helpers `execute/query/query_one/query_opt/query_value`), `fmo_server/src/error.rs` → `fmo_core/src/error.rs`
- Modify: `Cargo.toml` (workspace members), `fmo_server/Cargo.toml` (dep on fmo_core), `fmo_server/src/main.rs` and all `use crate::util::…`/`use crate::error::…` sites → `use fmo_core::…`

**Interfaces:**
- Produces: `fmo_core::db::query::{execute, query, query_one, query_opt, query_value}` (signatures unchanged from current `util.rs:83-141`), `fmo_core::error::{AppError, Result}`.
- `config_to_json`, `get_decoders`, `merge_metas` stay in `fmo_server` for now (moved in later tasks).

- [ ] **Step 1: Create the crate**

`fmo_core/Cargo.toml`:

```toml
[package]
name = "fmo_core"
version = "0.1.0"
edition = "2021"

[dependencies]
anyhow = "1.0.81"
async-trait = "0.1"
axum = { version = "0.7.5", features = ["json"] }
chrono = { version = "0.4.38", features = ["serde"] }
deadpool-postgres = "0.14.0"
esplora-client = { version = "0.10.0", default-features = false, features = ["async-https-rustls"] }
fedimint-api-client = { workspace = true }
fedimint-connectors = { workspace = true }
fedimint-core = { workspace = true }
fmo_api_types = { path = "../fmo_api_types" }
futures = "0.3.30"
postgres-from-row = "0.5.2"
serde = { version = "1.0.197", features = ["derive"] }
serde_json = "1.0.115"
tokio = { version = "1.37.0", features = ["full"] }
tokio-postgres = { version = "0.7.11", features = ["with-chrono-0_4", "with-serde_json-1"] }
tracing = "0.1.40"
```

`fmo_core/src/lib.rs`:

```rust
pub mod db;
pub mod error;

pub use db::query;
```

`fmo_core/src/db/mod.rs`:

```rust
pub mod query;
```

- [ ] **Step 2: Move the files, fix imports**

Move `util.rs` query helpers verbatim into `fmo_core/src/db/query.rs`; move `error.rs` verbatim. In `fmo_server`, replace `mod util;`-exposed helpers with re-imports (`use fmo_core::query::{...};`) and keep `config_to_json`/`get_decoders`/`merge_metas` in a slimmed `fmo_server/src/util.rs`. Add `"fmo_core"` to workspace members.

- [ ] **Step 3: Verify**

Run: `just clippy && cargo test --workspace`
Expected: clean/PASS.

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "refactor: extract fmo_core crate with shared DB/error plumbing"
```

### Task 3: Core schema v0 + migration runner (core + per-module schemas)

**Files:**
- Create: `fmo_core/schema/core/v0.sql`, `fmo_core/src/db/migrations.rs`
- Test: `fmo_core/tests/schema.rs`

**Interfaces:**
- Produces:
  - `pub struct Migration { pub sql: &'static str }`
  - `pub async fn setup_core_schema(pool: &Pool) -> anyhow::Result<()>` — runs `fmo_core/schema/core/vN.sql` files tracked in `core_schema_version`
  - `pub async fn setup_module_schema(pool: &Pool, kind: &str, version: u32, migrations: &[Migration]) -> anyhow::Result<()>` — creates schema `fmo_<kind>` if absent, runs pending migrations tracked in `fmo_<kind>.schema_version`, and **if the stored `module_version` differs from `version`: `DROP SCHEMA fmo_<kind> CASCADE`, delete `module_progress` rows for the kind, recreate + rerun all migrations** (replay is triggered by the reset cursor)
  - `pub fn schema_name(kind: &str) -> String` — `format!("fmo_{}", kind.replace(|c: char| !c.is_ascii_alphanumeric(), "_"))`

- [ ] **Step 1: Write `fmo_core/schema/core/v0.sql`**

```sql
CREATE TABLE core_schema_version (version INTEGER PRIMARY KEY);

CREATE TABLE federations (
    federation_id BYTEA PRIMARY KEY NOT NULL,
    config        BYTEA             NOT NULL
);

-- Bronze: raw session data, append-only. Fetch cursor = MAX(session_index)+1.
CREATE TABLE sessions (
    federation_id BYTEA   NOT NULL REFERENCES federations (federation_id),
    session_index INTEGER NOT NULL,
    data          BYTEA   NOT NULL,
    PRIMARY KEY (federation_id, session_index)
);

-- Structural silver (module-agnostic, filled by ingest; amounts/details by module dispatch)
CREATE TABLE transactions (
    federation_id BYTEA   NOT NULL REFERENCES federations (federation_id),
    txid          BYTEA   NOT NULL,
    session_index INTEGER NOT NULL,
    item_index    INTEGER NOT NULL,
    data          BYTEA   NOT NULL,
    PRIMARY KEY (federation_id, txid),
    FOREIGN KEY (federation_id, session_index) REFERENCES sessions (federation_id, session_index)
);
CREATE INDEX transactions_by_session ON transactions (federation_id, session_index);

CREATE TABLE transaction_inputs (
    federation_id BYTEA   NOT NULL REFERENCES federations (federation_id),
    txid          BYTEA   NOT NULL,
    in_index      INTEGER NOT NULL,
    kind          TEXT    NOT NULL,
    amount_msat   BIGINT,          -- NULL until a module processed it
    details       JSONB,           -- module-provided JSON representation
    PRIMARY KEY (federation_id, txid, in_index),
    FOREIGN KEY (federation_id, txid) REFERENCES transactions (federation_id, txid)
);
CREATE INDEX transaction_inputs_by_kind ON transaction_inputs (federation_id, kind);
CREATE INDEX transaction_inputs_mint_nonce ON transaction_inputs ((details->'V0'->'note'->>'nonce')) WHERE kind = 'mint';

CREATE TABLE transaction_outputs (
    federation_id BYTEA   NOT NULL REFERENCES federations (federation_id),
    txid          BYTEA   NOT NULL,
    out_index     INTEGER NOT NULL,
    kind          TEXT    NOT NULL,
    amount_msat   BIGINT,
    details       JSONB,
    PRIMARY KEY (federation_id, txid, out_index),
    FOREIGN KEY (federation_id, txid) REFERENCES transactions (federation_id, txid)
);
CREATE INDEX transaction_outputs_by_kind ON transaction_outputs (federation_id, kind);

CREATE TABLE consensus_items (
    federation_id BYTEA   NOT NULL REFERENCES federations (federation_id),
    session_index INTEGER NOT NULL,
    item_index    INTEGER NOT NULL,
    peer_id       INTEGER NOT NULL,
    kind          TEXT    NOT NULL,
    details       JSONB,
    PRIMARY KEY (federation_id, session_index, item_index),
    FOREIGN KEY (federation_id, session_index) REFERENCES sessions (federation_id, session_index)
);
CREATE INDEX consensus_items_by_kind ON consensus_items (federation_id, kind);

-- Per-module processing cursor
CREATE TABLE module_progress (
    module_kind        TEXT    NOT NULL,
    federation_id      BYTEA   NOT NULL REFERENCES federations (federation_id),
    next_session_index INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (module_kind, federation_id)
);

CREATE TABLE module_versions (
    module_kind    TEXT PRIMARY KEY,
    module_version INTEGER NOT NULL
);

-- Any module may contribute session timestamp estimates (see spec §2)
CREATE TABLE session_time_votes (
    federation_id BYTEA     NOT NULL REFERENCES federations (federation_id),
    session_index INTEGER   NOT NULL,
    source_kind   TEXT      NOT NULL,
    peer_id       INTEGER   NOT NULL,
    timestamp     TIMESTAMP NOT NULL,
    PRIMARY KEY (federation_id, session_index, source_kind, peer_id)
);

-- Core services (ported unchanged in Task 8; DDL copied from old v0/v3/v4/v7.sql)
CREATE TABLE block_times (
    block_height INTEGER PRIMARY KEY,
    timestamp    TIMESTAMP NOT NULL
);
CREATE INDEX block_times_time ON block_times (timestamp);

CREATE TABLE guardian_health (
    federation_id BYTEA     NOT NULL REFERENCES federations (federation_id),
    guardian_id   INTEGER   NOT NULL,
    time          TIMESTAMP NOT NULL,
    latency_ms    INTEGER,
    session_count INTEGER,
    PRIMARY KEY (federation_id, guardian_id, time)
);
CREATE INDEX guardian_health_federation_time ON guardian_health (federation_id, time);

CREATE TABLE nostr_votes (
    event_id      BYTEA PRIMARY KEY NOT NULL,
    federation_id BYTEA             NOT NULL,
    star_vote     INTEGER CHECK (star_vote BETWEEN 1 AND 5),
    event         JSONB             NOT NULL,
    fetch_time    TIMESTAMP         NOT NULL DEFAULT NOW()
);
CREATE INDEX nostr_votes_federation ON nostr_votes (federation_id);
CREATE INDEX nostr_votes_fetch_time ON nostr_votes (fetch_time);

CREATE TABLE nostr_relays (
    relay_url TEXT PRIMARY KEY NOT NULL
);
INSERT INTO nostr_relays VALUES ('wss://relay.damus.io'), ('wss://nostr.mutinywallet.com'), ('wss://relay.snort.social'), ('wss://nos.lol');

CREATE TABLE nostr_federations (
    federation_id BYTEA PRIMARY KEY NOT NULL,
    invite_code   TEXT              NOT NULL
);

CREATE MATERIALIZED VIEW session_times AS
WITH votes AS (
    SELECT federation_id, session_index, MAX(timestamp) AS ts
    FROM session_time_votes GROUP BY federation_id, session_index
), all_sessions AS (
    SELECT s.federation_id, s.session_index, v.ts
    FROM sessions s LEFT JOIN votes v USING (federation_id, session_index)
), grouped AS (
    SELECT *, SUM(CASE WHEN ts IS NOT NULL THEN 1 ELSE 0 END)
        OVER (PARTITION BY federation_id ORDER BY session_index) AS grp
    FROM all_sessions
)
SELECT federation_id, session_index,
       FIRST_VALUE(ts) OVER (PARTITION BY federation_id, grp ORDER BY session_index)
           AS estimated_session_timestamp
FROM grouped;
CREATE UNIQUE INDEX session_times_pk ON session_times (federation_id, session_index);
CREATE INDEX session_times_ts ON session_times (federation_id, estimated_session_timestamp);

INSERT INTO core_schema_version VALUES (0);
```

Note: check old `v3.sql`/`v4.sql`/`v7.sql` for the exact `guardian_health`/`nostr_*` column lists and copy them verbatim if they differ from the above (they are ported unchanged; the columns above were reconstructed and MUST be reconciled against the old files in this step).

- [ ] **Step 2: Write failing test `fmo_core/tests/schema.rs`**

```rust
use deadpool_postgres::{Config, Runtime};
use tokio_postgres::NoTls;

pub fn test_pool() -> Option<deadpool_postgres::Pool> {
    let url = std::env::var("FMO_TEST_DATABASE").ok()?;
    let cfg = Config { url: Some(url), ..Default::default() };
    Some(cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap())
}

#[tokio::test]
async fn core_schema_applies_and_is_idempotent() {
    let Some(pool) = test_pool() else { eprintln!("skipping: FMO_TEST_DATABASE unset"); return };
    // isolate: run in a scratch database created by the test
    let conn = pool.get().await.unwrap();
    conn.batch_execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;").await.unwrap();
    fmo_core::db::migrations::setup_core_schema(&pool).await.unwrap();
    fmo_core::db::migrations::setup_core_schema(&pool).await.unwrap(); // idempotent
    let v: i32 = conn.query_one("SELECT MAX(version) FROM core_schema_version", &[]).await.unwrap().get(0);
    assert_eq!(v, 0);
}

#[tokio::test]
async fn module_schema_version_bump_drops_and_recreates() {
    let Some(pool) = test_pool() else { eprintln!("skipping: FMO_TEST_DATABASE unset"); return };
    let conn = pool.get().await.unwrap();
    conn.batch_execute("DROP SCHEMA IF EXISTS fmo_testmod CASCADE;").await.unwrap();
    fmo_core::db::migrations::setup_core_schema(&pool).await.unwrap();
    let migs = [fmo_core::db::migrations::Migration { sql: "CREATE TABLE things (id INTEGER PRIMARY KEY);" }];
    fmo_core::db::migrations::setup_module_schema(&pool, "testmod", 1, &migs).await.unwrap();
    conn.execute("INSERT INTO fmo_testmod.things VALUES (1)", &[]).await.unwrap();
    // version bump wipes the schema
    fmo_core::db::migrations::setup_module_schema(&pool, "testmod", 2, &migs).await.unwrap();
    let n: i64 = conn.query_one("SELECT COUNT(*) FROM fmo_testmod.things", &[]).await.unwrap().get(0);
    assert_eq!(n, 0);
}
```

Run: `FMO_TEST_DATABASE="postgres://user@/postgres?host=$PWD/.pg_dev&port=5432" cargo test -p fmo_core --test schema`
Expected: FAIL (module `migrations` doesn't exist). Requires `just pg_start` beforehand. Use a dedicated database (`createdb fmo_test` and point the DSN at it) so tests don't clobber dev data.

- [ ] **Step 3: Implement `fmo_core/src/db/migrations.rs`**

```rust
use deadpool_postgres::Pool;

pub struct Migration { pub sql: &'static str }

pub fn schema_name(kind: &str) -> String {
    format!("fmo_{}", kind.replace(|c: char| !c.is_ascii_alphanumeric(), "_"))
}

const CORE_MIGRATIONS: &[Migration] =
    &[Migration { sql: include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/schema/core/v0.sql")) }];

pub async fn setup_core_schema(pool: &Pool) -> anyhow::Result<()> {
    let mut conn = pool.get().await?;
    let current: i32 = conn
        .query_one(
            "SELECT COALESCE((SELECT MAX(version) FROM pg_tables t
                JOIN LATERAL (SELECT MAX(version) AS version FROM core_schema_version) v ON TRUE
                WHERE t.tablename = 'core_schema_version'), -1)",
            &[],
        )
        .await
        .map(|r| r.get(0))
        .unwrap_or(-1);
    for (idx, m) in CORE_MIGRATIONS.iter().enumerate() {
        if (idx as i32) > current {
            let tx = conn.transaction().await?;
            tx.batch_execute(m.sql).await?;
            tx.execute("INSERT INTO core_schema_version VALUES ($1) ON CONFLICT DO NOTHING", &[&(idx as i32)]).await?;
            tx.commit().await?;
        }
    }
    Ok(())
}

pub async fn setup_module_schema(pool: &Pool, kind: &str, version: u32, migrations: &[Migration]) -> anyhow::Result<()> {
    let schema = schema_name(kind);
    let mut conn = pool.get().await?;

    let stored: Option<i32> = conn
        .query_opt("SELECT module_version FROM module_versions WHERE module_kind = $1", &[&kind])
        .await?
        .map(|r| r.get(0));

    if stored.is_some_and(|v| v != version as i32) {
        let tx = conn.transaction().await?;
        tx.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE")).await?;
        tx.execute("DELETE FROM module_progress WHERE module_kind = $1", &[&kind]).await?;
        tx.execute("DELETE FROM module_versions WHERE module_kind = $1", &[&kind]).await?;
        tx.commit().await?;
    }

    let tx = conn.transaction().await?;
    tx.batch_execute(&format!("CREATE SCHEMA IF NOT EXISTS {schema}")).await?;
    tx.batch_execute(&format!(
        "CREATE TABLE IF NOT EXISTS {schema}.schema_version (version INTEGER PRIMARY KEY)"
    )).await?;
    let current: i32 = tx
        .query_one(&format!("SELECT COALESCE(MAX(version), -1) FROM {schema}.schema_version"), &[])
        .await?
        .get(0);
    tx.batch_execute(&format!("SET LOCAL search_path TO {schema}, public")).await?;
    for (idx, m) in migrations.iter().enumerate() {
        if (idx as i32) > current {
            tx.batch_execute(m.sql).await?;
            tx.execute(
                &format!("INSERT INTO {schema}.schema_version VALUES ($1) ON CONFLICT DO NOTHING"),
                &[&(idx as i32)],
            ).await?;
        }
    }
    tx.execute(
        "INSERT INTO module_versions VALUES ($1, $2)
         ON CONFLICT (module_kind) DO UPDATE SET module_version = EXCLUDED.module_version",
        &[&kind, &(version as i32)],
    ).await?;
    tx.commit().await?;
    Ok(())
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: same command as Step 2. Expected: PASS (or clean skip without env var).

- [ ] **Step 5: Reconcile ported DDL + commit**

Diff the `guardian_health`/`nostr_*`/`block_times` DDL in `v0.sql` against old `fmo_server/schema/{v0,v3,v4,v7}.sql`; fix any column drift. Then:

```bash
git add -A && git commit -m "feat(core): new-lineage core schema v0 and migration runner"
```

### Task 4: `ObserverModule` trait, registry, and core services

**Files:**
- Create: `fmo_core/src/module.rs`, `fmo_core/src/registry.rs`, `fmo_core/src/services.rs`
- Test: unit tests inline in `registry.rs`

**Interfaces:**
- Produces (used by every later task — exact definitions):

```rust
// fmo_core/src/module.rs
use std::sync::Arc;
use deadpool_postgres::Transaction;
use fedimint_core::config::{ClientConfig, FederationId};
use fedimint_core::core::{Decoder, DynInput, DynModuleConsensusItem, DynOutput, ModuleKind};
use fedimint_core::{Amount, PeerId, TransactionId};

pub struct Migration { pub sql: &'static str }  // re-export of db::migrations::Migration

#[derive(Debug, Clone)]
pub struct ItemMeta {
    pub federation_id: FederationId,
    pub txid: TransactionId,
    pub session_index: u64,
    pub item_index: u64,
    /// input or output index within the transaction
    pub index: u64,
    pub peer_count: usize,
}

#[derive(Debug, Clone)]
pub struct CiMeta {
    pub federation_id: FederationId,
    pub session_index: u64,
    pub item_index: u64,
    pub peer: PeerId,
    pub peer_count: usize,
}

#[derive(Debug, Default)]
pub struct ProcessedItem {
    pub amount: Option<Amount>,
    pub details: Option<serde_json::Value>,
}

pub struct ProcessCtx<'a> {
    /// Transaction with `search_path = fmo_<kind>, public` already set.
    pub dbtx: &'a Transaction<'a>,
    pub federation_id: FederationId,
    pub config: ClientConfig,
    pub services: Arc<crate::services::CoreServices>,
}

impl ProcessCtx<'_> {
    /// Contribute a session timestamp estimate (spec §2, session_time_votes).
    pub async fn record_session_time_vote(
        &self, kind: &ModuleKind, session_index: u64, peer: PeerId, timestamp: chrono::NaiveDateTime,
    ) -> anyhow::Result<()> { /* INSERT INTO public.session_time_votes ... ON CONFLICT DO NOTHING */ }
}

pub struct ModuleTaskCtx {
    pub federation_id: FederationId,
    pub config: ClientConfig,
    pub pool: deadpool_postgres::Pool,
    pub services: Arc<crate::services::CoreServices>,
}

#[async_trait::async_trait]
pub trait ObserverModule: Send + Sync + 'static {
    fn kind(&self) -> ModuleKind;
    fn decoder(&self) -> Decoder;
    /// Bump to force: drop module schema, reset cursor, replay.
    fn version(&self) -> u32;
    fn migrations(&self) -> &'static [crate::db::migrations::Migration];

    async fn process_input(&self, ctx: &mut ProcessCtx<'_>, input: &DynInput, meta: &ItemMeta) -> anyhow::Result<ProcessedItem>;
    async fn process_output(&self, ctx: &mut ProcessCtx<'_>, output: &DynOutput, meta: &ItemMeta) -> anyhow::Result<ProcessedItem>;
    async fn process_ci(&self, ctx: &mut ProcessCtx<'_>, ci: &DynModuleConsensusItem, meta: &CiMeta) -> anyhow::Result<Option<serde_json::Value>>;

    /// Spawned once per (module, federation); loop internally. Default: no-op.
    async fn run_federation_task(self: Arc<Self>, _ctx: ModuleTaskCtx) {}
    /// Mounted at /federations/:federation_id/modules/<kind>. Default: none.
    fn api_router(&self) -> Option<axum::Router<crate::api::ModuleApiState>> { None }
}
```

```rust
// fmo_core/src/registry.rs
pub struct ModuleRegistry { modules: BTreeMap<ModuleKind, Arc<dyn ObserverModule>> }
impl ModuleRegistry {
    pub fn new(modules: Vec<Arc<dyn ObserverModule>>) -> Self;             // panics on duplicate kind
    pub fn get(&self, kind: &ModuleKind) -> Option<&Arc<dyn ObserverModule>>;
    pub fn iter(&self) -> impl Iterator<Item = (&ModuleKind, &Arc<dyn ObserverModule>)>;
    /// Full registry for a federation config: module decoders where installed, raw fallback otherwise.
    pub fn decoders(&self, config: &ClientConfig) -> ModuleDecoderRegistry;
    /// Fallback-only registry (structural ingest).
    pub fn fallback_decoders(config: &ClientConfig) -> ModuleDecoderRegistry;
}
pub fn instance_to_kind(config: &ClientConfig, id: ModuleInstanceId) -> String;  // moved from fmo_server/src/federation/mod.rs:186-192
```

```rust
// fmo_core/src/services.rs
pub struct CoreServices { pub mempool_url: String, pool: Pool }
impl CoreServices {
    pub fn esplora(&self) -> anyhow::Result<esplora_client::AsyncClient>;
    pub async fn block_time(&self, height: u32) -> anyhow::Result<Option<chrono::NaiveDateTime>>; // SELECT FROM block_times
}
```

- [ ] **Step 1: Write failing unit tests** (in `registry.rs`): `new()` panics on duplicate kinds; `decoders()` of an empty registry equals `fallback_decoders()` behavior (both decode nothing, fall back raw). Construct a `ClientConfig` is heavyweight — instead test `instance_to_kind` with a minimal config built via `serde_json`? If constructing `ClientConfig` proves impractical in a unit test, test only the duplicate-kind panic and registry lookup, and leave decoder behavior to the Task 6 integration test. Run `cargo test -p fmo_core` → FAIL.
- [ ] **Step 2: Implement** the three files exactly as specified above (fill in the `record_session_time_vote` body: `INSERT INTO public.session_time_votes VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`). `decoders()` is the existing `decoders_from_config` (`fmo_server/src/federation/mod.rs:174-184`) generalized: for each `(instance_id, kind)` in config, use `self.get(kind).map(|m| m.decoder())`, collect into `ModuleDecoderRegistry`, `.with_fallback()`.
- [ ] **Step 3: Run tests** → PASS. `just clippy` clean.
- [ ] **Step 4: Commit** — `git commit -m "feat(core): ObserverModule trait, module registry, core services"`

---

## Phase 2 — Pipeline

### Task 5: Structural ingest (shared by fetcher and importer)

**Files:**
- Create: `fmo_core/src/ingest.rs`
- Test: `fmo_core/tests/pipeline.rs` (started here, extended in Task 6)

**Interfaces:**
- Produces: `pub async fn ingest_session(dbtx: &Transaction<'_>, config: &ClientConfig, federation_id: FederationId, session_index: u64, session: &SessionOutcome) -> anyhow::Result<()>` — inserts into `sessions`, `transactions`, `transaction_inputs/outputs` (kind + NULL amount/details), `consensus_items` (kind + NULL details). All `ON CONFLICT DO NOTHING`.
- Consumes: `registry::instance_to_kind`.

- [ ] **Step 1: Write failing test.** Build a synthetic `SessionOutcome` using `fedimint-dummy-common` (add as dev-dependency: `fedimint-dummy-common = { version = "0.10.0", git = "https://github.com/fedimint/fedimint", tag = "v0.10.0" }`): one `ConsensusItem::Transaction` with a dummy input+output, one `ConsensusItem::Module` dummy CI. Build a minimal `ClientConfig` with one dummy module instance (instance id 0, kind "dummy"). Ingest into scratch DB; assert one row each in `transactions`, `transaction_inputs`, `transaction_outputs`, `consensus_items` with `kind = 'dummy'`, `amount_msat IS NULL`. If `ClientConfig`/`SessionOutcome` construction hits non-pub fields, fall back to consensus-decoding fixture bytes generated inline via `Encodable` on the fedimint types (both types are `Decodable`; encode-then-decode is the construction path). Test is `FMO_TEST_DATABASE`-gated like Task 3.
- [ ] **Step 2: Run** `cargo test -p fmo_core --test pipeline` → FAIL (ingest missing).
- [ ] **Step 3: Implement `ingest.rs`.** Port the structural INSERT statements from `observer.rs:615-623` (sessions), `671-681` (transactions), `728-739` (inputs — drop `ln_contract_id`, amount → NULL), `918-930` (outputs — drop ln columns, amount → NULL), and a plain `consensus_items` INSERT with NULL details (kind via `instance_to_kind`, skipping non-module CIs as in `observer.rs:651-653`). No module decode, no downcasts.
- [ ] **Step 4: Run test** → PASS. **Step 5: Commit** `feat(core): structural session ingest`.

### Task 6: Dispatch/replay engine with per-module cursors

**Files:**
- Create: `fmo_core/src/dispatch.rs`
- Test: extend `fmo_core/tests/pipeline.rs`

**Interfaces:**
- Produces:
  - `pub async fn process_pending(pool: &Pool, registry: &ModuleRegistry, services: &Arc<CoreServices>, federation_id: FederationId, config: &ClientConfig, batch_limit: u32) -> anyhow::Result<u64>` — processes up to `batch_limit` sessions for every installed module that is behind; returns number of (module, session) units processed. **This single function is both live processing and replay.**
  - `pub async fn run_processor(pool: Pool, registry: Arc<ModuleRegistry>, services: Arc<CoreServices>, federation_id: FederationId, config: ClientConfig)` — loop: `process_pending(...)`; sleep 1s when it returns 0.

- [ ] **Step 1: Write failing test.** Define `TestModule` in the test implementing `ObserverModule` for kind "dummy" (version 1, one migration creating `seen (session_index INTEGER, what TEXT)`), whose `process_input` inserts `('input')` and returns `ProcessedItem { amount: Some(Amount::from_msats(42)), details: Some(json!({"t":"i"})) }`, analogous for output/CI. Test: ingest 3 synthetic sessions (Task 5 helper), run `process_pending`, assert: `fmo_dummy.seen` rows exist; `transaction_inputs.amount_msat = 42` and `details` filled; `module_progress.next_session_index = 3`; second `process_pending` returns 0 (idempotent/caught-up). Then register a **second** module kind after the fact, run again, assert it catches up from 0 (replay).
- [ ] **Step 2: Run** → FAIL. 
- [ ] **Step 3: Implement `dispatch.rs`:**

```rust
pub async fn process_pending(/* as above */) -> anyhow::Result<u64> {
    let conn = pool.get().await?;
    let fed_id_bytes = federation_id.consensus_encode_to_vec();
    let fetched: Option<i32> = query_value(&conn, "SELECT MAX(session_index) FROM sessions WHERE federation_id = $1", &[&fed_id_bytes]).await?;
    let Some(fetched) = fetched else { return Ok(0) };

    // cursor per installed module
    let mut cursors: BTreeMap<ModuleKind, i32> = ...; // SELECT from module_progress, default 0
    let min_next = cursors.values().copied().min().unwrap_or(0);
    if min_next > fetched { return Ok(0); }

    let decoders = registry.decoders(config);
    let mut processed = 0u64;
    let rows = conn.query(
        "SELECT session_index, data FROM sessions WHERE federation_id = $1 AND session_index >= $2 ORDER BY session_index LIMIT $3",
        &[&fed_id_bytes, &min_next, &(batch_limit as i64)],
    ).await?;
    for row in rows {
        let session_index: i32 = row.get(0);
        let session = SessionOutcome::consensus_decode_whole(&row.get::<_, Vec<u8>>(1), &decoders)?;
        for (kind, module) in registry.iter() {
            if cursors.get(kind).copied().unwrap_or(0) != session_index { continue; }
            let mut mconn = pool.get().await?;
            let dbtx = mconn.transaction().await?;
            dbtx.batch_execute(&format!("SET LOCAL search_path TO {}, public", schema_name(kind.as_str()))).await?;
            dispatch_session_to_module(&dbtx, module, services, federation_id, config, session_index as u64, &session).await?;
            dbtx.execute(
                "INSERT INTO public.module_progress VALUES ($1, $2, $3)
                 ON CONFLICT (module_kind, federation_id) DO UPDATE SET next_session_index = EXCLUDED.next_session_index",
                &[&kind.as_str(), &fed_id_bytes, &(session_index + 1)],
            ).await?;
            dbtx.commit().await?;
            *cursors.entry(kind.clone()).or_default() = session_index + 1;
            processed += 1;
        }
    }
    Ok(processed)
}
```

`dispatch_session_to_module` iterates the session's items: for `ConsensusItem::Transaction`, for each input/output whose `instance_to_kind` matches the module's kind, call `process_input`/`process_output` and `UPDATE public.transaction_inputs SET amount_msat = $x, details = $y WHERE ...` (resp. outputs) from the returned `ProcessedItem`; for `ConsensusItem::Module` of matching kind, call `process_ci` and update `public.consensus_items.details`. Module errors: log `warn!` with kind+session and **return Err** — the failed module's tx rolls back, its cursor stays, other modules continue (the per-module loop isolates via separate transactions; wrap the per-module block so an `Err` is logged and skipped rather than aborting the whole batch — that module simply stalls at that session and retries next `process_pending` round).
- [ ] **Step 4: Run test** → PASS (both replay-catch-up and idempotency asserts). **Step 5: Commit** `feat(core): per-module dispatch and replay engine`.

### Task 7: Fetcher + FederationObserver core

**Files:**
- Create: `fmo_core/src/fetch.rs`, `fmo_core/src/observer.rs` (new core version), `fmo_core/src/builder.rs`
- Test: compile-level + Task 6 tests keep passing (network fetch is not unit-tested; it is exercised in Task 19 against a live federation)

**Interfaces:**
- Produces:
  - `fetch.rs`: `pub async fn run_fetcher(pool: Pool, federation_id: FederationId, config: ClientConfig)` — port of `observe_federation_history` (`observer.rs:536-605`) with three changes: (a) next session = `MAX(session_index)+1` (not `COUNT`), (b) decoding uses `ModuleRegistry::fallback_decoders(config)` only, (c) per session it opens a tx and calls `ingest_session` — no module processing. Keep the 0.10 API bootstrap from #115 (`ConnectorRegistry::build_from_client_env()`, `DynGlobalApi::new`).
  - `observer.rs`: `pub struct FederationObserver { pool, registry: Arc<ModuleRegistry>, services: Arc<CoreServices>, admin_auth, task_group }` with `new(...)`, `spawn_federation(&self, fed)` (spawns fetcher + processor + per-module `run_federation_task` + health monitor), `add_federation` (port `observer.rs:423-452`), `list_federations`, `get_federation`, `check_auth` (ports).
  - `builder.rs`:

```rust
pub struct FedimintObserverBuilder { modules: Vec<Arc<dyn ObserverModule>> }
impl FedimintObserverBuilder {
    pub fn new() -> Self;
    pub fn with_module(mut self, m: impl ObserverModule) -> Self;
    /// Applies core + module schemas, spawns observers/services, serves axum on opts.bind.
    pub async fn run(self, opts: crate::ServerOpts) -> anyhow::Result<()>;
}
pub struct ServerOpts { pub bind: SocketAddr, pub database: String, pub admin_auth: String, pub mempool_url: String }
```

- [ ] **Step 1:** Implement `fetch.rs` per the port notes above.
- [ ] **Step 2:** Implement core `observer.rs`: startup = `setup_core_schema` → for each registered module `setup_module_schema(pool, kind, module.version(), module.migrations())` → `spawn_federation` for each stored federation → spawn core service tasks (wired in Task 8). `spawn_federation` spawns `run_fetcher` and `run_processor` as separate `task_group.spawn_cancellable` tasks with the same restart-on-error loop pattern as `observer.rs:100-113`.
- [ ] **Step 3:** Implement `builder.rs::run`: build `ModuleRegistry`, `FederationObserver::new`, build router (placeholder `Router::new()` until Task 14), `axum::serve`.
- [ ] **Step 4:** `just clippy && cargo test --workspace` → clean/PASS. **Step 5: Commit** `feat(core): fetcher, core observer, builder`.

### Task 8: Move core services (block times, guardian health, nostr, meta, config API)

**Files:**
- Move: `fmo_server/src/federation/guardians.rs` → `fmo_core/src/services/guardians.rs`; `fmo_server/src/federation/nostr.rs` → `fmo_core/src/services/nostr.rs`; `fmo_server/src/meta.rs` + `fmo_server/src/config/meta.rs` cache → `fmo_core/src/services/meta.rs`; block-times code (`observer.rs:460-534`) → `fmo_core/src/services/block_times.rs`; `fmo_server/src/config/` (config API + CORS) → `fmo_core/src/api/config.rs`; `config_to_json`/`get_decoders`/`merge_metas` from `fmo_server/src/util.rs` → `fmo_core` (`get_decoders` is replaced by registry decoders; `config_to_json` takes a `&ModuleRegistry` argument now)
- Modify: `fmo_core/src/observer.rs` (spawn these tasks), `fmo_core/Cargo.toml` (+ `nostr-sdk`, `reqwest`, `hex`, `regex`, `csv` as needed by moved code)

**Interfaces:**
- Produces: `FederationObserver` methods used by API handlers later: `get_guardian_health_summary`, `federation_rating`, `submit_rating`, `get_federation_health` route fns, `sync_nostr_events`, `fetch_block_times`, consensus meta cache. Signatures unchanged from their current namesakes.

- [ ] **Step 1:** Move files wholesale; adjust `crate::` paths; table names are unchanged (they live in core schema `public`). These are mechanical moves — no behavior changes. The block-times seed (`observer.rs:213-229`, `schema/block_times.sql`) moves to `fmo_core/schema/block_times.sql` + a seed call in core observer startup.
- [ ] **Step 2:** `just clippy && cargo test --workspace` → clean. (fmo_server will be mostly hollow now; keep it compiling by re-exporting from fmo_core.)
- [ ] **Step 3: Commit** `refactor(core): move block-times/guardian/nostr/meta/config services into fmo_core`.

---

## Phase 3 — Module crates (ports)

Each module crate follows the same template — shown once here in full for mint, then only deltas.

### Task 9: `fmo_module_mint`

**Files:**
- Create: `fmo_modules/fmo_module_mint/Cargo.toml`, `src/lib.rs`, `schema/v0.sql` (empty besides a comment — mint needs no own tables; its data lives in core `details` JSONB), 
- Modify: workspace members
- Test: `fmo_modules/fmo_module_mint/tests/process.rs`

**Interfaces:**
- Produces: `pub struct MintObserver;` implementing `ObserverModule` (kind `mint`, version 1).

- [ ] **Step 1:** Crate setup:

```toml
[package]
name = "fmo_module_mint"
version = "0.1.0"
edition = "2021"

[dependencies]
anyhow = "1.0.81"
async-trait = "0.1"
axum = { version = "0.7.5", features = ["json"] }
fedimint-core = { workspace = true }
fedimint-mint-common = { workspace = true }
fmo_core = { path = "../../fmo_core" }
serde_json = "1.0.115"
tracing = "0.1.40"
```

- [ ] **Step 2 (failing test):** feed a `MintInput` (constructed via encode/decode of `fedimint_mint_common` types) through `process_input`; assert `amount == Some(...)` and `details` JSON contains the nonce. DB-free: `ProcessCtx` requires a dbtx — add a test helper in fmo_core (`fmo_core::test_util::with_ctx`, behind `#[cfg(feature = "test-util")]`) that opens a scratch-DB transaction; gate the test on `FMO_TEST_DATABASE`.
- [ ] **Step 3 (implement):** Port from `observer.rs`: amount extraction `696-707` (input) / `892-902` (output); JSON details `776-787` (input) / `979-990` (output) / CI `1067-1078`. Downcast failure or non-v0 variant: **no panic** — `warn!` and return `ProcessedItem { amount: None, details: None }` (this is the "graceful unknown-version" behavior; raw variants serialize into details via serde where possible).
- [ ] **Step 4:** api_router: `GET /nonces/spend` (POST body as today) — port handler `get_nonces_spend_info` (`observer.rs:1434-1493`); the query targets `public.transaction_inputs.details` now (`details->'V0'->'note'->>'nonce'`, index exists from Task 3). Mounted path: `/federations/:federation_id/modules/mint/nonces/spend`.
- [ ] **Step 5:** test PASS, clippy clean, commit `feat(module/mint): mint observer module`.

### Task 10: `fmo_module_wallet`

**Files:**
- Create: `fmo_modules/fmo_module_wallet/{Cargo.toml,src/lib.rs,schema/v0.sql}`; deps add `bitcoin`, `esplora-client`, `fedimint-wallet-common`, `chrono`, `futures`
- Test: `tests/process.rs`

**Interfaces:**
- Produces: `pub struct WalletObserver;` (kind `wallet`, version 1).

- [ ] **Step 1: `schema/v0.sql`** — port from old `v2.sql` + `v0.sql` block-vote table, unqualified names (land in `fmo_wallet`), FKs to `public.*`:

```sql
CREATE TABLE peg_ins (
    on_chain_txid  BYTEA   NOT NULL,
    on_chain_vout  INTEGER NOT NULL,
    address        TEXT    NOT NULL,
    amount_msat    BIGINT  NOT NULL,
    federation_id  BYTEA   NOT NULL REFERENCES public.federations (federation_id),
    txid           BYTEA   NOT NULL,
    in_index       INTEGER NOT NULL,
    PRIMARY KEY (federation_id, txid, in_index)
);
CREATE INDEX peg_ins_federation ON peg_ins (federation_id);
CREATE TABLE withdrawal_addresses (
    address       TEXT    NOT NULL,
    federation_id BYTEA   NOT NULL REFERENCES public.federations (federation_id),
    session_index INTEGER NOT NULL,
    item_index    INTEGER NOT NULL,
    txid          BYTEA   NOT NULL,
    out_index     INTEGER NOT NULL,
    PRIMARY KEY (federation_id, txid, out_index)
);
CREATE INDEX withdrawal_addresses_addr ON withdrawal_addresses (address);
CREATE TABLE withdrawal_transactions (
    on_chain_txid   BYTEA PRIMARY KEY,
    federation_id   BYTEA NOT NULL REFERENCES public.federations (federation_id),
    federation_txid BYTEA
);
CREATE TABLE withdrawal_signatures (
    on_chain_txid BYTEA   NOT NULL,
    session_index INTEGER NOT NULL,
    item_index    INTEGER NOT NULL,
    peer_id       INTEGER NOT NULL,
    PRIMARY KEY (on_chain_txid, session_index, item_index, peer_id)
);
CREATE TABLE withdrawal_transaction_inputs (
    prev_out_txid BYTEA   NOT NULL,
    prev_out_vout INTEGER NOT NULL,
    on_chain_txid BYTEA   NOT NULL REFERENCES withdrawal_transactions (on_chain_txid),
    PRIMARY KEY (prev_out_txid, prev_out_vout)
);
CREATE TABLE withdrawal_transaction_outputs (
    on_chain_txid BYTEA   NOT NULL REFERENCES withdrawal_transactions (on_chain_txid),
    out_index     INTEGER NOT NULL,
    address       TEXT    NOT NULL,
    amount_msat   BIGINT  NOT NULL,
    PRIMARY KEY (on_chain_txid, out_index)
);
CREATE TABLE block_height_votes (
    federation_id BYTEA   NOT NULL REFERENCES public.federations (federation_id),
    session_index INTEGER NOT NULL,
    item_index    INTEGER NOT NULL,
    proposer      INTEGER NOT NULL,
    height_vote   INTEGER NOT NULL,
    PRIMARY KEY (federation_id, session_index, item_index)
);
CREATE MATERIALIZED VIEW utxos AS
    SELECT p.federation_id, p.on_chain_txid, p.on_chain_vout, p.address, p.amount_msat
    FROM peg_ins p
    LEFT JOIN withdrawal_transaction_inputs wti
        ON p.on_chain_txid = wti.prev_out_txid AND p.on_chain_vout = wti.prev_out_vout
    WHERE wti.on_chain_txid IS NULL
    UNION ALL
    SELECT wt.federation_id, wto.on_chain_txid, wto.out_index, wto.address, wto.amount_msat
    FROM withdrawal_transaction_outputs wto
    JOIN withdrawal_transactions wt ON wto.on_chain_txid = wt.on_chain_txid
    LEFT JOIN withdrawal_transaction_inputs wti
        ON wto.on_chain_txid = wti.prev_out_txid AND wto.out_index = wti.prev_out_vout
    WHERE wt.federation_txid IS NOT NULL AND wti.on_chain_txid IS NULL;
CREATE UNIQUE INDEX utxos_pk ON utxos (federation_id, on_chain_txid, on_chain_vout);
```

Reconcile the `utxos` view against old `fmo_server/schema/v2.sql`'s definition (copy its logic verbatim, adjusting table names) — the old file is authoritative, the SQL above is the target shape.

- [ ] **Step 2 (ports into `process_input`/`process_output`/`process_ci`):**
  - input amount: `observer.rs:708-724`; peg-in insert: `741-773`; details JSON: `788-799`.
  - output amount: `903-916`; withdrawal address insert + **RBF handling**: `932-976` — replace the `panic!` for `WalletOutputV0::Rbf` with `error!` log + return `Err(...)` (stalls only the wallet module, preserving the "needs manual attention" property without killing the process); details JSON: `991-1002`.
  - CI: JSON `1079-1090`; `BlockCount` vote insert `1146-1158` **plus** `ctx.record_session_time_vote(...)` with `services.block_time(height)` timestamp when known (this feeds `session_times`); `PegOutSignature` chain `1159-1295` (esplora client from `ctx.services.esplora()`, threshold from `meta.peer_count` — formula at `observer.rs:1202-1205`).
  - Unknown variants (`WalletInput::Default`, unsupported versions): `warn!` + best-effort serde `details`, never panic (replaces panics at `observer.rs:717,753`).
- [ ] **Step 3:** api_router: `GET /utxos` → port `federation_utxos` (`observer.rs:1360-1389`) reading `fmo_wallet.utxos`.
- [ ] **Step 4 (failing→passing test):** peg-out signature threshold path is network-bound — test only vote + peg-in paths: feed a `WalletConsensusItem::BlockCount` CI, assert `block_height_votes` row + `session_time_votes` row appear.
- [ ] **Step 5:** clippy, tests, commit `feat(module/wallet): wallet observer module`.

### Task 11: `fmo_module_ln`

**Files:** `fmo_modules/fmo_module_ln/{Cargo.toml,src/lib.rs,schema/v0.sql}` (deps + `fedimint-ln-common`); test `tests/process.rs`

- [ ] **Step 1: schema** — `contracts` table from old `v0.sql:30-40` (unqualified, `type TEXT CHECK (type IN ('incoming','outgoing'))`, indexes included).
- [ ] **Step 2: ports** — input amount+contract id: `observer.rs:685-694` (also `UPDATE`… no: contract-id column lives in module scope now: add `input_contracts (federation_id, txid, in_index, contract_id)` table to schema, insert there); output contract handling `848-891` (contract insert → `contracts`, plus `output_contracts (federation_id, txid, out_index, interaction_kind CHECK IN ('fund','cancel','offer'), contract_id)`); details JSON input `800-811`, output `1003-1014`, CI `1091-1102`. Non-v0: warn + serde details, no panic.
- [ ] **Step 3: test** — incoming-contract output → `contracts` + `output_contracts` rows; **Step 4:** clippy/test/commit `feat(module/ln): ln observer module`.

### Task 12: `fmo_module_lnv2` (the v2 deliverable)

**Files:** `fmo_modules/fmo_module_lnv2/{Cargo.toml,src/lib.rs,schema/v0.sql}`; dep `fedimint-lnv2-common = { version = "0.10.0", git = "https://github.com/fedimint/fedimint", tag = "v0.10.0" }`; test `tests/process.rs`

**Interfaces:** `pub struct LnV2Observer;` (kind `lnv2`, version 1). Upstream types (verified against v0.10.0 source): `LightningInputV0::{Outgoing(OutPoint, OutgoingWitness), Incoming(OutPoint, AggregateDecryptionKey)}`, `LightningOutputV0::{Outgoing(OutgoingContract), Incoming(IncomingContract)}`, `LightningConsensusItem::{BlockCountVote(u64), UnixTimeVote(u64), Default{..}}`, `ContractId`, `contracts::{IncomingContract, OutgoingContract}` with `.contract_id()`.

- [ ] **Step 1: schema/v0.sql**

```sql
CREATE TABLE contracts (
    federation_id BYTEA   NOT NULL REFERENCES public.federations (federation_id),
    contract_id   BYTEA   NOT NULL,
    type          TEXT    NOT NULL CHECK (type IN ('incoming', 'outgoing')),
    amount_msat   BIGINT  NOT NULL,
    txid          BYTEA   NOT NULL,
    out_index     INTEGER NOT NULL,
    PRIMARY KEY (federation_id, contract_id)
);
CREATE INDEX contracts_federation ON contracts (federation_id);
CREATE TABLE input_outpoints (
    federation_id BYTEA   NOT NULL REFERENCES public.federations (federation_id),
    txid          BYTEA   NOT NULL,
    in_index      INTEGER NOT NULL,
    type          TEXT    NOT NULL CHECK (type IN ('incoming', 'outgoing')),
    outpoint_txid BYTEA   NOT NULL,
    outpoint_out_index INTEGER NOT NULL,
    PRIMARY KEY (federation_id, txid, in_index)
);
```

- [ ] **Step 2 (failing test):** encode/decode-construct a `LightningOutputV0::Incoming(IncomingContract{...})` (or decode fixture bytes), run `process_output`, assert a `contracts` row with `type='incoming'` and JSON details present. CI test: `UnixTimeVote(t)` → `session_time_votes` row with that timestamp; `BlockCountVote` → details JSON only.
- [ ] **Step 3 (implement):**
  - `process_output`: downcast `fedimint_lnv2_common::LightningOutput`, `maybe_v0_ref()`; on `Outgoing(c)`/`Incoming(c)`: insert contract (amount from the contract struct — `OutgoingContract.amount` / `IncomingContract.commitment.amount`; verify exact field names against the crate when implementing, both carry an `Amount`), return `ProcessedItem { amount: Some(that amount), details: serde_json }`. Non-v0 → warn + raw details.
  - `process_input`: `Outgoing(outpoint, _)`/`Incoming(outpoint, _)` → insert `input_outpoints`, amount `None` (input amounts aren't in the lnv2 input; the referenced contract's amount is already recorded), details serde JSON.
  - `process_ci`: `UnixTimeVote(t)` → `ctx.record_session_time_vote(kind, session, peer, DateTime::from_timestamp(t as i64,0))`; both vote variants → `Some(details JSON)`.
- [ ] **Step 4:** clippy/test/commit `feat(module/lnv2): lnv2 observer module — first v2 module`.

### Task 13: `fmo_module_stability_pool` (conditional)

- [ ] **Step 1 (gate):** check whether `stability-pool-common` (git `https://github.com/tacio/fedi`, branch `fmo-compatible`) builds against fedimint 0.10: create the crate, `cargo check -p fmo_module_stability_pool`. **If it fails to resolve/build: delete the crate, remove the old `stability_pool_v1` feature-gated code with the rest of fmo_server's old code (Task 15), note in the final report that the stability_pool module is blocked on tacio updating the crate to 0.10, and skip the rest of this task.**
- [ ] **Step 2 (if it builds):** JSON-details-only module (kind `stability_pool`, version 1, no own tables): port the three cfg-gated arms (`observer.rs:812-824`, `1015-1027`, `1103-1115`) into `process_*` returning serde JSON details, amounts `None`.
- [ ] **Step 3:** add `fmo_modules/fmo_module_stability_pool/examples/custom_fmo.rs` — the 10-line custom-binary demo from the issue:

```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    fmo_core::FedimintObserverBuilder::new()
        .with_module(fmo_module_mint::MintObserver)
        .with_module(fmo_module_wallet::WalletObserver)
        .with_module(fmo_module_ln::LnObserver)
        .with_module(fmo_module_stability_pool::StabilityPoolObserver)
        .run(fmo_core::ServerOpts::from_env()?)
        .await
}
```

- [ ] **Step 4:** clippy/commit `feat(module/stability-pool): out-of-default-binary module + custom build example` (or the removal commit per Step 1).

---

## Phase 4 — API, binary, import

### Task 14: Core API + module mounting + compat shims

**Files:**
- Create: `fmo_core/src/api/mod.rs`, `fmo_core/src/api/federations.rs`
- Move: `fmo_server/src/federation/session.rs` → `fmo_core/src/api/sessions.rs`; `fmo_server/src/federation/transaction.rs` → `fmo_core/src/api/transactions.rs`; route table from `fmo_server/src/federation/mod.rs:31-63`; summary/activity/totals/assets/overview handlers from `observer.rs:301-421,1331-1358,1391-1423`
- Modify: `fmo_core/src/builder.rs` (assemble real router)

**Interfaces:**
- Produces: `pub fn build_router(state: AppState) -> Router` where `AppState { observer: FederationObserver }` (moved from `fmo_server/src/main.rs`). `pub struct ModuleApiState { pub pool: Pool, pub services: Arc<CoreServices>, pub observer: FederationObserver }`.
- Route map (all responses byte-compatible with today):
  - Core: `/federations` GET/PUT, `/federations/totals`, `/federations/:id`, `/:id/config`, `/:id/meta`, `/:id/health`, `/:id/transactions{,/:txid,/count,/histogram}`, `/:id/sessions{,/count}`, `/:id/backfill` (now: reset **all** module cursors for the federation to `session_start.unwrap_or(0)` — replay does the rest; body params kept), `/federations/nostr/rating` PUT, `/config/*` (from Task 8).
  - Modules: for each registered module with `api_router()`, nest at `/federations/:federation_id/modules/{kind}`.
  - Compat shims (defined here in core for simplicity, since they forward to module routers): `/federations/:id/utxos` → wallet `GET /utxos` handler; `/federations/:id/nonces/spend` → mint handler. Implement by calling the module handler functions re-exported from the module crates? **No — core cannot depend on module crates.** Instead: `ObserverModule::api_router()` returns the router; compat shims are added in `fmo_server` (which depends on all module crates) via `Router::merge` — see Task 15.

- [ ] **Step 1:** Port handlers/queries. Queries referencing dropped columns change as follows: `federation_activity` (`observer.rs:382-394`) and `get_federation_assets` (`observer.rs:1342-1355`) work unchanged (columns kept); `transaction.rs` queries use `transaction_inputs/outputs` unchanged. `session_times` matview name/columns unchanged.
- [ ] **Step 2:** Adapt `refresh_views` (`observer.rs:1304-1329`): refresh `session_times` + every matview registered by modules — add `fn matviews(&self) -> &'static [&'static str] { &[] }` to `ObserverModule` (wallet returns `&["fmo_wallet.utxos"]`), qualify names, configurable interval via `FO_REFRESH_INTERVAL_SECS` env (default 60; honors the intent of PR #117).
- [ ] **Step 3:** `just clippy` + existing tests → clean. **Step 4: Commit** `feat(core): API router, module mounting, matview refresh`.

### Task 15: Thin `fmo_server` binary + compat shims + old-code deletion

**Files:**
- Rewrite: `fmo_server/src/main.rs`
- Delete: `fmo_server/src/federation/` (whole dir), `fmo_server/src/{config,meta.rs,util.rs,db.rs,error.rs}` leftovers, `fmo_server/schema/` (old v0–v8 — **keep** the directory contents in git history only; the import tool reads the *old DB*, not these files), old feature flag in `fmo_server/Cargo.toml`
- Modify: `fmo_server/Cargo.toml` (deps: fmo_core + the four default module crates + clap/tokio/dotenv/tracing only), `justfile` (`test` recipe: `cargo test --workspace`; drop frontend targets if #115 left any)

**Interfaces:**
- Produces:

```rust
// fmo_server/src/main.rs
#[derive(clap::Parser)]
enum Cmd {
    Serve(fmo_core::ServerOptsCli),                 // FO_BIND, FO_DATABASE, FO_ADMIN_AUTH, FO_MEMPOOL_URL — names unchanged
    Import { #[arg(long)] from: String },           // old-DB connection string; uses FO_DATABASE as target
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt().with_env_filter(...).init();
    match Cmd::parse() {
        Cmd::Serve(opts) => builder().run(opts.into()).await,
        Cmd::Import { from } => fmo_core::import::import(&from, &std::env::var("FO_DATABASE")?, &builder_registry()).await,
    }
}

fn builder() -> fmo_core::FedimintObserverBuilder {
    fmo_core::FedimintObserverBuilder::new()
        .with_module(fmo_module_mint::MintObserver)
        .with_module(fmo_module_wallet::WalletObserver)
        .with_module(fmo_module_ln::LnObserver)
        .with_module(fmo_module_lnv2::LnV2Observer)
        .with_compat_route("/federations/:federation_id/utxos", "wallet", "/utxos")
        .with_compat_route("/federations/:federation_id/nonces/spend", "mint", "/nonces/spend")
}
```

`with_compat_route(public_path, kind, module_path)`: builder stores triples; at router build time it adds an axum route at `public_path` whose handler rewrites the URI to `/federations/:id/modules/<kind><module_path>` and re-dispatches into the router (`tower::Service::call` on the nested router, or simply mount the module router a second time under the shim prefix — second mount is simpler and is the chosen mechanism: `.nest(public_prefix, module_router)` where the shim path prefix maps 1:1).

- [ ] **Step 1:** Implement main + builder wiring (import stub returns `bail!("not yet implemented")` until Task 16 — acceptable for one task since Task 16 lands next and the stub is explicit).
- [ ] **Step 2:** Delete old fmo_server code; fix justfile test recipe.
- [ ] **Step 3:** Manual smoke test against dev PG:

```bash
just pg_start
FO_BIND=127.0.0.1:3000 FO_DATABASE="postgres://user@/postgres?host=$PWD/.pg_dev&port=5432" \
  FO_ADMIN_AUTH=test FO_MEMPOOL_URL=https://mempool.space/api cargo run -p fmo_server -- serve &
curl -s localhost:3000/federations   # expect: []
```

- [ ] **Step 4:** `just clippy && cargo test --workspace` clean. **Step 5: Commit** `feat: thin fmo_server binary with builder + compat shims; drop legacy code`.

### Task 16: Import tool

**Files:**
- Create: `fmo_core/src/import.rs`
- Test: `fmo_core/tests/import.rs`

**Interfaces:**
- Produces: `pub async fn import(old_db: &str, new_db: &str, registry: &ModuleRegistry) -> anyhow::Result<()>`.

- [ ] **Step 1 (failing test):** create two scratch schemas in the test DB: `old` gets the *old* v8 DDL for just `federations`+`sessions`+`block_times` (inline the minimal DDL in the test, copied from old `schema/v0.sql:1-15,80-85`) seeded with one federation config + 2 synthetic session blobs (same fixtures as Task 5); run `import`; assert new-schema `sessions` count = 2, `transactions` populated (structural ingest ran), `module_progress` empty (modules replay on next serve), `block_times` copied.
- [ ] **Step 2 (implement):**

```rust
pub async fn import(old_db: &str, new_db: &str, registry: &ModuleRegistry) -> anyhow::Result<()> {
    let old = connect(old_db).await?;        // plain tokio_postgres
    let pool = make_pool(new_db)?;
    db::migrations::setup_core_schema(&pool).await?;

    // 1. federations
    for row in old.query("SELECT federation_id, config FROM federations", &[]).await? { /* INSERT ... ON CONFLICT DO NOTHING */ }

    // 2. block_times (bulk, saves esplora refetch)
    //    binary COPY OUT/IN via tokio_postgres copy_out/copy_in for speed

    // 3. sessions: stream per federation, decode with fallback registry, ingest_session per row
    for fed in federations {
        let config = ClientConfig::consensus_decode_whole(&fed.config, &decoders)?;
        let decoders = ModuleRegistry::fallback_decoders(&config);
        // portal: SELECT session_index, session FROM sessions WHERE federation_id=$1 ORDER BY session_index
        // batched via `query_raw` stream; per 100 sessions one tx: SessionOutcome::consensus_decode_whole + ingest_session
        // progress log every 1000 sessions (pattern from observer.rs:262-269)
    }

    // 4. verify: per federation, old COUNT(sessions) == new COUNT(sessions); log summary table
    Ok(())
}
```

Old `sessions.session` column name is `session` (old v0.sql:12); new is `data` — map explicitly. **This is the encoding round-trip proof**: 0.8-era blobs decoded under 0.10 fallback decoders; any decode error aborts with the federation + session index in the message.
- [ ] **Step 3:** wire `Cmd::Import` in fmo_server (replace Task 15 stub). Test PASS, clippy, commit `feat: v8-database import tool`.

---

## Phase 5 — Ports of in-flight work + verification

### Task 17: Port Ayus's gateway monitoring (#109) into `fmo_module_ln`

**Files:**
- Modify: `fmo_modules/fmo_module_ln/src/lib.rs`, `schema/v0.sql` (add `gateways` table — schema v0 is still unreleased, no migration needed), `fmo_server/src/main.rs` (compat route `/federations/:federation_id/gateways` → ln `/gateways`)

- [ ] **Step 1:** `gh pr diff 109 > /tmp/claude-.../109.diff` and port: the `gateways` DDL from its `schema/v9.sql` (unqualified into ln schema), the poller from its monitor task into `LnObserver::run_federation_task` (loop with 5-min sleep; `FO_GATEWAY_POLL_SECS` override), the `GatewayInfo` type into `fmo_api_types` (take verbatim), the handler into `api_router()` `GET /gateways`.
- [ ] **Step 2:** clippy + tests; commit `feat(module/ln): gateway monitoring (ported from #109, by bansalayush247)`.

### Task 18: End-to-end + replay-idempotency verification

**Files:**
- Create: `fmo_core/tests/e2e.rs`
- Test: this IS the test task

- [ ] **Step 1 (e2e, DB-gated):** full pipeline against scratch DB with the dummy TestModule from Task 6: ingest 5 sessions → `process_pending` → assert cursors, amounts, `session_times` matview refresh works (`REFRESH MATERIALIZED VIEW session_times`), then run `process_pending` again and assert **zero row changes** (replay idempotency: compare `SELECT COUNT(*)` on every core table before/after).
- [ ] **Step 2 (live smoke, manual):** run the Task 15 smoke test, `PUT /federations` with a real invite code (take the first invite from `curl -s https://observer.fedimint.org/api/federations`), watch logs for fetch+process progress, then `curl localhost:3000/federations/:id/transactions?limit=5` and `/federations/:id/utxos`. Record results in the final report. This exercises the 0.10 network path that unit tests can't.
- [ ] **Step 3:** commit `test: end-to-end pipeline and replay idempotency`.

### Task 19: Docs + final check

**Files:**
- Modify: `README.md`, `CLAUDE.md` (architecture section: crate list, module interface, per-module schemas, cursors, import command), `sample.env` (add `FO_REFRESH_INTERVAL_SECS`, `FO_GATEWAY_POLL_SECS` as comments)

- [ ] **Step 1:** Update docs: builder example (from Task 13/15), "adding your own module" section (trait surface, schema conventions, replay semantics), import runbook (spec §3 deployment steps).
- [ ] **Step 2:** Run: `just final-check` → all green (requires `FMO_TEST_DATABASE` exported for DB tests; without it they skip).
- [ ] **Step 3:** commit `docs: modularized architecture documentation`.

---

## Self-Review Notes

- Spec coverage: builder API (T7/T13/T15), per-module schemas + version-bump replay (T3), cursors/replay (T6), session_time_votes (T3/T10/T12), fetch/process split (T5–T7), compat API (T14/T15), import (T16), lnv2 (T12), gateway port (T17), stability_pool contingency (T13), tests incl. idempotency + encoding round-trip (T16/T18).
- Known deliberate deviations from spec: `/federations/:id/backfill` becomes cursor-reset (replay) instead of in-place reprocess — same observable effect, less code. Old `transaction_input_details` tables collapse into `details JSONB` columns on the structural tables (one table fewer, same JSON, nonce index preserved).
- Deferred (explicitly out of scope, matches spec): #113 guardian versions, #116 UTXO comparison, walletv2/mintv2.
- Line references into `observer.rs` etc. refer to the **pre-merge master state** (readable via `git show master:fmo_server/src/federation/observer.rs`); after Task 1 the merged tree may shift lines slightly — the anchor is master, which task 1 does not change.
