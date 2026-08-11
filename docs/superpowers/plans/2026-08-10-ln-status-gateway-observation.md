# LN Per-Payment Status + Gateway Observation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give every Lightning contract (LNv1 + LNv2) an authoritative, module-owned per-payment status, and extend gateway observation to LNv2 with a shared poller that also pings each gateway's API for real reachability.

**Architecture:** Each LN module maintains a `status` column on its `contracts` table, recomputed per-contract from that one contract's own legs by a `recompute_contract_status` helper called after each leg is recorded (O(1) per leg, derive-from-legs → replay-stable; no matview/global recompute). The gold tier drops its status entirely. The generic gateway poll loop (registry fetch → upsert → snapshot → prune) is extracted from `fmo_module_ln` into a shared `fmo_core` harness parameterized by a per-module `GatewaySource`, gains a bounded per-gateway HTTP ping (reachable + latency), and `fmo_module_lnv2` implements a source on it.

**Tech Stack:** Rust, async-trait, deadpool-postgres / tokio-postgres, PostgreSQL, fedimint 0.11.1 module types, reqwest (gateway ping), Nix dev shell + `just`.

## Global Constraints

- Per-payment status is authoritative **in the module tables** (`fmo_ln.contracts.status`, `fmo_lnv2.contracts.status`); **gold carries no status**.
- Status is maintained by **incremental per-contract updates** (`recompute_contract_status`, deriving from that contract's legs) — never a matview or global recompute; the derivation is a pure function of the current legs, so it is replay-stable regardless of processing order.
- Unknown/undecodable module data must never panic a module or stall a federation (existing invariant: downcast/`maybe_v0_ref` failures return early, storing JSON only).
- Gateway pinging must be bounded by a per-gateway timeout so one dead gateway cannot stall the poll loop.
- The extracted harness must preserve LNv1's existing registry-poll + `is_seen` snapshot behavior (its serving/`GatewayInfo` output stays working).
- Module schema change ⇒ **edit the module's `schema/v0.sql` in place and bump `version()`** (a version mismatch drops the module schema, resets cursors, and replays from raw sessions — no refetch). Core schema changes are **append-only** migration files under `fmo_core/schema/core/`.
- Module `process_*` hooks run on the processing transaction with `search_path = fmo_<kind>, public`; unqualified table names are module-owned. Hooks must NOT take a second pool connection.
- Repo-wide pre-commit hook (`typos` + `cargo fmt --all`) must stay green; commit **without** `--no-verify`.
- Work stays on the `modularization` branch. No PR. Nothing pushed.
- Tests are DB-gated: they no-op unless `FMO_TEST_DATABASE` is set. Dev DSN: `postgres://user@/fmo_test?host=$PWD/.pg_dev&port=5432` after `just pg_start`. Run a single module's tests with `just test_package fmo_module_ln` (and `fmo_module_lnv2`); lint with `just clippy`.

---

## Task 1: LNv1 per-contract status (`fmo_ln`)

**Files:**
- Modify: `fmo_modules/fmo_module_ln/schema/v0.sql` (add `status` column to `contracts`)
- Create: `fmo_modules/fmo_module_ln/src/status.rs` (the recompute helper)
- Modify: `fmo_modules/fmo_module_ln/src/lib.rs` (declare `mod status`; bump `version()`; call the helper in `process_output`/`process_input`/`process_ci`)
- Modify: `fmo_modules/fmo_module_ln/tests/process.rs` (status tests)

**Interfaces:**
- Produces: `pub async fn fmo_module_ln::status::recompute_contract_status(dbtx: &tokio_postgres::Transaction<'_>, federation_id: &[u8], contract_id: &[u8], threshold: i64) -> anyhow::Result<()>` — `federation_id`/`contract_id` are `consensus_encode_to_vec()` bytes; `threshold` is the decryption threshold `n - (n-1)/3`.
- Status domain: `pending | decrypted | succeeded | refunded`.

- [ ] **Step 1: Add the status column.** In `fmo_modules/fmo_module_ln/schema/v0.sql`, change the `contracts` table definition to add a `status` column (keep everything else):

```sql
CREATE TABLE contracts
(
    federation_id BYTEA NOT NULL REFERENCES public.federations (federation_id),
    contract_id   BYTEA NOT NULL,
    type          TEXT  NOT NULL CHECK (type IN ('incoming', 'outgoing')),
    payment_hash  BYTEA NOT NULL,
    status        TEXT  NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'decrypted', 'succeeded', 'refunded')),
    PRIMARY KEY (federation_id, contract_id)
);
```

- [ ] **Step 2: Write the failing test.** Append to `fmo_modules/fmo_module_ln/tests/process.rs`. This seeds contract legs directly via SQL (no fedimint fixtures) and asserts the derived status. Add these imports at the top of the file if missing: `use fmo_module_ln::status::recompute_contract_status;`.

```rust
/// Seeds the core FK chain (session + tx + one input row + one output row) so
/// module leg rows can reference them. `n` disambiguates txids across calls.
async fn seed_fk_chain(pool: &deadpool_postgres::Pool, fed: &[u8], n: u8) -> Vec<u8> {
    let txid = vec![n; 32];
    let conn = pool.get().await.unwrap();
    conn.execute("INSERT INTO sessions VALUES ($1, $2, ''::bytea) ON CONFLICT DO NOTHING",
        &[&fed, &(n as i32)]).await.unwrap();
    conn.execute("INSERT INTO transactions VALUES ($1, $2, $3, 0, ''::bytea) ON CONFLICT DO NOTHING",
        &[&fed, &txid, &(n as i32)]).await.unwrap();
    conn.execute("INSERT INTO transaction_inputs (federation_id, txid, in_index, kind) VALUES ($1,$2,0,'ln') ON CONFLICT DO NOTHING",
        &[&fed, &txid]).await.unwrap();
    conn.execute("INSERT INTO transaction_outputs (federation_id, txid, out_index, kind) VALUES ($1,$2,0,'ln') ON CONFLICT DO NOTHING",
        &[&fed, &txid]).await.unwrap();
    txid
}

async fn insert_contract(pool: &deadpool_postgres::Pool, fed: &[u8], cid: &[u8], typ: &str) {
    pool.get().await.unwrap().execute(
        "INSERT INTO fmo_ln.contracts (federation_id, contract_id, type, payment_hash) VALUES ($1,$2,$3,$2)",
        &[&fed, &cid, &typ]).await.unwrap();
}
async fn insert_output_contract(pool: &deadpool_postgres::Pool, fed: &[u8], txid: &[u8], cid: &[u8], kind: &str) {
    pool.get().await.unwrap().execute(
        "INSERT INTO fmo_ln.output_contracts VALUES ($1,$2,0,$3,$4)",
        &[&fed, &txid, &kind, &cid]).await.unwrap();
}
async fn insert_input_contract(pool: &deadpool_postgres::Pool, fed: &[u8], txid: &[u8], cid: &[u8]) {
    pool.get().await.unwrap().execute(
        "INSERT INTO fmo_ln.input_contracts VALUES ($1,$2,0,$3)",
        &[&fed, &txid, &cid]).await.unwrap();
}
async fn insert_share(pool: &deadpool_postgres::Pool, fed: &[u8], cid: &[u8], peer: i32) {
    pool.get().await.unwrap().execute(
        "INSERT INTO fmo_ln.decryption_shares VALUES ($1,$2,$3,0,$3)",
        &[&fed, &cid, &peer]).await.unwrap();
}
async fn status_of(pool: &deadpool_postgres::Pool, fed: &[u8], cid: &[u8]) -> String {
    pool.get().await.unwrap().query_one(
        "SELECT status FROM fmo_ln.contracts WHERE federation_id=$1 AND contract_id=$2",
        &[&fed, &cid]).await.unwrap().get(0)
}

async fn recompute(pool: &deadpool_postgres::Pool, fed: &[u8], cid: &[u8], threshold: i64) {
    let mut conn = pool.get().await.unwrap();
    let dbtx = conn.transaction().await.unwrap();
    recompute_contract_status(&dbtx, fed, cid, threshold).await.unwrap();
    dbtx.commit().await.unwrap();
}

#[tokio::test]
async fn ln_status_transitions() {
    let _guard = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else { eprintln!("skipping: FMO_TEST_DATABASE unset"); return; };
    reset_ln(&pool).await;
    let (config, federation_id) = minimal_config();
    insert_federation(&pool, &config, federation_id).await;
    let module = LnObserver;
    fmo_core::db::migrations::setup_module_schema(&pool, "ln", module.version(), module.migrations()).await.unwrap();
    let fed = federation_id.consensus_encode_to_vec();
    let threshold = 1; // 1 guardian -> threshold 1 - 0/3 = 1

    // outgoing: funded only -> pending
    let c1 = vec![1u8; 32]; let t1 = seed_fk_chain(&pool, &fed, 1).await;
    insert_contract(&pool, &fed, &c1, "outgoing").await;
    insert_output_contract(&pool, &fed, &t1, &c1, "fund").await;
    recompute(&pool, &fed, &c1, threshold).await;
    assert_eq!(status_of(&pool, &fed, &c1).await, "pending");

    // outgoing: funded + claimed (no cancel) -> succeeded
    insert_input_contract(&pool, &fed, &t1, &c1).await;
    recompute(&pool, &fed, &c1, threshold).await;
    assert_eq!(status_of(&pool, &fed, &c1).await, "succeeded");

    // outgoing: funded + cancel + refund input -> refunded (cancel wins over spend)
    let c2 = vec![2u8; 32]; let t2 = seed_fk_chain(&pool, &fed, 2).await;
    insert_contract(&pool, &fed, &c2, "outgoing").await;
    insert_output_contract(&pool, &fed, &t2, &c2, "fund").await;
    insert_output_contract(&pool, &fed, &t2, &c2, "cancel").await;
    insert_input_contract(&pool, &fed, &t2, &c2).await;
    recompute(&pool, &fed, &c2, threshold).await;
    assert_eq!(status_of(&pool, &fed, &c2).await, "refunded");

    // incoming: funded only -> pending; + decrypt -> decrypted; + claim -> succeeded
    let c3 = vec![3u8; 32]; let t3 = seed_fk_chain(&pool, &fed, 3).await;
    insert_contract(&pool, &fed, &c3, "incoming").await;
    insert_output_contract(&pool, &fed, &t3, &c3, "fund").await;
    recompute(&pool, &fed, &c3, threshold).await;
    assert_eq!(status_of(&pool, &fed, &c3).await, "pending");
    insert_share(&pool, &fed, &c3, 0).await;
    recompute(&pool, &fed, &c3, threshold).await;
    assert_eq!(status_of(&pool, &fed, &c3).await, "decrypted");
    insert_input_contract(&pool, &fed, &t3, &c3).await;
    recompute(&pool, &fed, &c3, threshold).await;
    assert_eq!(status_of(&pool, &fed, &c3).await, "succeeded");

    // incoming: funded + claim but NEVER decrypted -> refunded (reclaim of expired offer)
    let c4 = vec![4u8; 32]; let t4 = seed_fk_chain(&pool, &fed, 4).await;
    insert_contract(&pool, &fed, &c4, "incoming").await;
    insert_output_contract(&pool, &fed, &t4, &c4, "fund").await;
    insert_input_contract(&pool, &fed, &t4, &c4).await;
    recompute(&pool, &fed, &c4, threshold).await;
    assert_eq!(status_of(&pool, &fed, &c4).await, "refunded");
}
```

- [ ] **Step 3: Run the test to verify it fails.**

Run: `just test_package fmo_module_ln`
Expected: FAIL to compile — `fmo_module_ln::status` does not exist / `recompute_contract_status` unresolved.

- [ ] **Step 4: Implement the helper.** Create `fmo_modules/fmo_module_ln/src/status.rs`:

```rust
//! Per-contract terminal status, derived from the contract's own legs
//! (funding/cancel outputs, claim/refund inputs, preimage-decryption shares).
//! Called after each leg is recorded, so the status advances incrementally
//! without a global recompute; because it is a pure function of the current
//! legs it is idempotent and replay-stable regardless of processing order.

use tokio_postgres::Transaction;

/// Recompute one contract's `status`. `threshold` is the preimage-decryption
/// threshold `n - (n-1)/3`; an incoming contract counts as decrypted once it
/// has shares from at least `threshold` distinct guardians.
pub async fn recompute_contract_status(
    dbtx: &Transaction<'_>,
    federation_id: &[u8],
    contract_id: &[u8],
    threshold: i64,
) -> anyhow::Result<()> {
    dbtx.execute(
        "UPDATE contracts c SET status = CASE
             WHEN c.type = 'outgoing'
                  AND EXISTS (SELECT 1 FROM output_contracts oc
                              WHERE oc.federation_id = c.federation_id
                                AND oc.contract_id = c.contract_id
                                AND oc.interaction_kind = 'cancel')
                  THEN 'refunded'
             WHEN c.type = 'outgoing'
                  AND EXISTS (SELECT 1 FROM input_contracts ic
                              WHERE ic.federation_id = c.federation_id
                                AND ic.contract_id = c.contract_id)
                  THEN 'succeeded'
             WHEN c.type = 'incoming'
                  AND EXISTS (SELECT 1 FROM input_contracts ic
                              WHERE ic.federation_id = c.federation_id
                                AND ic.contract_id = c.contract_id)
                  AND (SELECT COUNT(DISTINCT ds.peer_id) FROM decryption_shares ds
                       WHERE ds.federation_id = c.federation_id
                         AND ds.contract_id = c.contract_id) >= $3
                  THEN 'succeeded'
             WHEN c.type = 'incoming'
                  AND EXISTS (SELECT 1 FROM input_contracts ic
                              WHERE ic.federation_id = c.federation_id
                                AND ic.contract_id = c.contract_id)
                  THEN 'refunded'
             WHEN c.type = 'incoming'
                  AND (SELECT COUNT(DISTINCT ds.peer_id) FROM decryption_shares ds
                       WHERE ds.federation_id = c.federation_id
                         AND ds.contract_id = c.contract_id) >= $3
                  THEN 'decrypted'
             ELSE 'pending'
         END
         WHERE c.federation_id = $1 AND c.contract_id = $2",
        &[&federation_id, &contract_id, &threshold],
    )
    .await?;
    Ok(())
}
```

- [ ] **Step 5: Wire it in and bump the version.** In `fmo_modules/fmo_module_ln/src/lib.rs`:
  - Add `pub mod status;` near the top (next to `mod gateways;`).
  - Change `fn version(&self) -> u32 { 2 }` to `{ 3 }`.
  - Add a small helper to compute the threshold from config, and call `recompute_contract_status` after each leg insert. Insert this private fn in the `impl LnObserver` area (or as a free fn in the module):

```rust
fn decryption_threshold(config: &fedimint_core::config::ClientConfig) -> i64 {
    let n = config.global.api_endpoints.len() as i64;
    n - (n - 1) / 3
}
```

  - In `process_output`, after the `INSERT INTO output_contracts ...` `.await?;` (both the `fund` and `cancel` paths reach it), call the helper for the touched `contract_id`:

```rust
        let threshold = decryption_threshold(&ctx.config);
        crate::status::recompute_contract_status(
            ctx.dbtx,
            &meta.federation_id.consensus_encode_to_vec(),
            &contract_id.consensus_encode_to_vec(),
            threshold,
        )
        .await?;
```

  (`contract_id` is already in scope from the `match`. The `Offer` branch produced `offer.hash.into()` as `contract_id` and interaction_kind `offer`; recomputing it is harmless — no `contracts` row exists yet for an unfunded offer, so the `UPDATE` affects 0 rows.)

  - In `process_input`, after the `INSERT INTO input_contracts ...` `.await?;`:

```rust
        let threshold = decryption_threshold(&ctx.config);
        crate::status::recompute_contract_status(
            ctx.dbtx,
            &meta.federation_id.consensus_encode_to_vec(),
            &input_v0.contract_id.consensus_encode_to_vec(),
            threshold,
        )
        .await?;
```

  - In `process_ci`, inside the `if let LightningConsensusItem::DecryptPreimage(contract_id, _share)` block, after the `INSERT INTO decryption_shares ...` `.await?;`:

```rust
            let threshold = decryption_threshold(&ctx.config);
            crate::status::recompute_contract_status(
                ctx.dbtx,
                &meta.federation_id.consensus_encode_to_vec(),
                &contract_id.consensus_encode_to_vec(),
                threshold,
            )
            .await?;
```

- [ ] **Step 6: Run the tests to verify they pass.**

Run: `just test_package fmo_module_ln`
Expected: PASS — `ln_status_transitions` green, plus the two pre-existing tests still green.

- [ ] **Step 7: Lint.**

Run: `just clippy`
Expected: no warnings in `fmo_module_ln`.

- [ ] **Step 8: Commit.**

```bash
git add fmo_modules/fmo_module_ln/schema/v0.sql fmo_modules/fmo_module_ln/src/status.rs \
        fmo_modules/fmo_module_ln/src/lib.rs fmo_modules/fmo_module_ln/tests/process.rs
git commit -m "feat(ln): incremental per-contract payment status"
```

---

## Task 2: LNv2 per-contract status (`fmo_lnv2`)

**Files:**
- Modify: `fmo_modules/fmo_module_lnv2/schema/v0.sql` (add `status` to `contracts`, `variant` to `input_outpoints`)
- Create: `fmo_modules/fmo_module_lnv2/src/status.rs`
- Modify: `fmo_modules/fmo_module_lnv2/src/lib.rs` (bump `version()`; record input variant; call helper)
- Modify: `fmo_modules/fmo_module_lnv2/tests/process.rs` (status tests)

**Interfaces:**
- Consumes: nothing from Task 1 (independent module).
- Produces: `pub async fn fmo_module_lnv2::status::recompute_contract_status(dbtx, federation_id: &[u8], outpoint_txid: &[u8]) -> anyhow::Result<()>` — keyed by the funding **outpoint** txid (the input references the outpoint, not the contract_id).
- Status domain: `pending | succeeded | refunded`. Input `variant` domain: `claim | refund`.

- [ ] **Step 1: Schema.** In `fmo_modules/fmo_module_lnv2/schema/v0.sql`, add `status` to `contracts` and `variant` to `input_outpoints`:

```sql
CREATE TABLE contracts
(
    federation_id BYTEA   NOT NULL REFERENCES public.federations (federation_id),
    contract_id   BYTEA   NOT NULL,
    type          TEXT    NOT NULL CHECK (type IN ('incoming', 'outgoing')),
    amount_msat   BIGINT  NOT NULL,
    txid          BYTEA   NOT NULL,
    out_index     INTEGER NOT NULL,
    status        TEXT    NOT NULL DEFAULT 'pending'
                          CHECK (status IN ('pending', 'succeeded', 'refunded')),
    PRIMARY KEY (federation_id, contract_id)
);
```

and add the `variant` column to `input_outpoints` (keep the existing columns/FK):

```sql
CREATE TABLE input_outpoints
(
    federation_id      BYTEA   NOT NULL REFERENCES public.federations (federation_id),
    txid               BYTEA   NOT NULL,
    in_index           INTEGER NOT NULL,
    type               TEXT    NOT NULL CHECK (type IN ('incoming', 'outgoing')),
    variant            TEXT    NOT NULL CHECK (variant IN ('claim', 'refund')),
    outpoint_txid      BYTEA   NOT NULL,
    outpoint_out_index INTEGER NOT NULL,
    PRIMARY KEY (federation_id, txid, in_index),
    FOREIGN KEY (federation_id, txid, in_index)
        REFERENCES public.transaction_inputs (federation_id, txid, in_index)
);
```

- [ ] **Step 2: Write the failing test.** Append to `fmo_modules/fmo_module_lnv2/tests/process.rs` (mirror its existing helpers — read the file for its `reset_lnv2`/`DB_LOCK`/fixture pattern and reuse them). Import `use fmo_module_lnv2::status::recompute_contract_status;`.

```rust
#[tokio::test]
async fn lnv2_status_transitions() {
    let _guard = DB_LOCK.lock().await;
    let Some(pool) = test_pool() else { eprintln!("skipping: FMO_TEST_DATABASE unset"); return; };
    reset_lnv2(&pool).await;
    let (config, federation_id) = minimal_config();
    insert_federation(&pool, &config, federation_id).await;
    let module = LnV2Observer;
    fmo_core::db::migrations::setup_module_schema(&pool, "lnv2", module.version(), module.migrations()).await.unwrap();
    let fed = federation_id.consensus_encode_to_vec();

    // helpers
    async fn seed_fk(pool: &deadpool_postgres::Pool, fed: &[u8], n: u8) -> Vec<u8> {
        let txid = vec![n; 32];
        let conn = pool.get().await.unwrap();
        conn.execute("INSERT INTO sessions VALUES ($1,$2,''::bytea) ON CONFLICT DO NOTHING", &[&fed, &(n as i32)]).await.unwrap();
        conn.execute("INSERT INTO transactions VALUES ($1,$2,$3,0,''::bytea) ON CONFLICT DO NOTHING", &[&fed,&txid,&(n as i32)]).await.unwrap();
        conn.execute("INSERT INTO transaction_inputs (federation_id,txid,in_index,kind) VALUES ($1,$2,0,'lnv2') ON CONFLICT DO NOTHING", &[&fed,&txid]).await.unwrap();
        conn.execute("INSERT INTO transaction_outputs (federation_id,txid,out_index,kind) VALUES ($1,$2,0,'lnv2') ON CONFLICT DO NOTHING", &[&fed,&txid]).await.unwrap();
        txid
    }
    async fn insert_contract(pool: &deadpool_postgres::Pool, fed: &[u8], cid: &[u8], typ: &str, txid: &[u8]) {
        pool.get().await.unwrap().execute(
            "INSERT INTO fmo_lnv2.contracts (federation_id,contract_id,type,amount_msat,txid,out_index) VALUES ($1,$2,$3,1000,$4,0)",
            &[&fed,&cid,&typ,&txid]).await.unwrap();
    }
    async fn insert_input(pool: &deadpool_postgres::Pool, fed: &[u8], txid: &[u8], typ: &str, variant: &str, outpoint_txid: &[u8]) {
        pool.get().await.unwrap().execute(
            "INSERT INTO fmo_lnv2.input_outpoints VALUES ($1,$2,0,$3,$4,$5,0)",
            &[&fed,&txid,&typ,&variant,&outpoint_txid]).await.unwrap();
    }
    async fn status_of(pool: &deadpool_postgres::Pool, fed: &[u8], cid: &[u8]) -> String {
        pool.get().await.unwrap().query_one("SELECT status FROM fmo_lnv2.contracts WHERE federation_id=$1 AND contract_id=$2", &[&fed,&cid]).await.unwrap().get(0)
    }
    async fn recompute(pool: &deadpool_postgres::Pool, fed: &[u8], outpoint_txid: &[u8]) {
        let mut conn = pool.get().await.unwrap();
        let dbtx = conn.transaction().await.unwrap();
        recompute_contract_status(&dbtx, fed, outpoint_txid).await.unwrap();
        dbtx.commit().await.unwrap();
    }

    // funded only -> pending
    let cf = vec![1u8;32]; let tf = seed_fk(&pool,&fed,1).await;
    insert_contract(&pool,&fed,&cf,"outgoing",&tf).await;
    recompute(&pool,&fed,&tf).await;
    assert_eq!(status_of(&pool,&fed,&cf).await, "pending");
    // claim -> succeeded
    let tclaim = seed_fk(&pool,&fed,2).await;
    insert_input(&pool,&fed,&tclaim,"outgoing","claim",&tf).await;
    recompute(&pool,&fed,&tf).await;
    assert_eq!(status_of(&pool,&fed,&cf).await, "succeeded");

    // second contract, refund -> refunded
    let cr = vec![3u8;32]; let tr = seed_fk(&pool,&fed,3).await;
    insert_contract(&pool,&fed,&cr,"outgoing",&tr).await;
    let trefund = seed_fk(&pool,&fed,4).await;
    insert_input(&pool,&fed,&trefund,"outgoing","refund",&tr).await;
    recompute(&pool,&fed,&tr).await;
    assert_eq!(status_of(&pool,&fed,&cr).await, "refunded");
}
```

- [ ] **Step 3: Run the test to verify it fails.**

Run: `just test_package fmo_module_lnv2`
Expected: FAIL to compile — `fmo_module_lnv2::status` unresolved.

- [ ] **Step 4: Implement the helper.** Create `fmo_modules/fmo_module_lnv2/src/status.rs`:

```rust
//! Per-contract terminal status for LNv2, derived from the spending input's
//! variant (claim/refund). Keyed by the funding outpoint, which is what an
//! input references. Pure function of the legs → idempotent and replay-stable.

use tokio_postgres::Transaction;

pub async fn recompute_contract_status(
    dbtx: &Transaction<'_>,
    federation_id: &[u8],
    outpoint_txid: &[u8],
) -> anyhow::Result<()> {
    dbtx.execute(
        "UPDATE contracts c SET status = CASE
             WHEN EXISTS (SELECT 1 FROM input_outpoints io
                          WHERE io.federation_id = c.federation_id
                            AND io.outpoint_txid = c.txid
                            AND io.outpoint_out_index = c.out_index
                            AND io.variant = 'claim')  THEN 'succeeded'
             WHEN EXISTS (SELECT 1 FROM input_outpoints io
                          WHERE io.federation_id = c.federation_id
                            AND io.outpoint_txid = c.txid
                            AND io.outpoint_out_index = c.out_index
                            AND io.variant = 'refund') THEN 'refunded'
             ELSE 'pending'
         END
         WHERE c.federation_id = $1 AND c.txid = $2",
        &[&federation_id, &outpoint_txid],
    )
    .await?;
    Ok(())
}
```

- [ ] **Step 5: Record the input variant, wire the helper, bump version.** In `fmo_modules/fmo_module_lnv2/src/lib.rs`:
  - Add `pub mod status;` at the top.
  - Change `fn version(&self) -> u32 { 2 }` to `{ 3 }`.
  - In `process_input`, replace the `let (contract_type, outpoint) = match input_v0 { ... }` block so it also derives the `variant` from the witness (an outgoing `Refund`/`Cancel` is a refund; an outgoing `Claim` and any incoming input are claims), and include `variant` in the `input_outpoints` INSERT:

```rust
        use fedimint_lnv2_common::OutgoingWitness;
        let (contract_type, variant, outpoint) = match input_v0 {
            LightningInputV0::Outgoing(outpoint, witness) => {
                let variant = match witness {
                    OutgoingWitness::Claim(_) => "claim",
                    OutgoingWitness::Refund | OutgoingWitness::Cancel(_) => "refund",
                };
                ("outgoing", variant, outpoint)
            }
            LightningInputV0::Incoming(outpoint, _agg_decryption_key) => {
                ("incoming", "claim", outpoint)
            }
        };

        ctx.dbtx
            .execute(
                "INSERT INTO input_outpoints VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT DO NOTHING",
                &[
                    &meta.federation_id.consensus_encode_to_vec(),
                    &meta.txid.consensus_encode_to_vec(),
                    &(meta.index as i32),
                    &contract_type,
                    &variant,
                    &outpoint.txid.consensus_encode_to_vec(),
                    &(outpoint.out_idx as i32),
                ],
            )
            .await?;
```

  - Still in `process_input`, after that INSERT (and it can be after the existing amount-resolution query), call the status helper for the funding outpoint:

```rust
        crate::status::recompute_contract_status(
            ctx.dbtx,
            &meta.federation_id.consensus_encode_to_vec(),
            &outpoint.txid.consensus_encode_to_vec(),
        )
        .await?;
```

  - `process_output` needs no status call — a freshly funded contract defaults to `pending` via the column default.

- [ ] **Step 6: Run the tests to verify they pass.**

Run: `just test_package fmo_module_lnv2`
Expected: PASS — `lnv2_status_transitions` green; pre-existing lnv2 tests still green.

- [ ] **Step 7: Lint & commit.**

Run: `just clippy`
Expected: clean.

```bash
git add fmo_modules/fmo_module_lnv2/schema/v0.sql fmo_modules/fmo_module_lnv2/src/status.rs \
        fmo_modules/fmo_module_lnv2/src/lib.rs fmo_modules/fmo_module_lnv2/tests/process.rs
git commit -m "feat(lnv2): incremental per-contract payment status"
```

---

## Task 3: Remove status from the gold tier

**Files:**
- Create: `fmo_core/schema/core/v2.sql` (drop `user_transactions.status`, redefine `user_tx_daily` without status)
- Modify: `fmo_core/src/db/migrations.rs` (append v2 to `CORE_MIGRATIONS`)
- Modify: `fmo_core/src/gold.rs` (remove `status` from `fold_ln_v1`/`fold_lnv2`/`fold_standalone` inserts + `user_transactions` column list)
- Modify: `fmo_core/tests/gold.rs` (drop status assertions)

**Interfaces:**
- Consumes: nothing (independent; the module status from Tasks 1–2 is the replacement, read by consumers via `user_transaction_txs → contract_id`).
- Produces: `user_transactions` with no `status` column; `user_tx_daily` grouped by `(federation_id, day, kind, direction)`.

- [ ] **Step 1: Inspect the current gold DDL** so the migration matches it. Read `fmo_core/schema/core/v1.sql` for the exact `user_transactions` column list and the `user_tx_daily` definition (it is `CREATE MATERIALIZED VIEW user_tx_daily ... GROUP BY federation_id, day, kind, direction, status`).

- [ ] **Step 2: Write the migration.** Create `fmo_core/schema/core/v2.sql`:

```sql
-- Per-payment status moved into the LN modules (fmo_ln/fmo_lnv2.contracts.status),
-- which own the lifecycle. Gold no longer carries or derives it.
DROP MATERIALIZED VIEW IF EXISTS user_tx_daily;

ALTER TABLE user_transactions DROP COLUMN IF EXISTS status;

CREATE MATERIALIZED VIEW user_tx_daily AS
SELECT
    federation_id,
    date(first_timestamp)                  AS day,
    kind,
    direction,
    count(*)                               AS tx_count,
    coalesce(sum(amount_msat), 0)          AS volume_msat,
    coalesce(sum(fedimint_fee_msat), 0)    AS fedimint_fee_msat,
    coalesce(sum(gateway_fee_estimate_msat), 0) AS gateway_fee_estimate_msat
FROM user_transactions
WHERE first_timestamp IS NOT NULL
GROUP BY federation_id, date(first_timestamp), kind, direction;

CREATE UNIQUE INDEX user_tx_daily_pk
    ON user_tx_daily (federation_id, day, kind, direction);
```

Note: adjust the column list to match the existing `user_tx_daily` in `v1.sql` exactly, minus `status`. The `UNIQUE INDEX` is required — the refresh path uses `REFRESH MATERIALIZED VIEW CONCURRENTLY`, which needs a unique index.

- [ ] **Step 3: Register the migration.** In `fmo_core/src/db/migrations.rs`, append to `CORE_MIGRATIONS` (after the v1 entry):

```rust
    Migration {
        sql: include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/schema/core/v2.sql")),
    },
```

- [ ] **Step 4: Strip status from the gold folds.** In `fmo_core/src/gold.rs`:
  - In each `INSERT INTO user_transactions (...)` column list, remove the trailing `status` column name.
  - In each corresponding `SELECT`, remove the status value expression (the `'completed'` literal in `fold_standalone`; the `CASE WHEN spends."any" THEN 'completed' ... END` in `fold_ln_v1` and `fold_lnv2`).
  - In each `ON CONFLICT ... DO UPDATE SET ...`, remove `status=EXCLUDED.status`.
  - Leave the `user_transaction_txs` role logic (`CASE WHEN cancel ... THEN 'refund' ELSE 'claim'`) untouched — that is the fedimint-tx role, not the payment status.

- [ ] **Step 5: Update gold tests.** In `fmo_core/tests/gold.rs`, remove assertions that read `user_transactions.status` or `user_tx_daily.status` (search for `status`). Keep all other assertions.

- [ ] **Step 6: Run the gold tests.**

Run: `just test_package fmo_core`
Expected: PASS — gold tests green without status.

- [ ] **Step 7: Lint & commit.**

Run: `just clippy`
Expected: clean.

```bash
git add fmo_core/schema/core/v2.sql fmo_core/src/db/migrations.rs fmo_core/src/gold.rs fmo_core/tests/gold.rs
git commit -m "refactor(gold): drop status; per-payment status now owned by LN modules"
```

---

## Task 4: Extract the shared gateway harness + gateway-API ping (LNv1)

**Files:**
- Create: `fmo_core/src/gateway_poll.rs` (generic harness: loop, snapshot, prune, ping)
- Modify: `fmo_core/src/lib.rs` (expose `pub mod gateway_poll;`)
- Modify: `fmo_modules/fmo_module_ln/schema/v0.sql` (add `reachable`, `latency_ms` to `gateway_poll_snapshots`)
- Modify: `fmo_modules/fmo_module_ln/src/gateways.rs` (implement the source on the harness; keep serving)
- Modify: `fmo_modules/fmo_module_ln/src/lib.rs` (bump `version()` to 4)
- Create: `fmo_core/tests/gateway_poll.rs` (ping + snapshot tests)

**Interfaces:**
- Produces (consumed by Task 5):
  - A trait `pub trait GatewaySource: Send + Sync` in `fmo_core::gateway_poll` with:
    - `fn schema(&self) -> &'static str;` (e.g. `"fmo_ln"`)
    - `async fn fetch_and_upsert(&self, dbtx: &Transaction<'_>, ctx: &ModuleTaskCtx, api: &DynGlobalApi, now: DateTime<Utc>) -> anyhow::Result<Vec<PolledGateway>>;` — fetches the registry, upserts the module's `gateways` table, and returns the currently-registered gateways (id + api endpoint) for snapshotting + pinging.
  - `pub struct PolledGateway { pub gateway_id: String, pub api_endpoint: Option<String> }`
  - `pub async fn run_gateway_poller(ctx: ModuleTaskCtx, source: impl GatewaySource + 'static) -> anyhow::Result<()>` — the shared loop.
  - `pub async fn ping_gateway(api_endpoint: &str, timeout: Duration) -> (bool, Option<i32>)` — returns `(reachable, latency_ms)`.

- [ ] **Step 1: Snapshot schema — add reachability columns.** In `fmo_modules/fmo_module_ln/schema/v0.sql`, extend `gateway_poll_snapshots`:

```sql
CREATE TABLE gateway_poll_snapshots
(
    federation_id BYTEA       NOT NULL REFERENCES public.federations (federation_id),
    gateway_id    TEXT        NOT NULL,
    poll_time     TIMESTAMPTZ NOT NULL,
    is_seen       BOOLEAN     NOT NULL,
    reachable     BOOLEAN     NOT NULL DEFAULT FALSE,
    latency_ms    INTEGER,
    PRIMARY KEY (federation_id, gateway_id, poll_time)
);
```

- [ ] **Step 2: Write the failing tests.** Create `fmo_core/tests/gateway_poll.rs`:

```rust
use std::time::Duration;
use fmo_core::gateway_poll::ping_gateway;

#[tokio::test]
async fn ping_unreachable_host_returns_false() {
    // Reserved-for-documentation IP that black-holes: connect times out fast.
    let (reachable, latency) = ping_gateway("http://192.0.2.1:9", Duration::from_millis(300)).await;
    assert!(!reachable);
    assert!(latency.is_none());
}

#[tokio::test]
async fn ping_reachable_host_returns_true_with_latency() {
    // Spin up a throwaway local HTTP listener.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            if let Ok((mut sock, _)) = listener.accept().await {
                use tokio::io::AsyncWriteExt;
                let _ = sock.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n").await;
            }
        }
    });
    let (reachable, latency) = ping_gateway(&format!("http://{addr}"), Duration::from_secs(2)).await;
    assert!(reachable);
    assert!(latency.is_some());
}
```

- [ ] **Step 3: Run to verify it fails.**

Run: `just test_package fmo_core`
Expected: FAIL to compile — `fmo_core::gateway_poll` does not exist.

- [ ] **Step 4: Implement the harness.** Create `fmo_core/src/gateway_poll.rs` with the generic loop, the ping, snapshot insert (with `reachable`/`latency_ms`), and prune. Move the loop/env/prune constants and the snapshot+prune SQL out of `fmo_module_ln/src/gateways.rs` into here, generalized over `GatewaySource` and the schema name. Ping implementation:

```rust
use std::time::{Duration, Instant};
use chrono::{DateTime, Utc};
use fedimint_api_client::api::DynGlobalApi;
use fedimint_core::encoding::Encodable;
use tokio_postgres::Transaction;
use tracing::warn;
use crate::module::ModuleTaskCtx;

const POLL_INTERVAL_MINUTES: u64 = 5;
const SNAPSHOT_RETENTION_DAYS: i64 = 90;
const PRUNE_INTERVAL_HOURS: i64 = 6;
const PING_TIMEOUT: Duration = Duration::from_secs(5);

pub struct PolledGateway { pub gateway_id: String, pub api_endpoint: Option<String> }

#[async_trait::async_trait]
pub trait GatewaySource: Send + Sync {
    fn schema(&self) -> &'static str;
    async fn fetch_and_upsert(
        &self,
        dbtx: &Transaction<'_>,
        ctx: &ModuleTaskCtx,
        api: &DynGlobalApi,
        now: DateTime<Utc>,
    ) -> anyhow::Result<Vec<PolledGateway>>;
}

/// GET the gateway's API root with a bounded timeout. `reachable` = any HTTP
/// response received in time; `latency_ms` = round-trip. Never errors — a dead
/// gateway just reports `(false, None)` and cannot stall the caller.
pub async fn ping_gateway(api_endpoint: &str, timeout: Duration) -> (bool, Option<i32>) {
    let client = match reqwest::Client::builder().timeout(timeout).build() {
        Ok(c) => c,
        Err(_) => return (false, None),
    };
    let start = Instant::now();
    match client.get(api_endpoint).send().await {
        Ok(_resp) => (true, Some(start.elapsed().as_millis().min(i32::MAX as u128) as i32)),
        Err(_) => (false, None),
    }
}

pub async fn run_gateway_poller(
    ctx: ModuleTaskCtx,
    source: impl GatewaySource + 'static,
) -> anyhow::Result<()> {
    let poll_secs = std::env::var("FO_GATEWAY_POLL_SECS").ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(POLL_INTERVAL_MINUTES * 60);
    let peers = ctx.config.global.api_endpoints.iter()
        .map(|(&id, url)| (id, url.url.clone())).collect();
    let api = DynGlobalApi::new(ctx.connectors.clone(), peers, None)?;
    let mut interval = tokio::time::interval(Duration::from_secs(poll_secs));
    loop {
        interval.tick().await;
        if let Err(e) = poll_once(&ctx, &api, &source).await {
            warn!("gateway poll for {} failed: {e:?}", ctx.federation_id);
        }
    }
}

async fn poll_once(
    ctx: &ModuleTaskCtx,
    api: &DynGlobalApi,
    source: &impl GatewaySource,
) -> anyhow::Result<()> {
    let now = Utc::now();
    let fed = ctx.federation_id.consensus_encode_to_vec();
    let mut conn = ctx.pool.get().await?;
    let dbtx = conn.transaction().await?;

    let polled = source.fetch_and_upsert(&dbtx, ctx, api, now).await?;

    // Ping each currently-registered gateway (bounded, isolated).
    let mut ping = std::collections::HashMap::new();
    for gw in &polled {
        if let Some(ep) = &gw.api_endpoint {
            ping.insert(gw.gateway_id.clone(), ping_gateway(ep, PING_TIMEOUT).await);
        }
    }

    // Snapshot: seen gateways (is_seen=true, with ping result) + previously-known
    // but currently-absent ones (is_seen=false, unreachable).
    let schema = source.schema();
    for gw in &polled {
        let (reachable, latency) = ping.get(&gw.gateway_id).copied().unwrap_or((false, None));
        dbtx.execute(
            &format!("INSERT INTO {schema}.gateway_poll_snapshots
                      (federation_id, gateway_id, poll_time, is_seen, reachable, latency_ms)
                      VALUES ($1,$2,$3,true,$4,$5) ON CONFLICT DO NOTHING"),
            &[&fed, &gw.gateway_id, &now, &reachable, &latency],
        ).await?;
    }
    dbtx.execute(
        &format!("INSERT INTO {schema}.gateway_poll_snapshots
                  (federation_id, gateway_id, poll_time, is_seen, reachable, latency_ms)
                  SELECT $1, g.gateway_id, $2, false, false, NULL
                  FROM {schema}.gateways g
                  WHERE g.federation_id = $1
                    AND g.gateway_id <> ALL($3::text[])
                  ON CONFLICT DO NOTHING"),
        &[&fed, &now, &polled.iter().map(|g| g.gateway_id.clone()).collect::<Vec<_>>()],
    ).await?;

    // Prune old snapshots on a coarse schedule.
    let prune_interval = PRUNE_INTERVAL_HOURS * 3600;
    if now.timestamp().rem_euclid(prune_interval) < (POLL_INTERVAL_MINUTES as i64 * 60) {
        let cutoff = now - chrono::Duration::days(SNAPSHOT_RETENTION_DAYS);
        dbtx.execute(
            &format!("DELETE FROM {schema}.gateway_poll_snapshots WHERE federation_id=$1 AND poll_time<$2"),
            &[&fed, &cutoff],
        ).await?;
    }
    dbtx.commit().await?;
    Ok(())
}
```

Add `pub mod gateway_poll;` to `fmo_core/src/lib.rs`. Ensure `reqwest` and `async-trait` are dependencies of `fmo_core` (they are already used elsewhere in the workspace; add to `fmo_core/Cargo.toml` if absent).

- [ ] **Step 5: Refactor LNv1 onto the harness.** In `fmo_modules/fmo_module_ln/src/gateways.rs`:
  - Delete `monitor_gateways`, the loop/env constants, and the snapshot+prune SQL now living in the harness.
  - Keep `fetch_and_store_gateways`'s registry-query + `gateways` upsert, but reshape it into `impl GatewaySource for LnGatewaySource` — its `fetch_and_upsert` does the peer `LIST_GATEWAYS_ENDPOINT` merge + the existing `INSERT INTO fmo_ln.gateways ... ON CONFLICT` upsert, then returns `Vec<PolledGateway>` (gateway_id + `api_endpoint` from `gw.info.api.to_string()`). `schema()` returns `"fmo_ln"`.
  - Keep all of `get_federation_gateways` / `list_federation_gateways` / `GatewayMetricsWindow` unchanged (serving is LNv1-specific).
  - Update `fmo_module_ln/src/lib.rs` `run_federation_task` to call `fmo_core::gateway_poll::run_gateway_poller(ctx, gateways::LnGatewaySource).await`.
  - Bump `fn version(&self)` from `3` (Task 1) to `4`.

- [ ] **Step 6: Run tests.**

Run: `just test_package fmo_core` then `just test_package fmo_module_ln`
Expected: PASS — the two `gateway_poll` ping tests pass; LNv1's existing tests still pass.

- [ ] **Step 7: Lint & commit.**

Run: `just clippy`
Expected: clean.

```bash
git add fmo_core/src/gateway_poll.rs fmo_core/src/lib.rs fmo_core/tests/gateway_poll.rs \
        fmo_modules/fmo_module_ln/schema/v0.sql fmo_modules/fmo_module_ln/src/gateways.rs \
        fmo_modules/fmo_module_ln/src/lib.rs fmo_core/Cargo.toml
git commit -m "refactor(gateways): shared poll harness + real gateway-API ping (reachable/latency)"
```

---

## Task 5: LNv2 gateway observation

**Files:**
- Modify: `fmo_modules/fmo_module_lnv2/schema/v0.sql` (add `gateways` + `gateway_poll_snapshots` tables)
- Create: `fmo_modules/fmo_module_lnv2/src/gateways.rs` (`GatewaySource` impl + `/gateways` handler)
- Modify: `fmo_modules/fmo_module_lnv2/src/lib.rs` (bump `version()` to 4; `run_federation_task`; `api_router`)
- Modify: `fmo_modules/fmo_module_lnv2/Cargo.toml` (deps: axum, serde, fedimint-lnv2-common gateway endpoint, fmo_core gateway_poll — mirror `fmo_module_ln`'s deps)

**Interfaces:**
- Consumes: `fmo_core::gateway_poll::{GatewaySource, PolledGateway, run_gateway_poller}` (Task 4).

- [ ] **Step 1: Schema.** Append to `fmo_modules/fmo_module_lnv2/schema/v0.sql` (LNv2 registry is thinner — gateway API URLs, no vetting/node-key/fees):

```sql
CREATE TABLE gateways
(
    federation_id BYTEA       NOT NULL REFERENCES public.federations (federation_id),
    gateway_id    TEXT        NOT NULL,
    api_endpoint  TEXT        NOT NULL,
    first_seen    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (federation_id, gateway_id)
);

CREATE TABLE gateway_poll_snapshots
(
    federation_id BYTEA       NOT NULL REFERENCES public.federations (federation_id),
    gateway_id    TEXT        NOT NULL,
    poll_time     TIMESTAMPTZ NOT NULL,
    is_seen       BOOLEAN     NOT NULL,
    reachable     BOOLEAN     NOT NULL DEFAULT FALSE,
    latency_ms    INTEGER,
    PRIMARY KEY (federation_id, gateway_id, poll_time)
);
```

- [ ] **Step 2: Implement the source.** Create `fmo_modules/fmo_module_lnv2/src/gateways.rs`. `fetch_and_upsert` queries the lnv2 `GATEWAYS_ENDPOINT` on the lnv2 module instance across peers, unions the returned gateway API URLs, upserts `fmo_lnv2.gateways` (using the URL string as `gateway_id` since LNv2 identifies gateways by URL, and the same string as `api_endpoint`), and returns `PolledGateway { gateway_id, api_endpoint: Some(url) }`. The endpoint returns `Vec<SafeUrl>` (confirm the exact type in `fedimint_lnv2_common`'s `GATEWAYS_ENDPOINT` response while implementing; parse accordingly). Mirror `fmo_module_ln/src/gateways.rs`'s peer-merge + upsert shape.

```rust
use axum::extract::{Path, State};
use axum::Json;
use chrono::{DateTime, Utc};
use fedimint_api_client::api::{DynGlobalApi, FederationApiExt};
use fedimint_core::config::FederationId;
use fedimint_core::encoding::Encodable;
use fedimint_core::module::ApiRequestErased;
use fedimint_lnv2_common::endpoint_constants::GATEWAYS_ENDPOINT;
use fmo_core::api::ModuleApiState;
use fmo_core::gateway_poll::{GatewaySource, PolledGateway};
use fmo_core::module::ModuleTaskCtx;
use fmo_core::query::query;
use futures::future::join_all;
use tokio_postgres::Transaction;

pub struct LnV2GatewaySource;

#[async_trait::async_trait]
impl GatewaySource for LnV2GatewaySource {
    fn schema(&self) -> &'static str { "fmo_lnv2" }

    async fn fetch_and_upsert(
        &self,
        dbtx: &Transaction<'_>,
        ctx: &ModuleTaskCtx,
        api: &DynGlobalApi,
        now: DateTime<Utc>,
    ) -> anyhow::Result<Vec<PolledGateway>> {
        let instance_id = ctx.config.modules.iter()
            .find_map(|(&id, m)| (m.kind.as_str() == "lnv2").then_some(id))
            .ok_or_else(|| anyhow::anyhow!("no lnv2 module in config"))?;
        let peer_ids: Vec<_> = ctx.config.global.api_endpoints.keys().copied().collect();

        let results = join_all(peer_ids.into_iter().map(|peer| async move {
            api.with_module(instance_id)
               .request_single_peer(GATEWAYS_ENDPOINT.to_owned(), ApiRequestErased::default(), peer)
               .await
               .ok()
               .and_then(|v| serde_json::from_value::<Vec<fedimint_core::util::SafeUrl>>(v).ok())
        })).await;

        let mut urls = std::collections::BTreeSet::new();
        let mut any = false;
        for r in results.into_iter().flatten() { any = true; for u in r { urls.insert(u.to_string()); } }
        if !any { anyhow::bail!("no lnv2 gateway responses"); }

        let fed = ctx.federation_id.consensus_encode_to_vec();
        let url_vec: Vec<String> = urls.iter().cloned().collect();
        if !url_vec.is_empty() {
            dbtx.execute(
                "INSERT INTO gateways (federation_id, gateway_id, api_endpoint, first_seen, last_seen)
                 SELECT $1, u, u, $2, $2 FROM UNNEST($3::text[]) AS u
                 ON CONFLICT (federation_id, gateway_id) DO UPDATE SET last_seen = EXCLUDED.last_seen",
                &[&fed, &now, &url_vec],
            ).await?;
        }
        Ok(urls.into_iter().map(|u| PolledGateway { gateway_id: u.clone(), api_endpoint: Some(u) }).collect())
    }
}

pub async fn get_federation_gateways(
    Path(federation_id): Path<FederationId>,
    State(state): State<ModuleApiState>,
) -> fmo_core::error::Result<Json<Vec<serde_json::Value>>> {
    #[derive(postgres_from_row::FromRow)]
    struct Row { gateway_id: String, api_endpoint: String,
                 first_seen: DateTime<Utc>, last_seen: DateTime<Utc> }
    let conn = state.pool.get().await?;
    let rows = query::<Row>(&conn,
        "SELECT gateway_id, api_endpoint, first_seen, last_seen FROM fmo_lnv2.gateways
         WHERE federation_id=$1 ORDER BY last_seen DESC",
        &[&federation_id.consensus_encode_to_vec()]).await?;
    let _ = warn; // keep import if unused after trimming
    Ok(Json(rows.into_iter().map(|r| serde_json::json!({
        "gateway_id": r.gateway_id, "api_endpoint": r.api_endpoint,
        "first_seen": r.first_seen, "last_seen": r.last_seen,
    })).collect()))
}
```

(`get_federation_gateways` is LNv2's thin serving; unlike LNv1 it computes no contract-derived activity. Uptime/reachability from `gateway_poll_snapshots` can be folded in later if wanted — not required for parity.)

- [ ] **Step 3: Wire the module.** In `fmo_modules/fmo_module_lnv2/src/lib.rs`:
  - Add `mod gateways;`, `use std::sync::Arc;`, and the axum/router imports mirroring `fmo_module_ln`.
  - Bump `fn version(&self)` from `3` (Task 2) to `4`.
  - Add:

```rust
    async fn run_federation_task(self: std::sync::Arc<Self>, ctx: fmo_core::module::ModuleTaskCtx) {
        if let Err(e) = fmo_core::gateway_poll::run_gateway_poller(ctx.clone(), gateways::LnV2GatewaySource).await {
            tracing::warn!("lnv2 gateway monitor for {} exited: {e:?}", ctx.federation_id);
        }
    }

    fn api_router(&self) -> Option<axum::Router<fmo_core::api::ModuleApiState>> {
        Some(axum::Router::new().route("/gateways", axum::routing::get(gateways::get_federation_gateways)))
    }
```

  - Add the needed deps to `fmo_modules/fmo_module_lnv2/Cargo.toml` (axum, futures, serde, postgres-from-row, async-trait, fedimint-api-client, fedimint-lnv2-common) — copy the relevant lines from `fmo_module_ln/Cargo.toml`.

- [ ] **Step 4: Compile & lint** (the poller loop is network-facing and stays integration-tested in deployment, like LNv1's; the shared ping/snapshot logic is already unit-tested in Task 4, and status tests cover the module).

Run: `just clippy` then `just test_package fmo_module_lnv2`
Expected: compiles clean; existing lnv2 tests (incl. Task 2 status) pass.

- [ ] **Step 5: Register the compat alias (optional parity).** If LNv1's `/gateways` is also mounted at a legacy top-level path via `compat_routes` in `fmo_server`, add the lnv2 equivalent there so both are reachable; otherwise the module route `/federations/:id/modules/lnv2/gateways` is sufficient. Check `fmo_server/src/*.rs` for the `compat_routes` list.

- [ ] **Step 6: Commit.**

```bash
git add fmo_modules/fmo_module_lnv2/schema/v0.sql fmo_modules/fmo_module_lnv2/src/gateways.rs \
        fmo_modules/fmo_module_lnv2/src/lib.rs fmo_modules/fmo_module_lnv2/Cargo.toml
git commit -m "feat(lnv2): gateway observation on the shared poll harness"
```

---

## Final verification (after all tasks)

- `just clippy` and `just test` (or the per-package variants) green across `fmo_core`, `fmo_module_ln`, `fmo_module_lnv2`.
- `just final-check` before considering the branch done.
- Deployment is a separate, user-gated step: the fmo_ln (v4) and fmo_lnv2 (v4) version bumps trigger a schema drop + replay from raw sessions on the running instance; the core v2 migration drops `user_transactions.status` and rebuilds `user_tx_daily`. No refetch. Not part of the automated task run.

## Notes on task ordering & dependencies

- Tasks 1, 2, 3 are independent (different modules / core) and could run in any order; the natural order is status-in-modules (1, 2) then gold-strip (3).
- Task 4 must precede Task 5 (Task 5 builds on the extracted harness).
- Tasks 1 and 4 both edit `fmo_module_ln/schema/v0.sql` and bump `fmo_ln.version()` (→3 then →4) — keep them sequential to avoid conflicts. Tasks 2 and 5 likewise for `fmo_lnv2` (→3 then →4).
