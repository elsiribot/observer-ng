# User-Transaction Aggregation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a cross-module "gold" layer that deduplicates the multiple fedimint transactions a single user action produces (LN payments span 2–3) into directly-queryable per-user-transaction and daily-rollup tables, plus a fedimint-tx↔user-tx membership table for drill-down.

**Architecture:** A new core-owned gold schema (in `public`) built by an incremental per-federation processor whose cursor (`gold_progress`) trails the min of the installed modules' cursors, so a session is folded only after every module has written its silver rows. The processor is **pure SQL over the silver** (core `transactions`/`transaction_inputs`/`transaction_outputs` + `fmo_ln`/`fmo_lnv2` contract tables) — no session decoding. LN legs are grouped by `contract_id` (recompute-per-contract → idempotent); everything else is 1:1 by input/output kind signature.

**Tech Stack:** Rust, tokio, deadpool-postgres, PostgreSQL. Same conventions as the existing dispatch/processor code.

## Global Constraints

- Design spec: `docs/superpowers/specs/2026-08-07-user-transaction-aggregation-design.md` (authoritative for taxonomy, dedup keys, fee rules, decisions).
- `amount_msat` = the primary value counted **once**; fees separate. `fedimint_fee_msat` exact (`Σin−Σout`); `gateway_fee_estimate_msat` outgoing-LN only, estimated.
- Stranded LN = `status='in_flight'` (not failed). `mint→mint` = external `ecash_transfer`.
- Everything idempotent (`ON CONFLICT`), crash/replay-safe. The gold layer is a pure function of silver.
- DB tests gated on `FMO_TEST_DATABASE`; build needs cmake on PATH (`/nix/store/rfad6gqp02yfhjkh7m080jzcrq104jq8-cmake-4.1.2/bin`) or `nix develop`. After any dep add, re-check `bitcoin_hashes`/`secp256k1` pins (restore `Cargo.lock` from git if a dev-dep add re-resolves them).
- `transactions` PK is `(federation_id, txid)`; `module_progress` PK `(module_kind, federation_id)` with `next_session_index`.

## File Structure

- Create `fmo_core/schema/core/v1.sql` — gold tables + `user_tx_daily` matview.
- Modify `fmo_core/src/db/migrations.rs` — append v1 to `CORE_MIGRATIONS`.
- Create `fmo_core/src/gold.rs` — cursor logic + `fold_sessions` (standalone + LN grouping + fees), `run_gold_processor`.
- Modify `fmo_core/src/lib.rs` — `pub mod gold;`.
- Modify `fmo_core/src/observer.rs` — spawn the gold task in `spawn_federation`.
- Modify `fmo_core/src/api/mod.rs` — add `user_tx_daily` to the core matview refresh list.
- Create `fmo_core/tests/gold.rs` — gold-layer tests.
- Modify `CLAUDE.md` / `README.md` — document the gold layer + tables.

---

### Task 1: Core v1 migration — gold schema

**Files:**
- Create: `fmo_core/schema/core/v1.sql`
- Modify: `fmo_core/src/db/migrations.rs:18` (`CORE_MIGRATIONS`)
- Test: `fmo_core/tests/schema.rs`

**Interfaces:**
- Produces: tables `gold_progress`, `user_transactions`, `user_transaction_txs`, matview `user_tx_daily` in `public`; `core_schema_version` advances 0→1.

- [ ] **Step 1: Write `fmo_core/schema/core/v1.sql`** — the four objects exactly as in the spec:

```sql
CREATE TABLE gold_progress (
    federation_id      BYTEA   NOT NULL PRIMARY KEY REFERENCES federations (federation_id),
    next_session_index INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE user_transactions (
    federation_id             BYTEA   NOT NULL REFERENCES federations (federation_id),
    user_tx_key               BYTEA   NOT NULL,   -- contract_id (LN) else txid
    kind                      TEXT    NOT NULL,
    direction                 TEXT    NOT NULL CHECK (direction IN ('in','out','internal')),
    amount_msat               BIGINT,
    fedimint_fee_msat         BIGINT,
    gateway_fee_estimate_msat BIGINT,
    num_fedimint_txs          INTEGER NOT NULL,
    first_session_index       INTEGER NOT NULL,
    first_timestamp           TIMESTAMPTZ,
    last_timestamp            TIMESTAMPTZ,
    status                    TEXT    NOT NULL DEFAULT 'completed'
                                      CHECK (status IN ('completed','in_flight','cancelled')),
    PRIMARY KEY (federation_id, user_tx_key)
);
CREATE INDEX user_tx_fed_kind   ON user_transactions (federation_id, kind);
CREATE INDEX user_tx_fed_time   ON user_transactions (federation_id, first_timestamp);
CREATE INDEX user_tx_fed_status ON user_transactions (federation_id, status);

CREATE TABLE user_transaction_txs (
    federation_id BYTEA   NOT NULL REFERENCES federations (federation_id),
    txid          BYTEA   NOT NULL,
    user_tx_key   BYTEA   NOT NULL,
    role          TEXT    NOT NULL,   -- fund|claim|offer|cancel|refund|self
    session_index INTEGER NOT NULL,
    PRIMARY KEY (federation_id, txid, user_tx_key),
    FOREIGN KEY (federation_id, user_tx_key)
        REFERENCES user_transactions (federation_id, user_tx_key) ON DELETE CASCADE,
    FOREIGN KEY (federation_id, txid)
        REFERENCES transactions (federation_id, txid)
);
CREATE INDEX user_tx_txs_by_user ON user_transaction_txs (federation_id, user_tx_key);

CREATE MATERIALIZED VIEW user_tx_daily AS
SELECT federation_id,
       (first_timestamp AT TIME ZONE 'UTC')::date AS day,
       kind, direction, status,
       COUNT(*)                                    AS tx_count,
       COALESCE(SUM(amount_msat), 0)               AS volume_msat,
       COALESCE(SUM(fedimint_fee_msat), 0)         AS fedimint_fee_msat,
       COALESCE(SUM(gateway_fee_estimate_msat), 0) AS gateway_fee_estimate_msat
FROM user_transactions
WHERE first_timestamp IS NOT NULL
GROUP BY federation_id, day, kind, direction, status;
CREATE UNIQUE INDEX user_tx_daily_pk
    ON user_tx_daily (federation_id, day, kind, direction, status);
```

- [ ] **Step 2: Append v1 to `CORE_MIGRATIONS`**:

```rust
const CORE_MIGRATIONS: &[Migration] = &[
    Migration { sql: include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/schema/core/v0.sql")) },
    Migration { sql: include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/schema/core/v1.sql")) },
];
```

- [ ] **Step 3: Extend `schema.rs` test** — assert `core_schema_version` MAX = 1 after `setup_core_schema`, and idempotent re-run keeps gold tables + a seeded row. Add to `core_schema_applies_and_is_idempotent`:

```rust
let v: i32 = conn.query_one("SELECT MAX(version) FROM core_schema_version", &[]).await.unwrap().get(0);
assert_eq!(v, 1);
// gold tables exist
conn.execute("INSERT INTO gold_progress (federation_id, next_session_index) VALUES ($1, 0)", &[&&[1u8;32][..]]).await.unwrap_err(); // FK: federation must exist -> proves table+FK present
```

- [ ] **Step 4: Run** `FMO_TEST_DATABASE=… cargo test -p fmo_core --test schema` → PASS.
- [ ] **Step 5: Commit** `feat(core): gold schema for user-transaction aggregation`.

---

### Task 2: Gold processor — cursor + standalone (non-LN) classification + FM fee

**Files:**
- Create: `fmo_core/src/gold.rs`
- Modify: `fmo_core/src/lib.rs` (`pub mod gold;`), `fmo_core/src/observer.rs` (spawn task)
- Test: `fmo_core/tests/gold.rs`

**Interfaces:**
- Produces: `pub async fn run_gold_processor(observer, federation_id)`; `pub async fn fold_sessions(dbtx, federation_id, start, end) -> anyhow::Result<()>` (used by tests + processor).
- Consumes: `module_progress` (target cursor), core silver tables.

- [ ] **Step 1: Write failing test** `fmo_core/tests/gold.rs` — standalone shapes. Seed a federation, `gold_progress` row, and three txs in session 1: peg-in (`wallet`-in `mint`-out, in=100k out=99k), peg-out (`mint`-in `wallet`-out), ecash (`mint`-in `mint`-out). Call `fold_sessions(&dbtx, fed, 0, 2)`. Assert:
  - a `user_transactions` row per txid with correct `kind`/`direction`/`amount_msat`/`fedimint_fee_msat` (peg_in fee = 100k−99k = 1000),
  - a `user_transaction_txs` `self` row per txid,
  - re-running `fold_sessions` changes nothing (idempotent).

- [ ] **Step 2: Run** → FAIL (module missing).

- [ ] **Step 3: Implement standalone classification** in `gold.rs`. Per batch, one INSERT…SELECT that aggregates each tx's input/output kinds+amounts and classifies, skipping LN-leg txs (handled in Task 3):

```rust
pub async fn fold_standalone(dbtx: &Transaction<'_>, fed: &[u8], start: i32, end: i32) -> anyhow::Result<()> {
    // in_kinds/out_kinds/in_amt/out_amt per tx; classify by kind signature.
    dbtx.execute(
        "INSERT INTO user_transactions
           (federation_id, user_tx_key, kind, direction, amount_msat,
            fedimint_fee_msat, num_fedimint_txs, first_session_index,
            first_timestamp, last_timestamp, status)
         SELECT t.federation_id, t.txid,
                CASE
                  WHEN i.kinds @> ARRAY['wallet'] AND NOT (i.kinds && ARRAY['ln','lnv2']) THEN 'peg_in'
                  WHEN o.kinds @> ARRAY['wallet'] AND NOT (o.kinds && ARRAY['ln','lnv2']) THEN 'peg_out'
                  WHEN o.kinds @> ARRAY['walletv2'] THEN 'peg_in_v2'
                  WHEN i.kinds @> ARRAY['walletv2'] THEN 'peg_out_v2'
                  WHEN (i.kinds && ARRAY['stability_pool','multi_sig_stability_pool'])
                    OR (o.kinds && ARRAY['stability_pool','multi_sig_stability_pool']) THEN 'stability_pool'
                  WHEN i.kinds <@ ARRAY['mint'] AND o.kinds <@ ARRAY['mint'] THEN 'ecash_transfer'
                  WHEN i.kinds <@ ARRAY['mintv2'] AND o.kinds <@ ARRAY['mintv2'] THEN 'ecash_transfer_v2'
                  ELSE 'other'
                END AS kind,
                CASE
                  WHEN i.kinds @> ARRAY['wallet'] OR o.kinds @> ARRAY['walletv2'] THEN 'in'
                  WHEN o.kinds @> ARRAY['wallet'] OR i.kinds @> ARRAY['walletv2'] THEN 'out'
                  ELSE 'internal'
                END AS direction,
                -- primary value: wallet side for pegs, else input side
                CASE
                  WHEN i.kinds @> ARRAY['wallet'] THEN i.wallet_amt
                  WHEN o.kinds @> ARRAY['wallet'] THEN o.wallet_amt
                  ELSE i.amt END AS amount_msat,
                (i.amt - o.amt) AS fedimint_fee_msat,
                1, t.session_index, st.ts, st.ts, 'completed'
         FROM transactions t
         JOIN LATERAL (SELECT array_agg(DISTINCT kind) kinds, SUM(amount_msat) amt,
                              SUM(amount_msat) FILTER (WHERE kind='wallet') wallet_amt
                       FROM transaction_inputs WHERE federation_id=t.federation_id AND txid=t.txid) i ON true
         JOIN LATERAL (SELECT array_agg(DISTINCT kind) kinds, SUM(amount_msat) amt,
                              SUM(amount_msat) FILTER (WHERE kind='wallet') wallet_amt
                       FROM transaction_outputs WHERE federation_id=t.federation_id AND txid=t.txid) o ON true
         LEFT JOIN session_times st ON st.federation_id=t.federation_id AND st.session_index=t.session_index
         WHERE t.federation_id=$1 AND t.session_index>=$2 AND t.session_index<$3
           AND NOT EXISTS (SELECT 1 FROM fmo_ln.output_contracts oc WHERE oc.federation_id=t.federation_id AND oc.txid=t.txid)
           AND NOT EXISTS (SELECT 1 FROM fmo_ln.input_contracts  ic WHERE ic.federation_id=t.federation_id AND ic.txid=t.txid)
           AND NOT EXISTS (SELECT 1 FROM fmo_lnv2.contracts        c2 WHERE c2.federation_id=t.federation_id AND c2.txid=t.txid)
           AND NOT EXISTS (SELECT 1 FROM fmo_lnv2.input_outpoints  io WHERE io.federation_id=t.federation_id AND io.txid=t.txid)
         ON CONFLICT (federation_id, user_tx_key) DO UPDATE SET
            kind=EXCLUDED.kind, direction=EXCLUDED.direction, amount_msat=EXCLUDED.amount_msat,
            fedimint_fee_msat=EXCLUDED.fedimint_fee_msat, first_timestamp=EXCLUDED.first_timestamp,
            last_timestamp=EXCLUDED.last_timestamp",
        &[&fed, &start, &end]).await?;
    // self membership rows
    dbtx.execute(
        "INSERT INTO user_transaction_txs (federation_id, txid, user_tx_key, role, session_index)
         SELECT federation_id, user_tx_key, user_tx_key, 'self', first_session_index
         FROM user_transactions WHERE federation_id=$1 AND first_session_index>=$2 AND first_session_index<$3
           AND user_tx_key IN (SELECT txid FROM transactions WHERE federation_id=$1)
         ON CONFLICT DO NOTHING", &[&fed, &start, &end]).await?;
    Ok(())
}
```

> Note: guard each `NOT EXISTS` on `fmo_ln`/`fmo_lnv2` with a schema-exists check (graceful degradation) — see Task 3 Step 4; for now assume present in tests.

- [ ] **Step 4: Implement `fold_sessions`** = `fold_standalone` for this task (LN added in Task 3), and `run_gold_processor`:

```rust
pub async fn run_gold_processor(observer: FederationObserver, fed: FederationId) -> anyhow::Result<()> {
    const BATCH: i32 = 500;
    let fedb = fed.consensus_encode_to_vec();
    loop {
        let conn = observer.pool().get().await?;
        conn.execute("INSERT INTO gold_progress (federation_id, next_session_index) VALUES ($1,0) ON CONFLICT DO NOTHING", &[&fedb]).await?;
        // target = min over installed module cursors for this federation
        let target: i32 = conn.query_one(
            "SELECT COALESCE(MIN(next_session_index), 0) FROM module_progress WHERE federation_id=$1", &[&fedb]).await?.get(0);
        let mut next: i32 = conn.query_one("SELECT next_session_index FROM gold_progress WHERE federation_id=$1", &[&fedb]).await?.get(0);
        // rewind if a module replayed below us
        if target < next { next = target; conn.execute("UPDATE gold_progress SET next_session_index=$2 WHERE federation_id=$1", &[&fedb, &next]).await?; }
        if next >= target { drop(conn); tokio::time::sleep(Duration::from_secs(5)).await; continue; }
        let end = (next + BATCH).min(target);
        let mut conn = observer.pool().get().await?;
        let dbtx = conn.transaction().await?;
        fold_sessions(&dbtx, &fedb, next, end).await?;
        dbtx.execute("UPDATE gold_progress SET next_session_index=$2 WHERE federation_id=$1", &[&fedb, &end]).await?;
        dbtx.commit().await?;
    }
}
```

- [ ] **Step 5: Wire** `pub mod gold;` in `lib.rs`; spawn in `observer.rs::spawn_federation` alongside the module task:

```rust
self.task_group.spawn_cancellable(
    format!("gold {federation_id}"),
    { let observer = self.clone(); async move {
        if let Err(e) = crate::gold::run_gold_processor(observer, federation_id).await { warn!("gold processor: {e:?}"); }
    }}.instrument(info_span!("gold", fed = %federation_id.to_prefix())));
```

- [ ] **Step 6: Run** `cargo test -p fmo_core --test gold` → PASS. Then `cargo clippy --workspace`.
- [ ] **Step 7: Commit** `feat(core): gold processor with standalone tx classification and fedimint fees`.

---

### Task 3: LN / LNv2 contract grouping + status + membership

**Files:**
- Modify: `fmo_core/src/gold.rs` (add `fold_ln`, call from `fold_sessions`)
- Test: `fmo_core/tests/gold.rs`

**Interfaces:**
- Consumes: `fmo_ln.contracts/output_contracts/input_contracts`, `fmo_lnv2.contracts/input_outpoints`, core `transaction_outputs.amount_msat`.
- Produces: `user_transactions` rows keyed by `contract_id`; `user_transaction_txs` rows with `role`.

- [ ] **Step 1: Write failing test** — LN receive across sessions. Seed `fmo_ln` (setup_module_schema "ln") + core rows: an **offer** output (session 1, no inputs), a **fund** output (session 2, `mint`-in amount 10000 → `ln`-out contract C, funded 10000), a **claim** input (session 3, `ln`-in C → `mint`-out). `fold_sessions(0,4)`. Assert one `user_transactions` row: `user_tx_key=C`, `kind='ln_receive'`, `direction='in'`, `amount_msat=10000`, `status='completed'`, `num_fedimint_txs=3`; three `user_transaction_txs` rows with roles `offer`/`fund`/`claim`. Second test: fund only, no claim → `status='in_flight'`, `num_fedimint_txs` counts offer+fund.

- [ ] **Step 2: Run** → FAIL.

- [ ] **Step 3: Implement `fold_ln`** — recompute every contract *touched* by a tx in `[start,end)` (idempotent, order-independent). One statement builds the grain from silver:

```sql
INSERT INTO user_transactions (federation_id, user_tx_key, kind, direction, amount_msat,
    fedimint_fee_msat, num_fedimint_txs, first_session_index, first_timestamp, last_timestamp, status)
SELECT c.federation_id, c.contract_id,
       CASE WHEN c.type='incoming' THEN 'ln_receive' ELSE 'ln_send' END,
       CASE WHEN c.type='incoming' THEN 'in' ELSE 'out' END,
       funds.amount_msat,
       fees.fee_msat,
       legs.n,
       legs.first_session,
       fst.ts, lst.ts,
       CASE WHEN spends.any THEN 'completed'
            WHEN cancels.any THEN 'cancelled'
            ELSE 'in_flight' END
FROM fmo_ln.contracts c
JOIN (SELECT federation_id, contract_id, SUM(o.amount_msat) amount_msat
      FROM fmo_ln.output_contracts oc JOIN transaction_outputs o USING (federation_id, txid, out_index)
      WHERE oc.interaction_kind='fund' GROUP BY 1,2) funds USING (federation_id, contract_id)
JOIN (SELECT federation_id, contract_id, COUNT(DISTINCT txid) n, MIN(session_index) first_session
      FROM (SELECT oc.federation_id, oc.contract_id, oc.txid, t.session_index
              FROM fmo_ln.output_contracts oc JOIN transactions t USING (federation_id, txid)
            UNION
            SELECT ic.federation_id, ic.contract_id, ic.txid, t.session_index
              FROM fmo_ln.input_contracts ic JOIN transactions t USING (federation_id, txid)) all_legs
      GROUP BY 1,2) legs USING (federation_id, contract_id)
JOIN LATERAL (SELECT COALESCE(SUM(f.fee),0) fee_msat FROM (
        SELECT (SELECT SUM(amount_msat) FROM transaction_inputs  WHERE federation_id=x.federation_id AND txid=x.txid)
             - (SELECT SUM(amount_msat) FROM transaction_outputs WHERE federation_id=x.federation_id AND txid=x.txid) fee
        FROM (SELECT DISTINCT federation_id, txid FROM (
               SELECT federation_id, txid FROM fmo_ln.output_contracts WHERE federation_id=c.federation_id AND contract_id=c.contract_id
               UNION SELECT federation_id, txid FROM fmo_ln.input_contracts WHERE federation_id=c.federation_id AND contract_id=c.contract_id) u) x) f) fees ON true
JOIN LATERAL (SELECT bool_or(true) any FROM fmo_ln.input_contracts WHERE federation_id=c.federation_id AND contract_id=c.contract_id) spends ON true
LEFT JOIN LATERAL (SELECT bool_or(true) any FROM fmo_ln.output_contracts WHERE federation_id=c.federation_id AND contract_id=c.contract_id AND interaction_kind='cancel') cancels ON true
LEFT JOIN session_times fst ON fst.federation_id=c.federation_id AND fst.session_index=legs.first_session
LEFT JOIN LATERAL (SELECT ts FROM (SELECT st.estimated_session_timestamp ts FROM ... ORDER BY session_index DESC LIMIT 1)) lst ON true
WHERE (c.federation_id, c.contract_id) IN (
        SELECT oc.federation_id, oc.contract_id FROM fmo_ln.output_contracts oc JOIN transactions t USING (federation_id, txid)
          WHERE t.federation_id=$1 AND t.session_index>=$2 AND t.session_index<$3
        UNION
        SELECT ic.federation_id, ic.contract_id FROM fmo_ln.input_contracts ic JOIN transactions t USING (federation_id, txid)
          WHERE t.federation_id=$1 AND t.session_index>=$2 AND t.session_index<$3)
ON CONFLICT (federation_id, user_tx_key) DO UPDATE SET
   kind=EXCLUDED.kind, direction=EXCLUDED.direction, amount_msat=EXCLUDED.amount_msat,
   fedimint_fee_msat=EXCLUDED.fedimint_fee_msat, num_fedimint_txs=EXCLUDED.num_fedimint_txs,
   first_session_index=EXCLUDED.first_session_index, first_timestamp=EXCLUDED.first_timestamp,
   last_timestamp=EXCLUDED.last_timestamp, status=EXCLUDED.status;
```

Then the membership rows with role:

```sql
INSERT INTO user_transaction_txs (federation_id, txid, user_tx_key, role, session_index)
SELECT oc.federation_id, oc.txid, oc.contract_id, oc.interaction_kind, t.session_index
  FROM fmo_ln.output_contracts oc JOIN transactions t USING (federation_id, txid)
  WHERE (oc.federation_id, oc.contract_id) IN (<touched-contracts subquery>)
UNION ALL
SELECT ic.federation_id, ic.txid, ic.contract_id,
       CASE WHEN EXISTS (SELECT 1 FROM fmo_ln.output_contracts x WHERE x.federation_id=ic.federation_id AND x.contract_id=ic.contract_id AND x.interaction_kind='cancel')
            THEN 'refund' ELSE 'claim' END,
       t.session_index
  FROM fmo_ln.input_contracts ic JOIN transactions t USING (federation_id, txid)
  WHERE (ic.federation_id, ic.contract_id) IN (<touched-contracts subquery>)
ON CONFLICT DO NOTHING;
```

- Add the analogous **lnv2** block (kinds `lnv2_send`/`lnv2_receive`; amount from `fmo_lnv2.contracts.amount_msat`; legs from `fmo_lnv2.contracts` (fund) + `fmo_lnv2.input_outpoints` (claim); lnv2 has no offer/cancel, so roles are `fund`/`claim`; status `completed` if an input_outpoint exists else `in_flight`).

- [ ] **Step 4: Graceful degradation** — before referencing `fmo_ln`/`fmo_lnv2`, check `to_regclass('fmo_ln.contracts')` etc. is non-null (module installed); skip that block otherwise. Factor the "touched contracts" subquery into a Rust `const &str` to avoid repetition.

- [ ] **Step 5: Run** `cargo test -p fmo_core --test gold` → PASS (both LN tests + Task 2 standalone tests still green). `cargo clippy --workspace`.
- [ ] **Step 6: Commit** `feat(core): group LN/LNv2 legs into user transactions with status and membership`.

---

### Task 4: `gateway_fee_estimate_msat` for outgoing LN

**Files:**
- Modify: `fmo_core/src/gold.rs` (post-pass updating outgoing rows)
- Test: `fmo_core/tests/gold.rs`

**Interfaces:**
- Consumes: outgoing fund output `details` (gateway_key), `fmo_ln.gateways.raw` (fee schedule).

- [ ] **Step 1: Verify the key mapping** (spike, not committed): confirm which `fmo_ln.gateways` column matches the `OutgoingContract.gateway_key` in the fund output's `details` JSON (`details #>> '{V0,Contract,contract,Outgoing,gateway_key}'` vs `gateway_id`/`node_pub_key`). Run a one-off query on the live DB. Record the join path in a code comment.

- [ ] **Step 2: Write failing test** — an `ln_send` contract funded 10520 msat, gateway with `fees.base_msat=2000, proportional_millionths=5000` (0.5%). Expected: `invoice=(10520-2000)/(1+0.000005)≈8477`, `gateway_fee_estimate_msat=10520-8477≈2043`. Assert the row's `gateway_fee_estimate_msat` within ±1 (rounding), and that `ln_receive`/non-LN rows stay NULL.

- [ ] **Step 3: Implement** as an UPDATE over outgoing user_txs touched this batch:

```sql
UPDATE user_transactions ut SET gateway_fee_estimate_msat =
   ut.amount_msat - round((ut.amount_msat - g.base) / (1 + g.ppm/1e6.0))
FROM (SELECT oc.federation_id, oc.contract_id,
             (gw.raw #>> '{fees,base_msat}')::numeric base,
             (gw.raw #>> '{fees,proportional_millionths}')::numeric ppm
      FROM fmo_ln.output_contracts oc
      JOIN transaction_outputs o USING (federation_id, txid, out_index)
      JOIN fmo_ln.gateways gw ON gw.federation_id=oc.federation_id
           AND gw.<matched_col> = (o.details #>> '{V0,Contract,contract,Outgoing,gateway_key}')
      WHERE oc.interaction_kind='fund') g
WHERE ut.federation_id=g.federation_id AND ut.user_tx_key=g.contract_id
  AND ut.kind='ln_send' AND ut.federation_id=$1 AND ut.first_session_index>=$2 AND ut.first_session_index<$3;
```

Guard on `fmo_ln.gateways` existing and on a matching gateway row (leave NULL when unknown). Add the lnv2 analogue only if lnv2 outgoing contracts expose a gateway key with a matching fee source; otherwise leave lnv2 send fees NULL and note it.

- [ ] **Step 4: Run** tests → PASS. `cargo clippy`.
- [ ] **Step 5: Commit** `feat(core): estimate outgoing LN gateway fees from advertised schedule`.

---

### Task 5: Refresh `user_tx_daily` + docs

**Files:**
- Modify: `fmo_core/src/api/mod.rs:140` (matview list), `CLAUDE.md`, `README.md`
- Test: `fmo_core/tests/gold.rs`

- [ ] **Step 1: Add** `"user_tx_daily"` to the core matview vector in `refresh_views_inner` (alongside `"session_times"`).
- [ ] **Step 2: Write test** — after folding a few user txs, `REFRESH MATERIALIZED VIEW user_tx_daily` and assert a `(federation, day, kind)` row with correct `tx_count`/`volume_msat`.
- [ ] **Step 3: Run** → PASS.
- [ ] **Step 4: Document** the gold layer + four objects in `CLAUDE.md` (Architecture + Database Schema sections) and `README.md` (Architecture). Note: gold layer is a pure function of silver, cursor trails module cursors, `gateway_fee_estimate_msat` is an estimate.
- [ ] **Step 5: Commit** `feat(core): refresh user_tx_daily; docs for the gold layer`.

---

### Task 6: Deploy + backfill validation (runner-01)

**Files:** none (ops); optionally `fmo_core/tests/gold.rs` for any regression found.

- [ ] **Step 1:** `cargo fmt --all`; `cargo clippy --workspace --all-targets` (0 warnings); full `cargo test --workspace` green.
- [ ] **Step 2:** Build the runner-01 closure (bump `fedimint-observer-modular` in `elsirion-infa`), deploy. Core schema v0→1 applies; gold processors backfill via `gold_progress` (no module replay).
- [ ] **Step 3: Validate** on real data once caught up:
  - `SELECT kind, COUNT(*), SUM(amount_msat) FROM user_transactions GROUP BY 1` — sanity per type.
  - Compare raw LN leg count vs `user_transactions` LN count (expect ~2–3× reduction).
  - Spot-check a known LN payment via `user_transaction_txs` (offer+fund+claim collapse to one row).
  - Verify `user_tx_daily` volume for a federation against a manual sum.
  - Confirm `gateway_fee_estimate_msat` is NULL for receives, populated for sends, and plausible (< amount).
- [ ] **Step 4:** If a shape/edge surfaces (e.g. atomic multi-module tx, `other` kind with volume), add a regression test to `gold.rs` and fix.
- [ ] **Step 5: Commit** any fixes; update the memory note with the gold-layer deployment + validation numbers.

## Self-Review notes

- **Spec coverage:** taxonomy (T2/T3), fees exact+estimate (T2/T4), status/in_flight (T3), membership/drill-down (T1/T3), incremental cursor + rewind (T2), rollup (T5), graceful degradation (T2/T3). ✓
- **Type consistency:** `user_tx_key` = contract_id (LN) / txid (else) everywhere; `fold_sessions(dbtx, fed:&[u8], start:i32, end:i32)` used by tests and processor; matview name `user_tx_daily` in both DDL and refresh list.
- **Known risks to watch in impl:** (a) the fedimint-fee LATERAL in Task 3 double-counts a tx shared by two contracts (rare atomic case) — acceptable, documented; (b) `details` JSON path for gateway_key must be confirmed (T4 S1) before relying on it; (c) `mint→mint` with 3+ mixed kinds falls to `other` — verify the `<@`/`@>` array predicates against real shapes during T2 validation.
