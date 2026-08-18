-- Fiat-denominated gold layer for the `multi_sig_stability_pool` module.
--
-- The core gold layer (`public.user_transactions`) is msat-denominated and
-- treats stability-pool activity as the opaque `stability_pool` kind. Stability
-- pool value is naturally fiat (a seeker holds a stabilized fiat amount), so its
-- gold layer lives here, in the module's own schema, valued in the federation's
-- stable-currency BASE UNIT (cents for a USD federation, whole units for
-- JPY/KRW/…). The currency is an oracle property and is NOT carried in
-- consensus, so it is never hardcoded — amounts are the raw base unit.
--
-- Two tiers, mirroring the repo's silver → gold split:
--   * silver (this file's plain tables): richer structural facts extracted by
--     `process_input`/`process_output` than v0 captured — transfer details and
--     observed multisig structure.
--   * gold (this file's materialized views): folded, fiat-valued analysis,
--     recomputed each refresh cycle via the module's `matviews()` hook (after
--     `public.session_times` and `heal_gold`, so timestamps/cycles are final).
--
-- Everything derives purely from consensus (tx inputs/outputs + cycle votes).
-- Guardian-internal state (per-cycle seeker↔provider settlement, auto-renewal,
-- staged/locked/idle balances, fees charged) is NOT observable, so per-account
-- figures are exact NET FLOWS, not live balances (see `account_totals`).

------------------------------------------------------------------------------
-- Silver
------------------------------------------------------------------------------

-- Transfers between stability-pool accounts. These are fedimint transaction
-- OUTPUTS that move no msats (amount 0) but carry a signed, fiat-denominated
-- transfer request. v0 collapsed them into `deposits` as 0-msat rows and threw
-- away the recipient / fiat amount; they now get a dedicated table and are no
-- longer written to `deposits`.
CREATE TABLE transfers
(
    federation_id     BYTEA    NOT NULL REFERENCES public.federations (federation_id),
    txid              BYTEA    NOT NULL,
    out_index         INTEGER  NOT NULL,
    -- Output enum version (0 = StabilityPoolOutputV0, 1 = StabilityPoolOutputV1).
    version           SMALLINT NOT NULL,
    -- Account type of both endpoints (transfers require matching types):
    -- 'seeker' | 'provider' | 'btc_depositor'.
    acc_type          TEXT     NOT NULL,
    from_account_id   TEXT     NOT NULL,
    to_account_id     TEXT     NOT NULL,
    -- Transferred amount, fiat base unit (the request is fiat-denominated).
    transfer_fiat     BIGINT   NOT NULL,
    -- Cycle index after which the signed request is no longer valid.
    valid_until_cycle BIGINT   NOT NULL,
    -- New provider fee rate (ppb) for provider→provider transfers only.
    new_fee_rate_ppb  BIGINT,
    -- Arbitrary client-embedded metadata attached to the request.
    meta              BYTEA    NOT NULL,
    PRIMARY KEY (federation_id, txid, out_index),
    FOREIGN KEY (federation_id, txid, out_index)
        REFERENCES public.transaction_outputs (federation_id, txid, out_index)
);
CREATE INDEX sp_transfers_from ON transfers (federation_id, from_account_id);
CREATE INDEX sp_transfers_to ON transfers (federation_id, to_account_id);

-- Observed multisig structure of an account. Only knowable when an account
-- reveals its full `Account` (withdrawal inputs and transfer-`from` carry it;
-- deposit outputs carry only the account-id hash). So a deposit-only account
-- never appears here — an inherent observability limit. Structure is stable per
-- account id (the id is the hash of the Account), so first writer wins.
CREATE TABLE account_multisig
(
    federation_id      BYTEA   NOT NULL REFERENCES public.federations (federation_id),
    account_id         TEXT    NOT NULL,
    acc_type           TEXT    NOT NULL,
    threshold          BIGINT  NOT NULL,
    n_keys             BIGINT  NOT NULL,
    first_seen_session INTEGER NOT NULL,
    PRIMARY KEY (federation_id, account_id)
);

-- The signing pubkeys of an observed `Account`, one row per key. `key_index` is
-- the 0-based position within the Account's (sorted) key set — the same index
-- signatures reference.
CREATE TABLE account_keys
(
    federation_id BYTEA   NOT NULL REFERENCES public.federations (federation_id),
    account_id    TEXT    NOT NULL,
    key_index     INTEGER NOT NULL,
    -- 33-byte compressed secp256k1 pubkey.
    pubkey        BYTEA   NOT NULL,
    PRIMARY KEY (federation_id, account_id, key_index)
);
CREATE INDEX sp_account_keys_pubkey ON account_keys (federation_id, pubkey);

------------------------------------------------------------------------------
-- Gold (materialized views; refreshed via the module's matviews() hook)
------------------------------------------------------------------------------

-- Price/time series per cycle: the guardians vote a wall-clock time and a
-- BTC→fiat price at each turnover and consensus takes the median, so the median
-- of the votes proposing `next_cycle_index = N` reconstructs cycle N's start.
-- This is what lets any msat leg be valued in fiat at the cycle active when it
-- happened.
CREATE MATERIALIZED VIEW cycles AS
SELECT federation_id,
       next_cycle_index                                                    AS cycle_index,
       (percentile_cont(0.5) WITHIN GROUP (ORDER BY price_fiat))::bigint   AS start_price_fiat,
       TIMESTAMP 'epoch'
           + percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM vote_time))
             * INTERVAL '1 second'                                         AS start_time,
       COUNT(*)                                                            AS num_votes
FROM cycle_votes
GROUP BY federation_id, next_cycle_index
WITH DATA;
CREATE UNIQUE INDEX cycles_pk ON cycles (federation_id, cycle_index);
-- Supports the `account_tx` lateral that finds the active cycle for a leg's
-- timestamp (federation_id filter + start_time ORDER BY … DESC LIMIT 1).
CREATE INDEX cycles_start_time ON cycles (federation_id, start_time);

-- Folded, fiat-valued transaction history — one row per LOGICAL user operation:
--   * deposit_seek / deposit_provide / deposit_btc — one row per deposit output.
--   * withdraw — the two-step unlock_for_withdrawal + withdrawal folded into ONE
--     row. Consensus carries no explicit correlation id, but at most one unlock
--     is active per account at a time, so unlocks and withdrawals for an account
--     pair up by ordinal. An unpaired unlock (abandoned withdrawal) becomes a
--     standalone withdraw row with NULL amount_msat (fiat target only); a
--     withdrawal with no observed unlock stands alone too.
--   * transfer_out / transfer_in — a transfer emits one row for the sender and
--     one for the recipient, so both accounts' histories show it.
-- Deposits/withdrawals are valued at their active cycle's price; transfers are
-- fiat-native. `fiat_amount` is NULL only when the session timestamp (hence the
-- cycle) is not yet known and there is no explicit fiat target.
CREATE MATERIALIZED VIEW account_tx AS
WITH dep AS (
    SELECT d.federation_id,
           encode(d.txid, 'hex') || ':o' || d.out_index AS tx_key,
           d.account_id,
           CASE d.action
               WHEN 'deposit_to_seek' THEN 'deposit_seek'
               WHEN 'deposit_to_provide' THEN 'deposit_provide'
               WHEN 'deposit_to_btc_balance' THEN 'deposit_btc'
           END                                          AS kind,
           'in'::text                                   AS direction,
           d.amount_msat,
           d.txid                                       AS primary_txid,
           NULL::bytea                                  AS secondary_txid,
           t.session_index,
           NULL::bigint                                 AS explicit_fiat
    FROM deposits d
             JOIN public.transactions t USING (federation_id, txid)
    WHERE d.action <> 'transfer'
),
w_all AS (
    SELECT w.*, t.session_index
    FROM withdrawals w
             JOIN public.transactions t USING (federation_id, txid)
),
unlocks AS (
    SELECT *, row_number() OVER (PARTITION BY federation_id, account_id
        ORDER BY session_index, in_index) AS rn
    FROM w_all WHERE kind = 'unlock_for_withdrawal'
),
wdraws AS (
    SELECT *, row_number() OVER (PARTITION BY federation_id, account_id
        ORDER BY session_index, in_index) AS rn
    FROM w_all WHERE kind = 'withdrawal'
),
wd AS (
    SELECT COALESCE(w.federation_id, u.federation_id) AS federation_id,
           encode(COALESCE(w.txid, u.txid), 'hex')
               || CASE WHEN w.txid IS NULL THEN ':u' || u.in_index
                       ELSE ':w' || w.in_index END      AS tx_key,
           COALESCE(w.account_id, u.account_id)         AS account_id,
           'withdraw'::text                             AS kind,
           'out'::text                                  AS direction,
           w.amount_msat,
           COALESCE(w.txid, u.txid)                     AS primary_txid,
           CASE WHEN w.txid IS NOT NULL THEN u.txid END AS secondary_txid,
           COALESCE(w.session_index, u.session_index)   AS session_index,
           u.unlock_fiat                                AS explicit_fiat
    FROM wdraws w
             FULL OUTER JOIN unlocks u
                 ON u.federation_id = w.federation_id
                     AND u.account_id = w.account_id AND u.rn = w.rn
),
tr AS (
    SELECT tr.federation_id, tr.txid, tr.out_index,
           tr.from_account_id, tr.to_account_id, tr.transfer_fiat,
           t.session_index
    FROM transfers tr
             JOIN public.transactions t USING (federation_id, txid)
),
tr_out AS (
    SELECT federation_id,
           encode(txid, 'hex') || ':o' || out_index || ':out' AS tx_key,
           from_account_id AS account_id, 'transfer_out'::text AS kind,
           'internal'::text AS direction, NULL::bigint AS amount_msat,
           txid AS primary_txid, NULL::bytea AS secondary_txid,
           session_index, transfer_fiat AS explicit_fiat
    FROM tr
),
tr_in AS (
    SELECT federation_id,
           encode(txid, 'hex') || ':o' || out_index || ':in' AS tx_key,
           to_account_id AS account_id, 'transfer_in'::text AS kind,
           'internal'::text AS direction, NULL::bigint AS amount_msat,
           txid AS primary_txid, NULL::bytea AS secondary_txid,
           session_index, transfer_fiat AS explicit_fiat
    FROM tr
),
unioned AS (
    SELECT * FROM dep
    UNION ALL SELECT * FROM wd
    UNION ALL SELECT * FROM tr_out
    UNION ALL SELECT * FROM tr_in
)
SELECT u.federation_id, u.tx_key, u.account_id, u.kind, u.direction,
       u.amount_msat,
       COALESCE(
           CASE WHEN u.amount_msat IS NOT NULL AND cyc.start_price_fiat IS NOT NULL
                THEN (u.amount_msat::numeric * cyc.start_price_fiat / 100000000000)::bigint
           END,
           u.explicit_fiat
       )                                        AS fiat_amount,
       u.amount_msat IS NULL AND u.explicit_fiat IS NOT NULL AS fiat_is_target,
       cyc.cycle_index,
       cyc.start_price_fiat                     AS cycle_price_fiat,
       u.session_index,
       st.estimated_session_timestamp           AS timestamp,
       u.primary_txid,
       u.secondary_txid
FROM unioned u
         LEFT JOIN public.session_times st
             ON st.federation_id = u.federation_id AND st.session_index = u.session_index
         LEFT JOIN LATERAL (
    SELECT c.cycle_index, c.start_price_fiat
    FROM cycles c
    WHERE c.federation_id = u.federation_id
      AND c.start_time <= st.estimated_session_timestamp
    ORDER BY c.start_time DESC
    LIMIT 1
    ) cyc ON true
WITH DATA;
CREATE UNIQUE INDEX account_tx_pk ON account_tx (federation_id, tx_key);
CREATE INDEX account_tx_account ON account_tx (federation_id, account_id);
CREATE INDEX account_tx_time ON account_tx (federation_id, timestamp);

-- Drill-down from a folded `account_tx` row to the underlying fedimint tx(s) and
-- their role. A withdraw contributes its withdrawal leg and (if paired) its
-- unlock leg; deposits and transfers contribute a single leg. Mirrors
-- `public.user_transaction_txs`.
CREATE MATERIALIZED VIEW account_tx_legs AS
SELECT federation_id, tx_key, primary_txid AS txid,
       CASE
           WHEN kind = 'withdraw' AND amount_msat IS NOT NULL THEN 'withdrawal'
           WHEN kind = 'withdraw' AND amount_msat IS NULL THEN 'unlock'
           WHEN kind IN ('transfer_in', 'transfer_out') THEN 'transfer'
           ELSE 'deposit'
       END AS role
FROM account_tx
UNION ALL
SELECT federation_id, tx_key, secondary_txid AS txid, 'unlock' AS role
FROM account_tx
WHERE secondary_txid IS NOT NULL
WITH DATA;
CREATE UNIQUE INDEX account_tx_legs_pk ON account_tx_legs (federation_id, tx_key, txid, role);

-- Per-account rollup — the "account totals". NET FLOWS from consensus, exact in
-- both msats and fiat (valued at each leg's cycle price). For a SEEKER,
-- `fiat_net` ≈ the current stabilized fiat balance (seeks preserve fiat value).
-- For a PROVIDER it is capital contributed, NOT a live balance: providers take
-- BTC price exposure and earn fees, both settled in unobservable guardian state.
CREATE MATERIALIZED VIEW account_totals AS
SELECT a.federation_id, a.account_id,
       COALESCE(SUM(a.amount_msat) FILTER (WHERE a.kind LIKE 'deposit%'), 0)::bigint  AS msat_deposited,
       COALESCE(SUM(a.amount_msat) FILTER (WHERE a.kind = 'withdraw'), 0)::bigint     AS msat_withdrawn,
       (COALESCE(SUM(a.amount_msat) FILTER (WHERE a.kind LIKE 'deposit%'), 0)
           - COALESCE(SUM(a.amount_msat) FILTER (WHERE a.kind = 'withdraw'), 0))::bigint AS msat_net,
       COALESCE(SUM(a.fiat_amount) FILTER (WHERE a.kind LIKE 'deposit%'), 0)::bigint  AS fiat_deposited,
       COALESCE(SUM(a.fiat_amount) FILTER (WHERE a.kind = 'withdraw'), 0)::bigint     AS fiat_withdrawn,
       (COALESCE(SUM(a.fiat_amount) FILTER (WHERE a.kind LIKE 'deposit%'), 0)
           - COALESCE(SUM(a.fiat_amount) FILTER (WHERE a.kind = 'withdraw'), 0))::bigint AS fiat_net,
       COALESCE(SUM(a.fiat_amount) FILTER (WHERE a.kind = 'transfer_in'), 0)::bigint  AS transfers_in_fiat,
       COALESCE(SUM(a.fiat_amount) FILTER (WHERE a.kind = 'transfer_out'), 0)::bigint AS transfers_out_fiat,
       COUNT(*)                                                              AS tx_count,
       MIN(a.session_index)                                                  AS first_session,
       MAX(a.session_index)                                                  AS last_session,
       MIN(a.timestamp)                                                      AS first_seen,
       MAX(a.timestamp)                                                      AS last_seen,
       m.acc_type,
       COALESCE(m.n_keys > 1, false)                                         AS is_multisig,
       m.threshold,
       m.n_keys
FROM account_tx a
         LEFT JOIN account_multisig m USING (federation_id, account_id)
GROUP BY a.federation_id, a.account_id, m.acc_type, m.threshold, m.n_keys
WITH DATA;
CREATE UNIQUE INDEX account_totals_pk ON account_totals (federation_id, account_id);

-- Convenience dimension view over the rollup (no separate refresh needed).
CREATE VIEW accounts AS
SELECT federation_id, account_id, acc_type, is_multisig, threshold, n_keys,
       first_seen, last_seen, tx_count
FROM account_totals;

-- Aggregated transfer graph: one edge per (from, to) pair, for multispend flow
-- analysis. Raw per-transfer detail stays in `transfers`.
CREATE MATERIALIZED VIEW transfer_edges AS
SELECT tr.federation_id, tr.from_account_id, tr.to_account_id,
       SUM(tr.transfer_fiat)::bigint AS total_fiat,
       COUNT(*)                      AS n,
       MIN(t.session_index)   AS first_session,
       MAX(t.session_index)   AS last_session
FROM transfers tr
         JOIN public.transactions t USING (federation_id, txid)
GROUP BY tr.federation_id, tr.from_account_id, tr.to_account_id
WITH DATA;
CREATE UNIQUE INDEX transfer_edges_pk
    ON transfer_edges (federation_id, from_account_id, to_account_id);

-- Daily rollup by kind/direction, for dashboards. Mirrors
-- `public.user_tx_daily`. Rows with an unknown timestamp are excluded.
CREATE MATERIALIZED VIEW sp_daily AS
SELECT federation_id, DATE(timestamp) AS day, kind, direction,
       COUNT(*)                                AS tx_count,
       COALESCE(SUM(amount_msat), 0)::bigint   AS sum_msat,
       COALESCE(SUM(fiat_amount), 0)::bigint   AS sum_fiat
FROM account_tx
WHERE timestamp IS NOT NULL
GROUP BY federation_id, DATE(timestamp), kind, direction
WITH DATA;
CREATE UNIQUE INDEX sp_daily_pk ON sp_daily (federation_id, day, kind, direction);

-- Net contributed flow per cycle and its running total — an approximate
-- "net contributed TVL" curve in msats and fiat. NOT true locked/staged/idle
-- pool size (that lives in unobservable guardian state); it is the cumulative
-- deposits − withdrawals actually seen on the ledger.
CREATE MATERIALIZED VIEW pool_flows AS
WITH per_cycle AS (
    SELECT federation_id, cycle_index,
           (SUM(CASE WHEN kind LIKE 'deposit%' THEN amount_msat ELSE 0 END)
               - SUM(CASE WHEN kind = 'withdraw' THEN COALESCE(amount_msat, 0) ELSE 0 END))::bigint AS net_msat,
           (SUM(CASE WHEN kind LIKE 'deposit%' THEN fiat_amount ELSE 0 END)
               - SUM(CASE WHEN kind = 'withdraw' THEN COALESCE(fiat_amount, 0) ELSE 0 END))::bigint AS net_fiat
    FROM account_tx
    WHERE cycle_index IS NOT NULL
    GROUP BY federation_id, cycle_index
)
SELECT federation_id, cycle_index, net_msat, net_fiat,
       SUM(net_msat) OVER (PARTITION BY federation_id ORDER BY cycle_index)::bigint AS cumulative_msat,
       SUM(net_fiat) OVER (PARTITION BY federation_id ORDER BY cycle_index)::bigint AS cumulative_fiat
FROM per_cycle
WITH DATA;
CREATE UNIQUE INDEX pool_flows_pk ON pool_flows (federation_id, cycle_index);
