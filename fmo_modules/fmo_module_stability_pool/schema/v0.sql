-- Observer schema for the fedi `multi_sig_stability_pool` module.
--
-- The stability pool lets users deposit BTC as a "seek" (fiat-stabilized) or
-- "provide" (liquidity) position, and withdraw it again. In fedimint terms:
-- deposits are transaction OUTPUTS (value leaves the transaction into the pool)
-- and withdrawals are transaction INPUTS (value enters the transaction from the
-- pool). Account-to-account transfers and the first "unlock" step of a
-- withdrawal move no msats in the fedimint transaction (amount 0).

-- Deposits into the pool (fedimint transaction outputs).
CREATE TABLE deposits
(
    federation_id    BYTEA   NOT NULL REFERENCES public.federations (federation_id),
    txid             BYTEA   NOT NULL,
    out_index        INTEGER NOT NULL,
    -- Output enum version (0 = StabilityPoolOutputV0, 1 = StabilityPoolOutputV1).
    version          SMALLINT NOT NULL,
    action           TEXT    NOT NULL CHECK (action IN
                         ('deposit_to_seek', 'deposit_to_provide',
                          'transfer', 'deposit_to_btc_balance')),
    -- bech32m-encoded account id the deposit credits (sender for transfers).
    account_id       TEXT    NOT NULL,
    -- msats moved out of the fedimint transaction into the pool. 0 for transfers.
    amount_msat      BIGINT  NOT NULL,
    -- Minimum fee rate (parts-per-billion) for `deposit_to_provide` only.
    min_fee_rate_ppb BIGINT,
    PRIMARY KEY (federation_id, txid, out_index),
    FOREIGN KEY (federation_id, txid, out_index)
        REFERENCES public.transaction_outputs (federation_id, txid, out_index)
);
CREATE INDEX stability_pool_deposits_federation ON deposits (federation_id);
CREATE INDEX stability_pool_deposits_account ON deposits (federation_id, account_id);

-- Withdrawals from the pool (fedimint transaction inputs). A withdrawal is a
-- two-step process: `unlock_for_withdrawal` reserves funds (moves 0 msats and
-- carries a fiat-or-all target), then `withdrawal` actually pulls the msats.
CREATE TABLE withdrawals
(
    federation_id BYTEA   NOT NULL REFERENCES public.federations (federation_id),
    txid          BYTEA   NOT NULL,
    in_index      INTEGER NOT NULL,
    kind          TEXT    NOT NULL CHECK (kind IN
                      ('unlock_for_withdrawal', 'withdrawal')),
    -- bech32m-encoded account id being withdrawn from.
    account_id    TEXT    NOT NULL,
    -- msats entering the fedimint transaction from the pool. 0 for unlocks.
    amount_msat   BIGINT  NOT NULL,
    -- For `unlock_for_withdrawal` with a concrete fiat target: the fiat amount
    -- (in the currency's base unit, e.g. cents). NULL for withdrawals and for
    -- "unlock all" requests (see unlock_all).
    unlock_fiat   BIGINT,
    -- TRUE when an `unlock_for_withdrawal` targets the account's full balance.
    unlock_all    BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (federation_id, txid, in_index),
    FOREIGN KEY (federation_id, txid, in_index)
        REFERENCES public.transaction_inputs (federation_id, txid, in_index)
);
CREATE INDEX stability_pool_withdrawals_federation ON withdrawals (federation_id);
CREATE INDEX stability_pool_withdrawals_account ON withdrawals (federation_id, account_id);

-- Guardian cycle-turnover votes (StabilityPoolConsensusItemV0). Each guardian
-- proposes the next cycle index, its wall-clock time and the BTC/fiat price;
-- consensus takes the median. `vote_time` doubles as a session time estimate
-- and is contributed to the core session_time_votes table.
CREATE TABLE cycle_votes
(
    federation_id   BYTEA       NOT NULL REFERENCES public.federations (federation_id),
    session_index   INTEGER     NOT NULL,
    item_index      INTEGER     NOT NULL,
    proposer        INTEGER     NOT NULL,
    next_cycle_index BIGINT     NOT NULL,
    -- UTC wall-clock (naive), matching core session_time_votes.
    vote_time       TIMESTAMP   NOT NULL,
    -- Fiat price of 1 BTC at the cycle turnover, in the currency's base unit.
    price_fiat      BIGINT      NOT NULL,
    PRIMARY KEY (federation_id, session_index, item_index)
);
CREATE INDEX stability_pool_cycle_votes_federation_sessions
    ON cycle_votes (federation_id, session_index);
