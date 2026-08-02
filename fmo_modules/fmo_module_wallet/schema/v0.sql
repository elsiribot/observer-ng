CREATE TABLE peg_ins
(
    on_chain_txid BYTEA   NOT NULL,
    on_chain_vout INTEGER NOT NULL,
    address       TEXT    NOT NULL,
    amount_msat   BIGINT  NOT NULL,
    federation_id BYTEA   NOT NULL REFERENCES public.federations (federation_id),
    txid          BYTEA   NOT NULL,
    in_index      INTEGER NOT NULL,
    PRIMARY KEY (on_chain_txid, on_chain_vout),
    FOREIGN KEY (federation_id, txid, in_index)
        REFERENCES public.transaction_inputs (federation_id, txid, in_index)
);
CREATE INDEX federation_peg_ins ON peg_ins (federation_id);

CREATE TABLE withdrawal_addresses
(
    address       TEXT    NOT NULL,
    federation_id BYTEA   NOT NULL REFERENCES public.federations (federation_id),
    session_index INTEGER NOT NULL,
    item_index    INTEGER NOT NULL,
    txid          BYTEA   NOT NULL,
    out_index     INTEGER NOT NULL,
    PRIMARY KEY (address, txid),
    FOREIGN KEY (federation_id, txid, out_index)
        REFERENCES public.transaction_outputs (federation_id, txid, out_index)
);
CREATE INDEX federation_withdrawal_addresses ON withdrawal_addresses (federation_id);

CREATE TABLE withdrawal_transactions
(
    on_chain_txid   BYTEA PRIMARY KEY,
    federation_id   BYTEA NOT NULL REFERENCES public.federations (federation_id),
    -- unknowable until we observe the on_chain_txid on an explorer
    federation_txid BYTEA
);

CREATE TABLE withdrawal_signatures
(
    on_chain_txid BYTEA   NOT NULL REFERENCES withdrawal_transactions (on_chain_txid),
    session_index INTEGER NOT NULL,
    item_index    INTEGER NOT NULL,
    peer_id       INTEGER NOT NULL,
    PRIMARY KEY (on_chain_txid, peer_id)
);

CREATE TABLE withdrawal_transaction_inputs
(
    previous_output_txid BYTEA   NOT NULL,
    previous_output_vout INTEGER NOT NULL,
    on_chain_txid        BYTEA   NOT NULL REFERENCES withdrawal_transactions (on_chain_txid),
    PRIMARY KEY (previous_output_txid, previous_output_vout)
);

CREATE TABLE withdrawal_transaction_outputs
(
    on_chain_txid BYTEA   NOT NULL REFERENCES withdrawal_transactions (on_chain_txid),
    on_chain_vout INTEGER NOT NULL,
    address       TEXT    NOT NULL,
    amount_msat   BIGINT  NOT NULL,
    PRIMARY KEY (on_chain_txid, on_chain_vout)
);

-- Consensus items contributing block height votes; also feeds the core
-- session_time_votes table via the block_times service.
CREATE TABLE block_height_votes
(
    federation_id BYTEA   NOT NULL REFERENCES public.federations (federation_id),
    session_index INTEGER NOT NULL,
    item_index    INTEGER NOT NULL,
    proposer      INTEGER NOT NULL,
    height_vote   INTEGER NOT NULL,
    PRIMARY KEY (federation_id, session_index, item_index)
);
CREATE INDEX block_height_vote_federation_sessions ON block_height_votes (federation_id, session_index);
CREATE INDEX block_height_vote_heights ON block_height_votes (height_vote);

CREATE MATERIALIZED VIEW utxos AS
WITH unspent_deposits AS (
    SELECT wpi.on_chain_txid, wpi.on_chain_vout, wpi.address, wpi.amount_msat, wpi.federation_id
    FROM peg_ins wpi
    WHERE NOT EXISTS (
        SELECT *
        FROM withdrawal_transaction_inputs wwti
        WHERE wpi.on_chain_txid = wwti.previous_output_txid
          AND wpi.on_chain_vout = wwti.previous_output_vout
    )
),
unspent_change AS (
    SELECT wwto.on_chain_txid, wwto.on_chain_vout, wwto.address, wwto.amount_msat, wwt.federation_id
    FROM withdrawal_transaction_outputs wwto
        JOIN withdrawal_transactions wwt ON wwto.on_chain_txid = wwt.on_chain_txid
    WHERE NOT EXISTS (
        SELECT *
        FROM withdrawal_transaction_inputs wwti
        WHERE wwto.on_chain_txid = wwti.previous_output_txid
          AND wwto.on_chain_vout = wwti.previous_output_vout
    )
    AND NOT EXISTS (
        SELECT *
        FROM withdrawal_addresses wwa
        WHERE wwto.address = wwa.address
    )
)
SELECT ud.on_chain_txid, ud.on_chain_vout, ud.address, ud.amount_msat, ud.federation_id
FROM unspent_deposits ud
UNION
SELECT uc.on_chain_txid, uc.on_chain_vout, uc.address, uc.amount_msat, uc.federation_id
FROM unspent_change uc;

CREATE UNIQUE INDEX on_chain_txid_on_chain_vout ON utxos (on_chain_txid, on_chain_vout);
