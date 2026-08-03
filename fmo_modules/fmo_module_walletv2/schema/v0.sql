-- Peg-in claims: a walletv2 input claims a tracked federation on-chain output
-- by its index. The claimed value is not part of the input itself (the server
-- looks it up in its own database), so no amount is recorded here.
CREATE TABLE receives
(
    federation_id BYTEA   NOT NULL REFERENCES public.federations (federation_id),
    txid          BYTEA   NOT NULL,
    in_index      INTEGER NOT NULL,
    output_index  BIGINT  NOT NULL,
    tweak         BYTEA   NOT NULL,
    fee_msat      BIGINT  NOT NULL,
    PRIMARY KEY (federation_id, txid, in_index),
    FOREIGN KEY (federation_id, txid, in_index)
        REFERENCES public.transaction_inputs (federation_id, txid, in_index)
);
CREATE INDEX walletv2_receives_federation ON receives (federation_id);

-- Peg-outs: a walletv2 output sends `value` to an on-chain destination, with
-- an additional on-chain `fee` both debited from the fedimint transaction.
CREATE TABLE sends
(
    federation_id BYTEA   NOT NULL REFERENCES public.federations (federation_id),
    txid          BYTEA   NOT NULL,
    out_index     INTEGER NOT NULL,
    -- NULL if the destination script variant is unknown to this observer
    address       TEXT,
    value_msat    BIGINT  NOT NULL,
    fee_msat      BIGINT  NOT NULL,
    PRIMARY KEY (federation_id, txid, out_index),
    FOREIGN KEY (federation_id, txid, out_index)
        REFERENCES public.transaction_outputs (federation_id, txid, out_index)
);
CREATE INDEX walletv2_sends_federation ON sends (federation_id);

-- Consensus items contributing block count votes; also feeds the core
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
CREATE INDEX walletv2_block_height_vote_federation_sessions
    ON block_height_votes (federation_id, session_index);
CREATE INDEX walletv2_block_height_vote_heights ON block_height_votes (height_vote);
