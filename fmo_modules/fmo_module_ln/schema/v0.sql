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
CREATE INDEX ln_contract_federation_contract ON contracts (federation_id, contract_id);
CREATE INDEX ln_contract_federation ON contracts (federation_id);
CREATE INDEX ln_contract_hashes ON contracts (payment_hash);

-- LN inputs spend a contract
CREATE TABLE input_contracts
(
    federation_id BYTEA   NOT NULL REFERENCES public.federations (federation_id),
    txid          BYTEA   NOT NULL,
    in_index      INTEGER NOT NULL,
    contract_id   BYTEA   NOT NULL,
    PRIMARY KEY (federation_id, txid, in_index),
    FOREIGN KEY (federation_id, txid, in_index)
        REFERENCES public.transaction_inputs (federation_id, txid, in_index)
);
-- Resolve "which input(s) spent this contract" without scanning a whole
-- federation's inputs (the PK starts with txid). Needed for spend/refund
-- lookups and stranded-contract analysis.
CREATE INDEX ln_input_contracts_by_contract ON input_contracts (federation_id, contract_id);

-- LN outputs interact with a contract: funding it, creating an offer or
-- cancelling an outgoing contract
CREATE TABLE output_contracts
(
    federation_id    BYTEA   NOT NULL REFERENCES public.federations (federation_id),
    txid             BYTEA   NOT NULL,
    out_index        INTEGER NOT NULL,
    interaction_kind TEXT    NOT NULL CHECK (interaction_kind IN ('fund', 'cancel', 'offer')),
    contract_id      BYTEA   NOT NULL,
    PRIMARY KEY (federation_id, txid, out_index),
    FOREIGN KEY (federation_id, txid, out_index)
        REFERENCES public.transaction_outputs (federation_id, txid, out_index)
);
CREATE INDEX ln_output_contracts_by_contract
    ON output_contracts (federation_id, contract_id, interaction_kind);

-- Gateway registrations announced to the federation's LN module, polled
-- periodically (ported from PR #109 by bansalayush247)
CREATE TABLE gateways
(
    federation_id   BYTEA       NOT NULL REFERENCES public.federations (federation_id),
    gateway_id      TEXT        NOT NULL,
    node_pub_key    TEXT        NOT NULL,
    api_endpoint    TEXT        NOT NULL,
    lightning_alias TEXT        NOT NULL,
    vetted          BOOLEAN     NOT NULL DEFAULT FALSE,
    raw             JSONB       NOT NULL,
    first_seen      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (federation_id, gateway_id)
);
CREATE INDEX gateways_federation_id ON gateways (federation_id);
CREATE INDEX gateways_node_pub_key ON gateways (node_pub_key);

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
CREATE INDEX gateway_poll_snapshots_fed_time
    ON gateway_poll_snapshots (federation_id, poll_time);

-- Preimage decryption shares from `DecryptPreimage` consensus items. Each
-- guardian contributes one share per incoming contract; once the federation's
-- threshold of shares agree, the preimage is decrypted and the contract
-- becomes spendable (claimed by the recipient on a valid preimage, or refunded
-- to the gateway on an invalid one). The valid-vs-invalid RESULT is not
-- recorded here: reproducing it needs the guardians' threshold `PublicKeySet`,
-- which is absent from the client config the observer downloads. What we can
-- record is decryption progress and timing.
CREATE TABLE decryption_shares
(
    federation_id BYTEA   NOT NULL REFERENCES public.federations (federation_id),
    contract_id   BYTEA   NOT NULL,
    peer_id       INTEGER NOT NULL,
    session_index INTEGER NOT NULL,
    item_index    INTEGER NOT NULL,
    -- one (first) share per guardian per contract
    PRIMARY KEY (federation_id, contract_id, peer_id)
);
CREATE INDEX ln_decryption_shares_contract ON decryption_shares (federation_id, contract_id);

-- Per incoming contract: how many distinct guardians contributed a decryption
-- share and whether that reached the federation's decryption threshold
-- (n - (n-1)/3, with n = number of guardians, derived from the distinct peers
-- that ever submit shares). `decrypted = true` means the preimage was
-- decrypted and the contract became spendable — independent of whether the
-- recipient/gateway actually swept it (a stranded-but-decrypted contract is a
-- recipient/gateway that never claimed; decrypted = false with funds locked is
-- a decryption that never completed).
CREATE MATERIALIZED VIEW contract_decryption AS
WITH guardians AS (
    SELECT federation_id, COUNT(DISTINCT peer_id) AS num_guardians
    FROM decryption_shares
    GROUP BY federation_id
),
shares AS (
    SELECT federation_id, contract_id,
           COUNT(*)           AS num_shares,
           MIN(session_index) AS first_share_session,
           MAX(session_index) AS last_share_session
    FROM decryption_shares
    GROUP BY federation_id, contract_id
)
SELECT s.federation_id,
       s.contract_id,
       s.num_shares,
       g.num_guardians,
       (g.num_guardians - (g.num_guardians - 1) / 3)              AS threshold,
       s.num_shares >= (g.num_guardians - (g.num_guardians - 1) / 3) AS decrypted,
       s.first_share_session,
       s.last_share_session
FROM shares s
JOIN guardians g USING (federation_id);
CREATE UNIQUE INDEX contract_decryption_pk ON contract_decryption (federation_id, contract_id);
