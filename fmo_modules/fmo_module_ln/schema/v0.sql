CREATE TABLE contracts
(
    federation_id BYTEA NOT NULL REFERENCES public.federations (federation_id),
    contract_id   BYTEA NOT NULL,
    type          TEXT  NOT NULL CHECK (type IN ('incoming', 'outgoing')),
    payment_hash  BYTEA NOT NULL,
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
    PRIMARY KEY (federation_id, gateway_id, poll_time)
);
CREATE INDEX gateway_poll_snapshots_fed_time
    ON gateway_poll_snapshots (federation_id, poll_time);
