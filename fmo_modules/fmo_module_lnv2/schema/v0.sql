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
CREATE INDEX lnv2_contracts_federation ON contracts (federation_id);

-- LNv2 inputs reference the funding outpoint of the contract they claim or
-- refund; amounts are not part of the input itself.
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

-- Gateway registry + reachability polling (fmo_core::gateway_poll harness).
-- LNv2's registry is thinner than LNv1's: gateway API URLs only, no
-- vetting/node-key/fees, and the URL string doubles as gateway_id.
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
