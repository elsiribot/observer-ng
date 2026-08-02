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
