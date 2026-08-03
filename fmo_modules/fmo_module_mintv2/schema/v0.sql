-- E-cash notes spent as transaction inputs, keyed by their unique nonce
-- (hex-encoded compressed public key, matching the JSON details encoding).
CREATE TABLE spent_nonces
(
    federation_id BYTEA    NOT NULL REFERENCES public.federations (federation_id),
    nonce         TEXT     NOT NULL,
    denomination  SMALLINT NOT NULL,
    amount_msat   BIGINT   NOT NULL,
    txid          BYTEA    NOT NULL,
    in_index      INTEGER  NOT NULL,
    PRIMARY KEY (federation_id, nonce),
    FOREIGN KEY (federation_id, txid, in_index)
        REFERENCES public.transaction_inputs (federation_id, txid, in_index)
);
CREATE INDEX mintv2_spent_nonces_federation ON spent_nonces (federation_id);
