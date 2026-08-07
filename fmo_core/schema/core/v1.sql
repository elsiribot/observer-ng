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
