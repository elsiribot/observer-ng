-- Per-payment status moved into the LN modules (fmo_ln/fmo_lnv2.contracts.status),
-- which own the lifecycle. Gold no longer carries or derives it.
DROP MATERIALIZED VIEW IF EXISTS user_tx_daily;

ALTER TABLE user_transactions DROP COLUMN IF EXISTS status;

CREATE MATERIALIZED VIEW user_tx_daily AS
SELECT federation_id,
       (first_timestamp AT TIME ZONE 'UTC')::date AS day,
       kind, direction,
       COUNT(*)                                    AS tx_count,
       COALESCE(SUM(amount_msat), 0)               AS volume_msat,
       COALESCE(SUM(fedimint_fee_msat), 0)         AS fedimint_fee_msat,
       COALESCE(SUM(gateway_fee_estimate_msat), 0) AS gateway_fee_estimate_msat
FROM user_transactions
WHERE first_timestamp IS NOT NULL
GROUP BY federation_id, day, kind, direction;
CREATE UNIQUE INDEX user_tx_daily_pk
    ON user_tx_daily (federation_id, day, kind, direction);
