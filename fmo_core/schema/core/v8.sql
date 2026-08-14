-- Tracks fedimint signed API-URL announcements. A guardian can rotate its API
-- endpoint by publishing a nonce-ordered, signature-verified announcement over
-- consensus (`api_announcements`), which clients use to override the base
-- config URL. The health monitor fetches and verifies these, records the
-- highest-nonce URL per guardian here, and polls the overridden URL — so a
-- guardian that moved endpoints is no longer shown as offline just because the
-- observer kept hitting its stale config URL.
CREATE TABLE guardian_api_announcements
(
    federation_id BYTEA     NOT NULL REFERENCES federations (federation_id),
    guardian_id   INTEGER   NOT NULL,
    api_url       TEXT      NOT NULL,
    nonce         BIGINT    NOT NULL,
    updated_at    TIMESTAMP NOT NULL,
    PRIMARY KEY (federation_id, guardian_id)
);
