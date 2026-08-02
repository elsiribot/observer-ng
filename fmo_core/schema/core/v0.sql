CREATE TABLE core_schema_version (version INTEGER PRIMARY KEY);

CREATE TABLE federations
(
    federation_id BYTEA PRIMARY KEY NOT NULL,
    config        BYTEA             NOT NULL
);

-- Bronze layer: raw session data, append-only. Fetch cursor = MAX(session_index)+1.
CREATE TABLE sessions
(
    federation_id BYTEA   NOT NULL REFERENCES federations (federation_id),
    session_index INTEGER NOT NULL,
    data          BYTEA   NOT NULL,
    PRIMARY KEY (federation_id, session_index)
);

-- Structural silver layer: module-agnostic facts filled at ingest time.
-- amount_msat/details stay NULL until the owning module processed the item.
CREATE TABLE transactions
(
    federation_id BYTEA   NOT NULL REFERENCES federations (federation_id),
    txid          BYTEA   NOT NULL,
    session_index INTEGER NOT NULL,
    item_index    INTEGER NOT NULL,
    data          BYTEA   NOT NULL,
    PRIMARY KEY (federation_id, txid),
    FOREIGN KEY (federation_id, session_index) REFERENCES sessions (federation_id, session_index)
);
CREATE INDEX transactions_by_session ON transactions (federation_id, session_index);

CREATE TABLE transaction_inputs
(
    federation_id BYTEA   NOT NULL REFERENCES federations (federation_id),
    txid          BYTEA   NOT NULL,
    in_index      INTEGER NOT NULL,
    kind          TEXT    NOT NULL,
    amount_msat   BIGINT,
    details       JSONB,
    PRIMARY KEY (federation_id, txid, in_index),
    FOREIGN KEY (federation_id, txid) REFERENCES transactions (federation_id, txid)
);
CREATE INDEX transaction_inputs_by_kind ON transaction_inputs (federation_id, kind);
CREATE INDEX transaction_inputs_mint_nonce ON transaction_inputs ((details -> 'V0' -> 'note' ->> 'nonce'))
    WHERE kind = 'mint';

CREATE TABLE transaction_outputs
(
    federation_id BYTEA   NOT NULL REFERENCES federations (federation_id),
    txid          BYTEA   NOT NULL,
    out_index     INTEGER NOT NULL,
    kind          TEXT    NOT NULL,
    amount_msat   BIGINT,
    details       JSONB,
    PRIMARY KEY (federation_id, txid, out_index),
    FOREIGN KEY (federation_id, txid) REFERENCES transactions (federation_id, txid)
);
CREATE INDEX transaction_outputs_by_kind ON transaction_outputs (federation_id, kind);

CREATE TABLE consensus_items
(
    federation_id BYTEA   NOT NULL REFERENCES federations (federation_id),
    session_index INTEGER NOT NULL,
    item_index    INTEGER NOT NULL,
    peer_id       INTEGER NOT NULL,
    kind          TEXT    NOT NULL,
    details       JSONB,
    PRIMARY KEY (federation_id, session_index, item_index),
    FOREIGN KEY (federation_id, session_index) REFERENCES sessions (federation_id, session_index)
);
CREATE INDEX consensus_items_by_kind ON consensus_items (federation_id, kind);

-- Per-module processing cursor: next session index each module still has to process.
CREATE TABLE module_progress
(
    module_kind        TEXT    NOT NULL,
    federation_id      BYTEA   NOT NULL REFERENCES federations (federation_id),
    next_session_index INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (module_kind, federation_id)
);

-- Installed module versions; a version bump drops the module schema and replays.
CREATE TABLE module_versions
(
    module_kind    TEXT PRIMARY KEY,
    module_version INTEGER NOT NULL
);

-- Modules contribute session timestamp estimates here (e.g. wallet block height
-- votes resolved via block_times, lnv2 unix time votes). Core aggregates them
-- into the session_times materialized view.
CREATE TABLE session_time_votes
(
    federation_id BYTEA     NOT NULL REFERENCES federations (federation_id),
    session_index INTEGER   NOT NULL,
    source_kind   TEXT      NOT NULL,
    peer_id       INTEGER   NOT NULL,
    timestamp     TIMESTAMP NOT NULL,
    PRIMARY KEY (federation_id, session_index, source_kind, peer_id)
);

-- Core services ------------------------------------------------------------

CREATE TABLE block_times
(
    block_height INTEGER PRIMARY KEY,
    timestamp    TIMESTAMP NOT NULL
);
CREATE INDEX block_times_time ON block_times (timestamp);

CREATE TABLE guardian_health
(
    federation_id BYTEA     NOT NULL REFERENCES federations (federation_id),
    time          TIMESTAMP NOT NULL,
    guardian_id   INTEGER   NOT NULL,
    status        JSONB,
    block_height  INTEGER,
    latency_ms    INTEGER,
    PRIMARY KEY (federation_id, guardian_id, time)
);
CREATE INDEX guardian_health_federation_time ON guardian_health (federation_id, time);

CREATE OR REPLACE VIEW latest_guardian_health AS
WITH latest_federation_times AS (
    SELECT federation_id,
           MAX(time) AS latest_time
    FROM guardian_health
    GROUP BY federation_id
)
SELECT gh.federation_id,
       gh.time,
       gh.guardian_id,
       gh.status,
       gh.block_height,
       gh.latency_ms
FROM guardian_health gh
         INNER JOIN
     latest_federation_times lft
     ON gh.federation_id = lft.federation_id AND gh.time = lft.latest_time;

CREATE TABLE nostr_votes
(
    event_id      BYTEA NOT NULL PRIMARY KEY,
    federation_id BYTEA NOT NULL REFERENCES federations (federation_id),
    star_vote     INTEGER,
    event         JSONB NOT NULL,
    fetch_time    TIMESTAMP NOT NULL
);
CREATE INDEX nostr_votes_federation ON nostr_votes (federation_id);
CREATE INDEX nostr_votes_fetch_time ON nostr_votes (fetch_time);

CREATE TABLE nostr_relays
(
    relay_url TEXT NOT NULL PRIMARY KEY
);
INSERT INTO nostr_relays (relay_url)
VALUES ('wss://relay.damus.io'),
       ('wss://nostr.bitcoiner.social/'),
       ('wss://relay.nostr.info/'),
       ('wss://nostr-01.bolt.observer/'),
       ('wss://nostr.mutinywallet.com/'),
       ('wss://relay.snort.social/'),
       ('wss://relay.primal.net/'),
       ('wss://relay.satoshidnc.com/'),
       ('wss://nos.lol/'),
       ('wss://nostr-pub.wellorder.net/')
ON CONFLICT DO NOTHING;

CREATE TABLE nostr_federations
(
    event_id      BYTEA PRIMARY KEY,
    federation_id BYTEA     NOT NULL,
    invite_code   TEXT      NOT NULL,
    event         JSONB     NOT NULL,
    fetch_time    TIMESTAMP NOT NULL
);

-- Session timestamps aggregated from module-contributed votes, forward-filled
-- so sessions without votes inherit the previous known timestamp.
CREATE MATERIALIZED VIEW session_times AS
WITH votes AS (
    SELECT federation_id, session_index, MAX(timestamp) AS ts
    FROM session_time_votes
    GROUP BY federation_id, session_index
),
all_sessions AS (
    SELECT s.federation_id, s.session_index, v.ts
    FROM sessions s
             LEFT JOIN votes v USING (federation_id, session_index)
),
grouped AS (
    SELECT *,
           SUM(CASE WHEN ts IS NOT NULL THEN 1 ELSE 0 END)
               OVER (PARTITION BY federation_id ORDER BY session_index) AS grp
    FROM all_sessions
)
SELECT federation_id,
       session_index,
       FIRST_VALUE(ts) OVER (PARTITION BY federation_id, grp ORDER BY session_index)
           AS estimated_session_timestamp
FROM grouped;
CREATE UNIQUE INDEX session_times_pk ON session_times (federation_id, session_index);
CREATE INDEX session_times_ts ON session_times (federation_id, estimated_session_timestamp);
