-- Time-resolved ecash in-circulation curve: one change point per
-- (federation, mint kind, denomination, session-with-activity), storing the
-- cumulative (issued - spent) pool AFTER that session. Read the latest row with
-- session_index < s to get the pool STRICTLY BEFORE a spend at session s.
-- v1 `mint` and v2 `mintv2` are disjoint pools, hence the `kind` column.
-- Populated by gold::rebuild_note_circulation (one-time heal) and extended
-- incrementally by gold::maintain_note_circulation.
CREATE TABLE note_circulation (
    federation_id     BYTEA   NOT NULL REFERENCES federations (federation_id),
    kind              TEXT    NOT NULL,
    denomination_msat BIGINT  NOT NULL,
    session_index     INTEGER NOT NULL,
    in_circulation    BIGINT  NOT NULL,
    PRIMARY KEY (federation_id, kind, denomination_msat, session_index)
);
-- Latest-change-point-before-session lookup.
CREATE INDEX note_circulation_lookup
    ON note_circulation (federation_id, kind, denomination_msat, session_index DESC);

-- Per-fedimint-transaction ecash anonymity-set estimate (bits): the crowd of
-- possible spenders = min over the transaction's spent (kind, denomination) of
-- log2(in-circulation pool strictly before the transaction's session). Keyed by
-- `txid` (the natural grain of an ecash spend), so it covers every fedimint
-- transaction that spends ecash notes uniformly -- ecash transfers, on-chain
-- withdrawals (peg-outs burn notes), and Lightning `fund` legs -- regardless of
-- how the gold layer later groups them (LN is contract_id-grained, so a
-- user_transactions-keyed score would miss it). A row exists only for scored
-- transactions; absence = not an ecash spend (or no pool data before it).
-- Written by gold::compute_ecash_anon_bits / gold::backfill_ecash_anon_bits.
CREATE TABLE transaction_privacy (
    federation_id   BYTEA            NOT NULL,
    txid            BYTEA            NOT NULL,
    ecash_anon_bits DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (federation_id, txid),
    FOREIGN KEY (federation_id, txid) REFERENCES transactions (federation_id, txid)
);
