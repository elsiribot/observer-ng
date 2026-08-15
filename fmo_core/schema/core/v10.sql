-- Issuance-side ecash anonymity estimate. `ecash_anon_bits` scores the crowd of
-- possible SPENDERS of a transaction's spent notes; `ecash_issuance_bits` scores
-- the crowd its freshly-MINTED notes are minted into (min over the transaction's
-- output denominations of log2(pool strictly before the tx's session)). The
-- issuance figure is forward-looking -- the note's realized anonymity happens at
-- its future spend -- so it is a weaker notion than the spend side, but it gives
-- peg-ins / receives (which spend no ecash) a privacy figure.
--
-- A transaction may spend, issue, or both, so either column can be NULL:
-- ecash_anon_bits becomes nullable (a pure peg-in issues but spends nothing).
-- Populated by gold::compute_transaction_privacy / gold::backfill_transaction_privacy.
ALTER TABLE transaction_privacy ALTER COLUMN ecash_anon_bits DROP NOT NULL;
ALTER TABLE transaction_privacy ADD COLUMN ecash_issuance_bits DOUBLE PRECISION;
