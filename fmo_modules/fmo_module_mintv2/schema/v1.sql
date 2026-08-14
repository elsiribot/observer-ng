-- Per-federation cumulative count of ecash notes by denomination.
--
-- Each mint OUTPUT mints exactly one note of `amount_msat` (issued); each mint
-- INPUT spends exactly one note of `amount_msat` (spent). This table is
-- maintained incrementally by the mintv2 observer module's process_output /
-- process_input (one +1 upsert per note), which rides the same exactly-once
-- transaction as the module cursor (see dispatch::process_module_batch), so no
-- periodic re-scan is needed.
--
-- `in_circulation` for a denomination is `issued - spent` (computed at read
-- time, clamped at >= 0).
CREATE TABLE note_denominations
(
    federation_id     BYTEA  NOT NULL REFERENCES public.federations (federation_id),
    denomination_msat BIGINT NOT NULL,
    issued            BIGINT NOT NULL DEFAULT 0,
    spent             BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (federation_id, denomination_msat)
);

-- One-time history backfill from the core structural tables, so notes minted /
-- spent before this table existed are counted without a module version bump or
-- full replay. This runs once, inside the migration, at startup -- before the
-- per-federation processors spawn -- so it counts everything the module has
-- already processed (present in the core tables up to the current cursor).
-- Incremental upserts then take over from the cursor forward: no gap, no
-- double-count. On a freshly imported/replayed database this simply counts zero
-- rows and every note is tallied incrementally instead.
INSERT INTO note_denominations (federation_id, denomination_msat, issued, spent)
SELECT COALESCE(o.federation_id, i.federation_id),
       COALESCE(o.denom, i.denom),
       COALESCE(o.n, 0),
       COALESCE(i.n, 0)
FROM (SELECT federation_id, amount_msat AS denom, COUNT(*) AS n
      FROM transaction_outputs
      WHERE kind = 'mintv2' AND amount_msat IS NOT NULL
      GROUP BY federation_id, amount_msat) o
FULL OUTER JOIN (SELECT federation_id, amount_msat AS denom, COUNT(*) AS n
                 FROM transaction_inputs
                 WHERE kind = 'mintv2' AND amount_msat IS NOT NULL
                 GROUP BY federation_id, amount_msat) i
  ON o.federation_id = i.federation_id AND o.denom = i.denom;
