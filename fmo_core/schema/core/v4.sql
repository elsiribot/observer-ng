-- Open (unsigned) sessions are ingested live with NULL data; the authoritative
-- SessionOutcome bytes are written when the session signs (SP-2 live view).
-- The optional guardian signature is stored for provenance only (not verified).
ALTER TABLE sessions ALTER COLUMN data DROP NOT NULL;
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS signature BYTEA;
