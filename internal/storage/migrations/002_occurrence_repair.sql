-- Repair migration for databases created by 04791af.
ALTER TABLE occurrences ADD COLUMN IF NOT EXISTS payload_raw BYTEA;
UPDATE occurrences SET payload_raw = convert_to(payload::text, 'UTF8') WHERE payload_raw IS NULL;
ALTER TABLE occurrences ALTER COLUMN payload_raw SET NOT NULL;
ALTER TABLE occurrences DROP CONSTRAINT IF EXISTS occurrences_source_stream_position_key;

ALTER TABLE occurrences ADD COLUMN IF NOT EXISTS payload_raw_reconstructed BOOLEAN;
UPDATE occurrences SET payload_raw_reconstructed = TRUE WHERE payload_raw_reconstructed IS NULL;
ALTER TABLE occurrences ALTER COLUMN payload_raw_reconstructed SET DEFAULT FALSE;
ALTER TABLE occurrences ALTER COLUMN payload_raw_reconstructed SET NOT NULL;
