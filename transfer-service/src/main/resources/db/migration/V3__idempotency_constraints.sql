-- INC-105: the idempotency key is the synchronization boundary, not a SELECT-then-INSERT race.
-- One key holds one request: the unique constraint is what makes that true under concurrent
-- duplicates. A mobile-app retry that arrives twice finds the same row, never two transfers.
ALTER TABLE idempotency_keys
    ADD CONSTRAINT idempotency_keys_pkey PRIMARY KEY (idempotency_key);

-- The request fingerprint is a 64 character SHA-256 hex string.
ALTER TABLE idempotency_keys
    ALTER COLUMN request_hash SET NOT NULL;

-- Reconciliation scans PENDING transfers; the index that makes that scan cheap.
CREATE INDEX IF NOT EXISTS transfers_pending_idx ON transfers (status, updated_at);
