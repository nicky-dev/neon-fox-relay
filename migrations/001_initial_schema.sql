-- Initial schema for neon-fox-relay
-- PostgreSQL migration: create the events table with all required indexes

CREATE TABLE IF NOT EXISTS events (
    -- NIP-01 required fields
    id         TEXT        NOT NULL PRIMARY KEY,   -- 32-byte lowercase hex SHA256
    pubkey     TEXT        NOT NULL,               -- 32-byte lowercase hex public key
    created_at BIGINT      NOT NULL,               -- Unix timestamp in seconds
    kind       INTEGER     NOT NULL,               -- Event kind integer
    tags       JSONB       NOT NULL DEFAULT '[]',  -- Array of tag arrays
    content    TEXT        NOT NULL DEFAULT '',    -- Arbitrary string
    sig        TEXT        NOT NULL,               -- 64-byte lowercase hex Schnorr sig
    -- Bookkeeping
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Efficient lookups by author (sorted by time)
CREATE INDEX IF NOT EXISTS idx_events_pubkey_created_at
    ON events (pubkey, created_at DESC);

-- Efficient lookups by kind (sorted by time)
CREATE INDEX IF NOT EXISTS idx_events_kind_created_at
    ON events (kind, created_at DESC);

-- GIN index for fast tag queries (#e, #p, etc.)
CREATE INDEX IF NOT EXISTS idx_events_tags_gin
    ON events USING GIN (tags);

-- Replaceable events: unique constraint on (pubkey, kind) for kinds 0,3,10000–19999
-- Handled at application level with upsert logic.
-- Parameterized replaceable (NIP-33): unique on (pubkey, kind, d-tag)
-- Also handled at application level.
