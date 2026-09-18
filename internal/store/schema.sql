-- Access gate schema. Idempotent: applied at every service startup.
CREATE TABLE IF NOT EXISTS grants (
    id              TEXT PRIMARY KEY,
    receiver        TEXT NOT NULL,
    mode            TEXT NOT NULL CHECK (mode IN ('SHARED', 'EXCLUSIVE')),
    status          TEXT NOT NULL CHECK (status IN ('ACTIVE', 'RELEASED')),
    token_hash      TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    released_at     TIMESTAMPTZ
);

-- Hot path: count/scan active authorizations per receiver.
CREATE INDEX IF NOT EXISTS grants_receiver_status_idx
    ON grants (receiver, status, created_at);
