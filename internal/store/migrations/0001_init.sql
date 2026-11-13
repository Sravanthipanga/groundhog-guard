CREATE TABLE IF NOT EXISTS accounts (
    id            UUID PRIMARY KEY,
    name          TEXT NOT NULL UNIQUE,
    balance_cents BIGINT NOT NULL CHECK (balance_cents >= 0),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS transfers (
    id              UUID PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    request_hash    TEXT NOT NULL,
    from_account    UUID NOT NULL REFERENCES accounts(id),
    to_account      UUID NOT NULL REFERENCES accounts(id),
    amount_cents    BIGINT NOT NULL,
    reference       TEXT NOT NULL DEFAULT '',
    status          TEXT NOT NULL DEFAULT 'pending',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS outbox (
    id           BIGSERIAL PRIMARY KEY,
    event_id     UUID NOT NULL UNIQUE,
    transfer_id  UUID NOT NULL REFERENCES transfers(id),
    topic        TEXT NOT NULL,
    key          TEXT NOT NULL,
    payload      JSONB NOT NULL,
    published_at TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS outbox_unpublished_idx
    ON outbox (id) WHERE published_at IS NULL;

-- The heart of the idempotency guarantee: one row per applied event.
-- The consumer inserts this row in the SAME transaction that moves the
-- money, so "mark processed" and "apply" commit or roll back together.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     UUID PRIMARY KEY,
    transfer_id  UUID NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dlq (
    id          BIGSERIAL PRIMARY KEY,
    event_id    UUID NOT NULL UNIQUE,
    transfer_id UUID,
    payload     JSONB NOT NULL,
    error       TEXT NOT NULL,
    attempts    INT NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    replayed_at TIMESTAMPTZ
);

-- Cross-process counters so the API can report consumer-side numbers.
CREATE TABLE IF NOT EXISTS stats_counters (
    name  TEXT PRIMARY KEY,
    value BIGINT NOT NULL DEFAULT 0
);

INSERT INTO stats_counters (name, value) VALUES
    ('events_received', 0),
    ('duplicates_dropped', 0)
ON CONFLICT (name) DO NOTHING;
