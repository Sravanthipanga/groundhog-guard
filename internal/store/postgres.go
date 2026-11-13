package store

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Sravanthipanga/groundhog-guard/internal/domain"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// PostgresStore implements Store against PostgreSQL via pgx.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore connects to Postgres at dsn.
func NewPostgresStore(ctx context.Context, dsn string) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &PostgresStore{pool: pool}, nil
}

// Close releases the connection pool.
func (s *PostgresStore) Close() { s.pool.Close() }

// Pool exposes the underlying pool (used by tests and diagnostics).
func (s *PostgresStore) Pool() *pgxpool.Pool { return s.pool }

// Migrate applies embedded SQL migrations in filename order. Each file
// runs in its own transaction and is recorded in schema_migrations, so
// restarts are no-ops. Deliberately tiny — no external migration tool.
func (s *PostgresStore) Migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		name TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	for _, name := range names {
		var exists bool
		if err := s.pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE name = $1)`, name).Scan(&exists); err != nil {
			return err
		}
		if exists {
			continue
		}
		sqlBytes, err := migrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return err
		}
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, string(sqlBytes)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (name) VALUES ($1)`, name); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *PostgresStore) EnsureAccount(ctx context.Context, name string, openingBalanceCents int64) (domain.Account, error) {
	var acc domain.Account
	err := s.pool.QueryRow(ctx, `
		INSERT INTO accounts (id, name, balance_cents)
		VALUES ($1, $2, $3)
		ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
		RETURNING id, name, balance_cents, updated_at`,
		uuid.NewString(), name, openingBalanceCents).
		Scan(&acc.ID, &acc.Name, &acc.BalanceCents, &acc.UpdatedAt)
	if err != nil {
		return domain.Account{}, fmt.Errorf("ensure account: %w", err)
	}
	return acc, nil
}

func (s *PostgresStore) GetAccount(ctx context.Context, id string) (domain.Account, error) {
	var acc domain.Account
	err := s.pool.QueryRow(ctx,
		`SELECT id, name, balance_cents, updated_at FROM accounts WHERE id = $1`, id).
		Scan(&acc.ID, &acc.Name, &acc.BalanceCents, &acc.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Account{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.Account{}, err
	}
	return acc, nil
}

func (s *PostgresStore) ListAccounts(ctx context.Context) ([]domain.Account, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, name, balance_cents, updated_at FROM accounts ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Account
	for rows.Next() {
		var acc domain.Account
		if err := rows.Scan(&acc.ID, &acc.Name, &acc.BalanceCents, &acc.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, acc)
	}
	return out, rows.Err()
}

func (s *PostgresStore) ListTransfersByAccount(ctx context.Context, accountID string, limit int) ([]domain.Transfer, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, idempotency_key, from_account, to_account, amount_cents, reference, status, created_at
		FROM transfers
		WHERE from_account = $1 OR to_account = $1
		ORDER BY created_at DESC LIMIT $2`, accountID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Transfer
	for rows.Next() {
		var tr domain.Transfer
		if err := rows.Scan(&tr.ID, &tr.IdempotencyKey, &tr.FromAccount, &tr.ToAccount,
			&tr.AmountCents, &tr.Reference, &tr.Status, &tr.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, tr)
	}
	return out, rows.Err()
}

func (s *PostgresStore) CreateTransfer(ctx context.Context, req CreateTransferRequest) (CreateTransferResult, error) {
	// Accounts must exist (foreign keys would catch this too, but a
	// typed error is a better API response than a constraint failure).
	for _, id := range []string{req.FromAccount, req.ToAccount} {
		if _, err := s.GetAccount(ctx, id); err != nil {
			return CreateTransferResult{}, err
		}
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return CreateTransferResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tr := domain.Transfer{
		ID:             uuid.NewString(),
		IdempotencyKey: req.IdempotencyKey,
		FromAccount:    req.FromAccount,
		ToAccount:      req.ToAccount,
		AmountCents:    req.AmountCents,
		Reference:      req.Reference,
		Status:         domain.StatusPending,
	}
	var insertedID string
	err = tx.QueryRow(ctx, `
		INSERT INTO transfers (id, idempotency_key, request_hash, from_account, to_account, amount_cents, reference, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING id`,
		tr.ID, tr.IdempotencyKey, Fingerprint(req), tr.FromAccount, tr.ToAccount,
		tr.AmountCents, tr.Reference, tr.Status).Scan(&insertedID)

	if errors.Is(err, pgx.ErrNoRows) {
		// Key already used: replay (same payload) or conflict (different).
		var existing domain.Transfer
		var hash string
		if err := tx.QueryRow(ctx, `
			SELECT id, idempotency_key, request_hash, from_account, to_account, amount_cents, reference, status, created_at
			FROM transfers WHERE idempotency_key = $1`, req.IdempotencyKey).
			Scan(&existing.ID, &existing.IdempotencyKey, &hash, &existing.FromAccount, &existing.ToAccount,
				&existing.AmountCents, &existing.Reference, &existing.Status, &existing.CreatedAt); err != nil {
			return CreateTransferResult{}, err
		}
		if hash != Fingerprint(req) {
			return CreateTransferResult{}, domain.ErrConflict
		}
		var payload []byte
		if err := tx.QueryRow(ctx,
			`SELECT payload FROM outbox WHERE transfer_id = $1`, existing.ID).Scan(&payload); err != nil {
			return CreateTransferResult{}, err
		}
		var ev domain.Event
		if err := json.Unmarshal(payload, &ev); err != nil {
			return CreateTransferResult{}, err
		}
		existing.EventID = ev.EventID
		return CreateTransferResult{Transfer: existing, Event: ev, Replay: true}, nil
	}
	if err != nil {
		return CreateTransferResult{}, fmt.Errorf("insert transfer: %w", err)
	}

	ev := domain.Event{
		EventID:     uuid.NewString(),
		TransferID:  tr.ID,
		Type:        domain.EventTransferRequested,
		FromAccount: req.FromAccount,
		ToAccount:   req.ToAccount,
		AmountCents: req.AmountCents,
		Reference:   req.Reference,
		OccurredAt:  time.Now(),
	}
	payload, err := json.Marshal(ev)
	if err != nil {
		return CreateTransferResult{}, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO outbox (event_id, transfer_id, topic, key, payload)
		VALUES ($1, $2, $3, $4, $5)`,
		ev.EventID, tr.ID, domain.TopicTransfers, req.FromAccount, payload); err != nil {
		return CreateTransferResult{}, fmt.Errorf("insert outbox: %w", err)
	}
	if err := tx.QueryRow(ctx,
		`SELECT created_at FROM transfers WHERE id = $1`, tr.ID).Scan(&tr.CreatedAt); err != nil {
		return CreateTransferResult{}, err
	}
	tr.EventID = ev.EventID
	if err := tx.Commit(ctx); err != nil {
		return CreateTransferResult{}, err
	}
	return CreateTransferResult{Transfer: tr, Event: ev, Replay: false}, nil
}

func (s *PostgresStore) GetTransfer(ctx context.Context, id string) (domain.Transfer, error) {
	var tr domain.Transfer
	var eventID *string
	err := s.pool.QueryRow(ctx, `
		SELECT t.id, t.idempotency_key, t.from_account, t.to_account, t.amount_cents,
		       t.reference, t.status, t.created_at, o.event_id
		FROM transfers t
		LEFT JOIN outbox o ON o.transfer_id = t.id
		WHERE t.id = $1`, id).
		Scan(&tr.ID, &tr.IdempotencyKey, &tr.FromAccount, &tr.ToAccount, &tr.AmountCents,
			&tr.Reference, &tr.Status, &tr.CreatedAt, &eventID)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Transfer{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.Transfer{}, err
	}
	if eventID != nil {
		tr.EventID = *eventID
	}
	return tr, nil
}

func (s *PostgresStore) WithOutboxBatch(ctx context.Context, limit int, fn func(rows []domain.OutboxRow) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		SELECT id, event_id, transfer_id, topic, key, payload
		FROM outbox
		WHERE published_at IS NULL
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return err
	}
	var batch []domain.OutboxRow
	for rows.Next() {
		var r domain.OutboxRow
		if err := rows.Scan(&r.ID, &r.EventID, &r.TransferID, &r.Topic, &r.Key, &r.Payload); err != nil {
			rows.Close()
			return err
		}
		batch = append(batch, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if len(batch) == 0 {
		return tx.Commit(ctx)
	}

	// Publish while holding the row locks. A crash between the Kafka
	// publish and the commit below leaves the rows unpublished, so they
	// will be published again — that duplicate is intentional and is
	// exactly what the consumer's dedup gate exists to absorb.
	if err := fn(batch); err != nil {
		return err
	}

	ids := make([]int64, len(batch))
	for i, r := range batch {
		ids[i] = r.ID
	}
	if _, err := tx.Exec(ctx,
		`UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, ids); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *PostgresStore) OutboxPending(ctx context.Context) (int64, error) {
	var n int64
	err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&n)
	return n, err
}

// ApplyEvent is the exactly-once core. Everything happens in one
// transaction: the processed_events insert is the lock, the check and
// the receipt, all at once. If the insert conflicts, this delivery is a
// duplicate and the transaction rolls back having changed nothing.
func (s *PostgresStore) ApplyEvent(ctx context.Context, ev domain.Event) (ApplyResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var marker string
	err = tx.QueryRow(ctx, `
		INSERT INTO processed_events (event_id, transfer_id)
		VALUES ($1, $2)
		ON CONFLICT (event_id) DO NOTHING
		RETURNING event_id`, ev.EventID, ev.TransferID).Scan(&marker)
	if errors.Is(err, pgx.ErrNoRows) {
		return ApplyDuplicate, nil // rollback via defer; nothing changed
	}
	if err != nil {
		return 0, fmt.Errorf("insert processed marker: %w", err)
	}

	// Lock both account rows in id order to avoid deadlocks between
	// opposite-direction transfers.
	balances := map[string]int64{}
	rows, err := tx.Query(ctx, `
		SELECT id, balance_cents FROM accounts
		WHERE id IN ($1, $2)
		ORDER BY id
		FOR UPDATE`, ev.FromAccount, ev.ToAccount)
	if err != nil {
		return 0, err
	}
	for rows.Next() {
		var id string
		var bal int64
		if err := rows.Scan(&id, &bal); err != nil {
			rows.Close()
			return 0, err
		}
		balances[id] = bal
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	fromBal, okFrom := balances[ev.FromAccount]
	_, okTo := balances[ev.ToAccount]
	if !okFrom || !okTo {
		return 0, domain.ErrNotFound
	}

	if fromBal < ev.AmountCents {
		// Permanent failure, recorded atomically with the marker: the
		// transfer is final-failed and the event is in the DLQ. Because
		// the marker exists, any redelivery is dropped as a duplicate.
		if _, err := tx.Exec(ctx,
			`UPDATE transfers SET status = $1 WHERE id = $2`,
			domain.StatusFailedInsufficientFunds, ev.TransferID); err != nil {
			return 0, err
		}
		payload, _ := json.Marshal(ev)
		if _, err := tx.Exec(ctx, `
			INSERT INTO dlq (event_id, transfer_id, payload, error, attempts)
			VALUES ($1, $2, $3, $4, 1)
			ON CONFLICT (event_id) DO NOTHING`,
			ev.EventID, ev.TransferID, payload, domain.ErrInsufficientFunds.Error()); err != nil {
			return 0, err
		}
		if err := tx.Commit(ctx); err != nil {
			return 0, err
		}
		return ApplyInsufficientFunds, nil
	}

	if _, err := tx.Exec(ctx,
		`UPDATE accounts SET balance_cents = balance_cents - $1, updated_at = now() WHERE id = $2`,
		ev.AmountCents, ev.FromAccount); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE accounts SET balance_cents = balance_cents + $1, updated_at = now() WHERE id = $2`,
		ev.AmountCents, ev.ToAccount); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE transfers SET status = $1 WHERE id = $2`,
		domain.StatusApplied, ev.TransferID); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return ApplyApplied, nil
}

func (s *PostgresStore) RecordDeadLetter(ctx context.Context, ev domain.Event, errMsg string, attempts int) (bool, error) {
	payload, _ := json.Marshal(ev)
	// transfer_id is nullable: raw/garbage events may not carry one.
	var transferID any
	if ev.TransferID != "" {
		transferID = ev.TransferID
	}
	var id int64
	err := s.pool.QueryRow(ctx, `
		INSERT INTO dlq (event_id, transfer_id, payload, error, attempts)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (event_id) DO NOTHING
		RETURNING id`, ev.EventID, transferID, payload, errMsg, attempts).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil // already recorded
	}
	if err != nil {
		return false, err
	}
	if _, err := s.pool.Exec(ctx,
		`UPDATE transfers SET status = $1 WHERE id = $2 AND status = 'pending'`,
		domain.StatusDeadLettered, ev.TransferID); err != nil {
		return false, err
	}
	return true, nil
}

func (s *PostgresStore) ListDLQ(ctx context.Context) ([]domain.DLQEntry, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, event_id, transfer_id, payload, error, attempts, created_at, replayed_at
		FROM dlq ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.DLQEntry
	for rows.Next() {
		var e domain.DLQEntry
		var transferID *string
		var payload []byte
		if err := rows.Scan(&e.ID, &e.EventID, &transferID, &payload, &e.Error,
			&e.Attempts, &e.CreatedAt, &e.ReplayedAt); err != nil {
			return nil, err
		}
		if transferID != nil {
			e.TransferID = *transferID
		}
		if err := json.Unmarshal(payload, &e.Payload); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *PostgresStore) PrepareReplay(ctx context.Context, eventID string) (domain.DLQEntry, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return domain.DLQEntry{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var e domain.DLQEntry
	var transferID *string
	var payload []byte
	err = tx.QueryRow(ctx, `
		SELECT id, event_id, transfer_id, payload, error, attempts, created_at, replayed_at
		FROM dlq WHERE event_id = $1 FOR UPDATE`, eventID).
		Scan(&e.ID, &e.EventID, &transferID, &payload, &e.Error, &e.Attempts, &e.CreatedAt, &e.ReplayedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.DLQEntry{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.DLQEntry{}, err
	}
	if e.ReplayedAt != nil {
		return domain.DLQEntry{}, domain.ErrAlreadyReplayed
	}
	if transferID != nil {
		e.TransferID = *transferID
	}
	if err := json.Unmarshal(payload, &e.Payload); err != nil {
		return domain.DLQEntry{}, err
	}
	// Fresh chance: remove the processed marker (present when the event
	// was dead-lettered for insufficient funds inside ApplyEvent).
	if _, err := tx.Exec(ctx,
		`DELETE FROM processed_events WHERE event_id = $1`, eventID); err != nil {
		return domain.DLQEntry{}, err
	}
	// And reset the transfer to pending so its final status can move.
	if _, err := tx.Exec(ctx,
		`UPDATE transfers SET status = 'pending' WHERE id = $1`, e.TransferID); err != nil {
		return domain.DLQEntry{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.DLQEntry{}, err
	}
	return e, nil
}

func (s *PostgresStore) MarkReplayed(ctx context.Context, eventID string) error {
	ct, err := s.pool.Exec(ctx,
		`UPDATE dlq SET replayed_at = now() WHERE event_id = $1`, eventID)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return domain.ErrNotFound
	}
	return nil
}

func (s *PostgresStore) IncrementCounter(ctx context.Context, name string, delta int64) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO stats_counters (name, value) VALUES ($1, $2)
		ON CONFLICT (name) DO UPDATE SET value = stats_counters.value + EXCLUDED.value`,
		name, delta)
	return err
}

func (s *PostgresStore) Stats(ctx context.Context) (domain.Stats, error) {
	var st domain.Stats
	if err := s.pool.QueryRow(ctx,
		`SELECT COALESCE((SELECT value FROM stats_counters WHERE name = 'events_received'), 0)`).
		Scan(&st.EventsReceived); err != nil {
		return st, err
	}
	if err := s.pool.QueryRow(ctx,
		`SELECT COALESCE((SELECT value FROM stats_counters WHERE name = 'duplicates_dropped'), 0)`).
		Scan(&st.DuplicatesDropped); err != nil {
		return st, err
	}
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM transfers WHERE status = 'applied'`).Scan(&st.TransfersApplied); err != nil {
		return st, err
	}
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM dlq WHERE replayed_at IS NULL`).Scan(&st.DLQCount); err != nil {
		return st, err
	}
	pending, err := s.OutboxPending(ctx)
	if err != nil {
		return st, err
	}
	st.OutboxPending = pending
	return st, nil
}
