// Package store defines the persistence contract for OnceOnly and two
// implementations: Postgres (production) and an in-memory store whose
// semantics mirror the SQL exactly (used by the unit tests).
package store

import (
	"context"

	"github.com/Sravanthipanga/onceonly/internal/domain"
)

// Counter names kept in stats_counters.
const (
	CounterEventsReceived    = "events_received"
	CounterDuplicatesDropped = "duplicates_dropped"
)

// CreateTransferRequest is the input to the atomic create-transfer operation.
type CreateTransferRequest struct {
	IdempotencyKey string
	FromAccount    string
	ToAccount      string
	AmountCents    int64
	Reference      string
}

// CreateTransferResult reports whether the transfer is new or an
// idempotent replay of an earlier submission.
type CreateTransferResult struct {
	Transfer domain.Transfer
	Event    domain.Event // the outbox event created with the transfer
	Replay   bool         // true if IdempotencyKey was already used with the same payload
}

// Store is the full persistence contract. The Postgres implementation runs
// every multi-row operation in a single transaction; the in-memory
// implementation serialises with a mutex to mirror that atomicity.
type Store interface {
	// Migrate applies the embedded SQL migrations.
	Migrate(ctx context.Context) error

	// EnsureAccount inserts the account if absent (by name) and returns it.
	EnsureAccount(ctx context.Context, name string, openingBalanceCents int64) (domain.Account, error)
	GetAccount(ctx context.Context, id string) (domain.Account, error)
	ListAccounts(ctx context.Context) ([]domain.Account, error)
	ListTransfersByAccount(ctx context.Context, accountID string, limit int) ([]domain.Transfer, error)

	// CreateTransfer inserts the transfer and its outbox event atomically.
	// If the idempotency key exists with the same payload it returns the
	// original with Replay=true; with a different payload it returns
	// domain.ErrConflict.
	CreateTransfer(ctx context.Context, req CreateTransferRequest) (CreateTransferResult, error)
	GetTransfer(ctx context.Context, id string) (domain.Transfer, error)

	// WithOutboxBatch locks up to limit unpublished outbox rows, invokes fn
	// with them, and — only if fn returns nil — marks them published and
	// commits. If fn fails or the process crashes mid-batch, nothing is
	// marked and the rows will be published again (at-least-once relay).
	WithOutboxBatch(ctx context.Context, limit int, fn func(rows []domain.OutboxRow) error) error
	OutboxPending(ctx context.Context) (int64, error)

	// ApplyEvent performs the consumer's exactly-once step in one
	// transaction: insert the processed_events marker, check funds, move
	// the money, update the transfer status. Returns one of:
	//   ApplyApplied            — money moved by this call
	//   ApplyDuplicate          — marker already existed; nothing changed
	//   ApplyInsufficientFunds  — marker inserted, DLQ row written in the
	//                             same transaction, no money moved
	ApplyEvent(ctx context.Context, ev domain.Event) (ApplyResult, error)

	// RecordDeadLetter idempotently records a dead-lettered event whose
	// failure happened outside the apply transaction (invalid payloads,
	// exhausted transient retries). It inserts the dlq row if absent —
	// dlq.event_id is UNIQUE, which is what makes this idempotent — and
	// updates the transfer status. It deliberately does NOT insert the
	// processed_events marker: for retry-exhausted events the consumer's
	// offset commit is what stops Kafka redelivery, and leaving the
	// marker out is what allows a later replay (same event_id) to apply.
	// (Insufficient-funds events are different: ApplyEvent inserts their
	// marker in-transaction, and PrepareReplay removes it again.)
	// Returns true if this call created the DLQ entry.
	RecordDeadLetter(ctx context.Context, ev domain.Event, errMsg string, attempts int) (bool, error)

	ListDLQ(ctx context.Context) ([]domain.DLQEntry, error)
	// PrepareReplay removes the processed marker for the event (giving it
	// a fresh chance) and returns the stored DLQ entry. It does not mark
	// the entry replayed — call MarkReplayed after a successful publish.
	PrepareReplay(ctx context.Context, eventID string) (domain.DLQEntry, error)
	MarkReplayed(ctx context.Context, eventID string) error

	IncrementCounter(ctx context.Context, name string, delta int64) error
	Stats(ctx context.Context) (domain.Stats, error)
}

// ApplyResult is the outcome of Store.ApplyEvent.
type ApplyResult int

const (
	ApplyApplied ApplyResult = iota
	ApplyDuplicate
	ApplyInsufficientFunds
)

func (r ApplyResult) String() string {
	switch r {
	case ApplyApplied:
		return "applied"
	case ApplyDuplicate:
		return "duplicate"
	case ApplyInsufficientFunds:
		return "insufficient_funds"
	default:
		return "unknown"
	}
}
