// Package domain holds the shared types and sentinel errors for Groundhog Guard.
package domain

import (
	"errors"
	"time"
)

// Event types carried on the "transfers" topic.
const (
	EventTransferRequested = "TransferRequested"
	TopicTransfers         = "transfers"
	TopicDLQ               = "transfers-dlq"
)

// PoisonReference is the demo mechanic: any event whose reference equals
// this value makes the processor fail with a retryable error every time,
// simulating a downstream dependency that is hard-down. The DLQ replay
// endpoint "fixes" the event by rewriting the reference (see service.ReplayDLQ).
const PoisonReference = "POISON"

// FixedReference is what PoisonReference is rewritten to on replay.
const FixedReference = "POISON-FIXED"

// Transfer statuses.
const (
	StatusPending                 = "pending"
	StatusApplied                 = "applied"
	StatusFailedInsufficientFunds = "failed_insufficient_funds"
	StatusDeadLettered            = "dead_lettered"
)

// Event is the payload published to the transfers topic. EventID is the
// unit of idempotency for the consumer: it is generated once, when the
// transfer is created, and every duplicate delivery carries the same ID.
type Event struct {
	EventID     string    `json:"event_id"`
	TransferID  string    `json:"transfer_id"`
	Type        string    `json:"type"`
	FromAccount string    `json:"from_account"`
	ToAccount   string    `json:"to_account"`
	AmountCents int64     `json:"amount_cents"`
	Reference   string    `json:"reference"`
	OccurredAt  time.Time `json:"occurred_at"`
}

// Account is a ledger account. Money is integer cents everywhere.
type Account struct {
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	BalanceCents int64     `json:"balance_cents"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// Transfer is one requested movement of money, created by the ingest API.
type Transfer struct {
	ID             string    `json:"id"`
	IdempotencyKey string    `json:"idempotency_key"`
	FromAccount    string    `json:"from_account"`
	ToAccount      string    `json:"to_account"`
	AmountCents    int64     `json:"amount_cents"`
	Reference      string    `json:"reference"`
	Status         string    `json:"status"`
	EventID        string    `json:"event_id,omitempty"` // joined from outbox
	CreatedAt      time.Time `json:"created_at"`
}

// OutboxRow is one row of the transactional outbox.
type OutboxRow struct {
	ID         int64
	EventID    string
	TransferID string
	Topic      string
	Key        string
	Payload    []byte // the serialised Event
}

// DLQEntry is one dead-lettered event.
type DLQEntry struct {
	ID         int64      `json:"id"`
	EventID    string     `json:"event_id"`
	TransferID string     `json:"transfer_id,omitempty"`
	Payload    Event      `json:"payload"`
	Error      string     `json:"error"`
	Attempts   int        `json:"attempts"`
	CreatedAt  time.Time  `json:"created_at"`
	ReplayedAt *time.Time `json:"replayed_at,omitempty"`
}

// Stats is the cross-process view served by GET /stats. The counters are
// kept in Postgres (stats_counters) precisely so the API process can report
// numbers produced by the consumer process.
type Stats struct {
	EventsReceived    int64 `json:"events_received"`
	DuplicatesDropped int64 `json:"duplicates_dropped"`
	TransfersApplied  int64 `json:"transfers_applied"`
	DLQCount          int64 `json:"dlq_count"`
	OutboxPending     int64 `json:"outbox_pending"`
}

// Outcome is the result of processing one event delivery.
type Outcome int

const (
	// OutcomeApplied — the transfer was applied exactly once (by this delivery).
	OutcomeApplied Outcome = iota
	// OutcomeDuplicate — this delivery was a duplicate; nothing was applied.
	OutcomeDuplicate
	// OutcomeDeadLettered — the event failed permanently (or exhausted
	// retries) and was written to the DLQ.
	OutcomeDeadLettered
)

func (o Outcome) String() string {
	switch o {
	case OutcomeApplied:
		return "applied"
	case OutcomeDuplicate:
		return "duplicate"
	case OutcomeDeadLettered:
		return "dead_lettered"
	default:
		return "unknown"
	}
}

// Sentinel errors. ErrPoison is deliberately retryable; the rest of the
// processing errors are classified by the helpers below.
var (
	ErrNotFound          = errors.New("not found")
	ErrConflict          = errors.New("idempotency key already used with a different payload")
	ErrInsufficientFunds = errors.New("insufficient funds")
	ErrInvalidEvent      = errors.New("invalid event")
	ErrPoison            = errors.New("poison event: downstream dependency always fails (simulated)")
	ErrAlreadyReplayed   = errors.New("dlq entry already replayed")
)

// IsTransient reports whether err is worth retrying. Only simulated
// downstream failures (poison) and explicitly wrapped transient errors are.
func IsTransient(err error) bool {
	return errors.Is(err, ErrPoison)
}
