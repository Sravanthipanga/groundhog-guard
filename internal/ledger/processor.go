// Package ledger is the consumer's brain: it decides, for one event
// delivery, whether the transfer is applied, dropped as a duplicate, or
// dead-lettered — and it is written entirely against interfaces so the
// chaos scenarios can be unit-tested without Kafka, Postgres or Valkey.
//
// The layering, in order, for every delivery:
//
//  1. Poison check — a simulated hard-down downstream dependency fails
//     fast with a retryable error and writes nothing, so retries are safe.
//  2. Payload validation — permanently invalid events go straight to the
//     DLQ (retrying garbage is waste).
//  3. Valkey fast-path — a hit means "definitely applied already"
//     (keys are only written after the Postgres commit).
//  4. Postgres apply transaction — the authoritative dedup: the
//     processed_events insert and the money movement commit or roll back
//     together. A unique-constraint conflict means duplicate.
package ledger

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Sravanthipanga/onceonly/internal/broker"
	"github.com/Sravanthipanga/onceonly/internal/dedup"
	"github.com/Sravanthipanga/onceonly/internal/domain"
	"github.com/Sravanthipanga/onceonly/internal/metrics"
	"github.com/Sravanthipanga/onceonly/internal/store"
)

// Processor handles single deliveries. It is safe for concurrent use:
// all authoritative state lives behind the Store's unique constraints.
type Processor struct {
	store    store.Store
	dedup    dedup.Store
	dlqTopic broker.Producer // optional; the DLQ *table* is the system of record for /dlq
}

// NewProcessor builds a Processor. dlqTopic may be nil (tests, or a
// deployment that only wants the DLQ table).
func NewProcessor(st store.Store, dd dedup.Store, dlqTopic broker.Producer) *Processor {
	return &Processor{store: st, dedup: dd, dlqTopic: dlqTopic}
}

// Process handles ONE delivery of ev. A non-nil error means the delivery
// failed in a way the caller may retry (transient) or must surface
// (infrastructure failure); the returned Outcome is meaningful only when
// err is nil, except that ErrPoison is returned together with a zero
// outcome to signal "retry me".
func (p *Processor) Process(ctx context.Context, ev domain.Event) (domain.Outcome, error) {
	_ = p.store.IncrementCounter(ctx, store.CounterEventsReceived, 1)
	metrics.EventsReceived.Inc()

	// (1) Simulated downstream failure. Writes nothing, so the retry
	// loop can hammer this safely.
	if ev.Reference == domain.PoisonReference {
		return 0, domain.ErrPoison
	}

	// (2) Permanent validation failures → DLQ immediately.
	if err := validate(ev); err != nil {
		return p.deadLetter(ctx, ev, err.Error(), 1)
	}

	// (3) Fast path. A hit is trustworthy (post-commit writes only).
	if p.dedup != nil {
		seen, err := p.dedup.Seen(ctx, ev.EventID)
		if err == nil && seen {
			p.countDuplicate(ctx)
			return domain.OutcomeDuplicate, nil
		}
		// A dedup-store error is not fatal: fall through to Postgres,
		// which is authoritative. The fast path is an optimisation,
		// never a single point of failure.
	}

	// (4) The authoritative exactly-once step.
	res, err := p.store.ApplyEvent(ctx, ev)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			// Well-formed event, unknown account: permanent. Retrying
			// will never conjure the account into existence.
			return p.deadLetter(ctx, ev, err.Error(), 1)
		}
		return 0, fmt.Errorf("apply event %s: %w", ev.EventID, err)
	}
	switch res {
	case store.ApplyApplied:
		metrics.TransfersApplied.Inc()
		if p.dedup != nil {
			// Best-effort fast-path warm-up for the duplicates that
			// are certainly coming. Failure here only costs a
			// Postgres round-trip next time.
			_ = p.dedup.MarkProcessed(ctx, ev.EventID)
		}
		return domain.OutcomeApplied, nil
	case store.ApplyDuplicate:
		p.countDuplicate(ctx)
		return domain.OutcomeDuplicate, nil
	case store.ApplyInsufficientFunds:
		// ApplyEvent already wrote the DLQ row atomically; mirror the
		// event to the DLQ topic for stream consumers.
		metrics.DLQTotal.Inc()
		p.publishDLQTopic(ctx, ev)
		return domain.OutcomeDeadLettered, nil
	default:
		return 0, fmt.Errorf("unknown apply result %v", res)
	}
}

// ProcessWithRetries is the consumer's per-record driver: transient
// failures are retried up to maxAttempts total attempts with backoff,
// then the event is dead-lettered. The sleep function is injectable so
// tests run instantly.
func (p *Processor) ProcessWithRetries(ctx context.Context, ev domain.Event, maxAttempts int, sleep func(time.Duration)) (domain.Outcome, error) {
	if sleep == nil {
		sleep = time.Sleep
	}
	backoff := []time.Duration{100 * time.Millisecond, 300 * time.Millisecond, 900 * time.Millisecond}
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		outcome, err := p.Process(ctx, ev)
		if err == nil {
			return outcome, nil
		}
		if !domain.IsTransient(err) {
			// Infrastructure errors (DB down etc.) are surfaced, not
			// retried inline and never dead-lettered: the caller
			// leaves the offset uncommitted so Kafka redelivers.
			return 0, err
		}
		lastErr = err
		if attempt < maxAttempts {
			d := backoff[min(attempt-1, len(backoff)-1)]
			sleep(d)
		}
	}
	// Retries exhausted.
	return p.deadLetter(ctx, ev, fmt.Sprintf("retries exhausted after %d attempts: %v", maxAttempts, lastErr), maxAttempts)
}

// deadLetter records the event in the DLQ (idempotently) and mirrors it
// to the DLQ topic.
func (p *Processor) deadLetter(ctx context.Context, ev domain.Event, errMsg string, attempts int) (domain.Outcome, error) {
	created, err := p.store.RecordDeadLetter(ctx, ev, errMsg, attempts)
	if err != nil {
		return 0, fmt.Errorf("record dead letter %s: %w", ev.EventID, err)
	}
	if !created {
		// Already dead-lettered: this delivery is a duplicate of a
		// corpse. Count it as a duplicate, not a second DLQ entry.
		p.countDuplicate(ctx)
		return domain.OutcomeDuplicate, nil
	}
	metrics.DLQTotal.Inc()
	p.publishDLQTopic(ctx, ev)
	return domain.OutcomeDeadLettered, nil
}

func (p *Processor) publishDLQTopic(ctx context.Context, ev domain.Event) {
	if p.dlqTopic == nil {
		return
	}
	// Best-effort: the DLQ table is the system of record (it backs
	// GET /dlq and replay); the topic is a convenience stream.
	_ = p.dlqTopic.Publish(ctx, domain.TopicDLQ, ev.FromAccount, ev)
}

func (p *Processor) countDuplicate(ctx context.Context) {
	_ = p.store.IncrementCounter(ctx, store.CounterDuplicatesDropped, 1)
	metrics.DuplicatesDropped.Inc()
}

func validate(ev domain.Event) error {
	switch {
	case ev.EventID == "" || ev.TransferID == "":
		return fmt.Errorf("%w: missing event/transfer id", domain.ErrInvalidEvent)
	case ev.AmountCents <= 0:
		return fmt.Errorf("%w: amount_cents must be positive, got %d", domain.ErrInvalidEvent, ev.AmountCents)
	case ev.FromAccount == "" || ev.ToAccount == "":
		return fmt.Errorf("%w: missing account", domain.ErrInvalidEvent)
	case ev.FromAccount == ev.ToAccount:
		return fmt.Errorf("%w: from_account == to_account", domain.ErrInvalidEvent)
	default:
		return nil
	}
}
