// Package relay implements the transactional-outbox relay: it polls
// unpublished outbox rows and publishes them to Kafka, keyed by
// from_account so all events for one account stay in order on a single
// partition.
//
// The relay is deliberately at-least-once: rows are locked
// (FOR UPDATE SKIP LOCKED), published, and only then marked published
// inside the same DB transaction. A crash between the Kafka publish and
// the commit republishes the event next run — a duplicate on the topic
// that the consumer's dedup gate exists to absorb. Exactly-once
// publishing would require Kafka transactions coupled to the DB, which
// is precisely the dual-write trap the outbox pattern avoids by making
// the consumer idempotent instead.
package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/Sravanthipanga/onceonly/internal/broker"
	"github.com/Sravanthipanga/onceonly/internal/domain"
	"github.com/Sravanthipanga/onceonly/internal/metrics"
	"github.com/Sravanthipanga/onceonly/internal/store"
)

const (
	batchSize    = 100
	pollInterval = 250 * time.Millisecond
)

// Relay drains the outbox to Kafka.
type Relay struct {
	store    store.Store
	producer broker.Producer
	log      *slog.Logger
}

// New builds a Relay.
func New(st store.Store, producer broker.Producer, log *slog.Logger) *Relay {
	if log == nil {
		log = slog.Default()
	}
	return &Relay{store: st, producer: producer, log: log}
}

// PublishOnce runs a single relay pass and returns how many events were
// published. Exposed separately from Run for tests and diagnostics.
func (r *Relay) PublishOnce(ctx context.Context) (int, error) {
	published := 0
	err := r.store.WithOutboxBatch(ctx, batchSize, func(rows []domain.OutboxRow) error {
		for _, row := range rows {
			var ev domain.Event
			if err := json.Unmarshal(row.Payload, &ev); err != nil {
				return fmt.Errorf("unmarshal outbox payload %d: %w", row.ID, err)
			}
			if err := r.producer.Publish(ctx, row.Topic, row.Key, ev); err != nil {
				return err
			}
			published++
		}
		return nil
	})
	if err != nil {
		return published, err
	}
	if pending, err := r.store.OutboxPending(ctx); err == nil {
		metrics.OutboxPending.Set(float64(pending))
	}
	return published, nil
}

// Run polls the outbox until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) error {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			n, err := r.PublishOnce(ctx)
			if err != nil {
				r.log.Error("relay pass failed", "err", err)
				continue
			}
			if n > 0 {
				r.log.Debug("relay published", "count", n)
			}
		}
	}
}
