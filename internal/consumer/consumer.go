// Package consumer runs the Kafka consumer group that feeds events to
// the ledger processor and commits offsets only after an event has
// reached a final state (applied, duplicate, or dead-lettered).
//
// Kafka gives us at-least-once delivery; the processor makes each
// delivery idempotent; together they behave as effectively-once.
// Offsets are committed per record, in order, and never past a record
// whose processing failed with an infrastructure error — that record
// (and everything after it on the partition) is redelivered instead.
package consumer

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Sravanthipanga/onceonly/internal/domain"
	"github.com/Sravanthipanga/onceonly/internal/ledger"
)

// GroupName is the consumer group for ledger consumers. Multiple
// consumer processes can share the work; the DB unique constraint — not
// group membership — is what guarantees exactly-once application.
const GroupName = "ledger-consumers"

// Consumer drives records from the transfers topic into the processor.
type Consumer struct {
	client    *kgo.Client
	processor *ledger.Processor
	log       *slog.Logger
}

// New builds a Consumer connected to the brokers.
func New(brokers []string, processor *ledger.Processor, log *slog.Logger) (*Consumer, error) {
	if log == nil {
		log = slog.Default()
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(GroupName),
		kgo.ConsumeTopics(domain.TopicTransfers),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxWait(500*time.Millisecond),
	)
	if err != nil {
		return nil, err
	}
	return &Consumer{client: cl, processor: processor, log: log}, nil
}

// Close shuts down the Kafka client.
func (c *Consumer) Close() { c.client.Close() }

// garbageID derives a stable UUID-shaped id from a payload hash, so the
// same unparseable payload always maps to the same DLQ entry.
func garbageID(payload []byte) string {
	sum := sha256.Sum256(payload)
	return fmt.Sprintf("%x-%x-%x-%x-%x",
		sum[0:4], sum[4:6], sum[6:8], sum[8:10], sum[10:16])
}

// Run polls until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		fetches := c.client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				c.log.Error("fetch error", "topic", e.Topic, "partition", e.Partition, "err", e.Err)
			}
			continue
		}

		failedPartition := map[int32]bool{}
		var toCommit []*kgo.Record
		fetches.EachRecord(func(rec *kgo.Record) {
			if failedPartition[rec.Partition] {
				// An earlier record on this partition failed; leave
				// this and everything after it uncommitted so the
				// whole tail is redelivered in order.
				return
			}
			var ev domain.Event
			if err := json.Unmarshal(rec.Value, &ev); err != nil {
				// Not even parseable: synthesise an event whose id is
				// derived from the payload hash (stable across
				// redeliveries of the same garbage) and let the
				// processor's validation dead-letter it as invalid
				// instead of wedging the partition forever.
				c.log.Error("unparseable record, dead-lettering as invalid",
					"partition", rec.Partition, "offset", rec.Offset, "err", err)
				ev = domain.Event{EventID: garbageID(rec.Value)}
			}
			outcome, err := c.processor.ProcessWithRetries(ctx, ev, 3, nil)
			if err != nil {
				c.log.Error("processing failed (will be redelivered)",
					"event_id", ev.EventID, "partition", rec.Partition,
					"offset", rec.Offset, "err", err)
				failedPartition[rec.Partition] = true
				return
			}
			c.log.Debug("processed",
				"event_id", ev.EventID, "outcome", outcome.String(),
				"partition", rec.Partition, "offset", rec.Offset)
			toCommit = append(toCommit, rec)
		})

		if len(toCommit) > 0 {
			if err := c.client.CommitRecords(ctx, toCommit...); err != nil {
				c.log.Error("commit failed", "err", err)
			}
		}
	}
}
