package relay_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Sravanthipanga/groundhog-guard/internal/broker"
	"github.com/Sravanthipanga/groundhog-guard/internal/dedup"
	"github.com/Sravanthipanga/groundhog-guard/internal/domain"
	"github.com/Sravanthipanga/groundhog-guard/internal/ledger"
	"github.com/Sravanthipanga/groundhog-guard/internal/relay"
	"github.com/Sravanthipanga/groundhog-guard/internal/store"
)

// crashyProducer records every publish (the message DID reach the
// broker) but reports failure for the first `failures` calls — the
// classic relay crash window: published to Kafka, died before marking
// the outbox row.
type crashyProducer struct {
	inner    *broker.FakeProducer
	failures int
	calls    int
}

func (p *crashyProducer) Publish(ctx context.Context, topic, key string, ev domain.Event) error {
	p.calls++
	if err := p.inner.Publish(ctx, topic, key, ev); err != nil {
		return err
	}
	if p.calls <= p.failures {
		return errors.New("simulated crash after publish, before outbox mark")
	}
	return nil
}

func (p *crashyProducer) PublishRaw(ctx context.Context, topic, key string, payload []byte, headers map[string]string) error {
	return p.inner.PublishRaw(ctx, topic, key, payload, headers)
}

func (p *crashyProducer) Close() {}

func TestRelayPublishesAndMarks(t *testing.T) {
	ctx := context.Background()
	st := store.NewMemoryStore()
	a, _ := st.EnsureAccount(ctx, "Aisha Kapoor", 100_000)
	b, _ := st.EnsureAccount(ctx, "Rohan Mehta", 100_000)
	for i := 0; i < 3; i++ {
		if _, err := st.CreateTransfer(ctx, store.CreateTransferRequest{
			IdempotencyKey: "relay-k" + string(rune('a'+i)),
			FromAccount:    a.ID, ToAccount: b.ID, AmountCents: 100,
		}); err != nil {
			t.Fatal(err)
		}
	}
	pub := broker.NewFakeProducer()
	r := relay.New(st, pub, nil)

	n, err := r.PublishOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("published = %d, want 3", n)
	}
	if pending, _ := st.OutboxPending(ctx); pending != 0 {
		t.Fatalf("pending = %d, want 0", pending)
	}
	n, err = r.PublishOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("second pass published = %d, want 0", n)
	}
}

// The full at-least-once story: relay publishes, "crashes" before
// marking, republishes next pass — so the topic holds every event TWICE —
// and the consumer still applies each exactly once.
func TestRelayCrashCreatesDuplicatesConsumerDropsThem(t *testing.T) {
	ctx := context.Background()
	st := store.NewMemoryStore()
	a, _ := st.EnsureAccount(ctx, "Aisha Kapoor", 100_000)
	b, _ := st.EnsureAccount(ctx, "Rohan Mehta", 100_000)
	const transfers = 3
	for i := 0; i < transfers; i++ {
		if _, err := st.CreateTransfer(ctx, store.CreateTransferRequest{
			IdempotencyKey: "crash-k" + string(rune('a'+i)),
			FromAccount:    a.ID, ToAccount: b.ID, AmountCents: 700,
		}); err != nil {
			t.Fatal(err)
		}
	}

	inner := broker.NewFakeProducer()
	crashy := &crashyProducer{inner: inner, failures: 1}
	r := relay.New(st, crashy, nil)

	// Pass 1: first publish reaches the "broker", then the crash hits;
	// the batch aborts and nothing is marked.
	if _, err := r.PublishOnce(ctx); err == nil {
		t.Fatal("expected first pass to fail (simulated crash)")
	}
	if pending, _ := st.OutboxPending(ctx); pending != transfers {
		t.Fatalf("pending after crash = %d, want %d (nothing marked)", pending, transfers)
	}
	// Pass 2: healthy — republishes everything.
	n, err := r.PublishOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != transfers {
		t.Fatalf("second pass published = %d, want %d", n, transfers)
	}

	msgs := inner.MessagesFor(domain.TopicTransfers)
	if len(msgs) != transfers+1 {
		t.Fatalf("topic holds %d messages, want %d (one event went out twice)", len(msgs), transfers+1)
	}

	proc := ledger.NewProcessor(st, dedup.NewMemoryStore(), nil)
	applied, duplicates := 0, 0
	for _, m := range msgs {
		outcome, err := proc.Process(ctx, m.Event())
		if err != nil {
			t.Fatal(err)
		}
		switch outcome {
		case domain.OutcomeApplied:
			applied++
		case domain.OutcomeDuplicate:
			duplicates++
		}
	}
	if applied != transfers {
		t.Fatalf("applied = %d, want %d", applied, transfers)
	}
	if duplicates != 1 {
		t.Fatalf("duplicates dropped = %d, want 1", duplicates)
	}
	acc, _ := st.GetAccount(ctx, b.ID)
	if acc.BalanceCents != 100_000+transfers*700 {
		t.Fatalf("receiver balance = %d, want %d", acc.BalanceCents, 100_000+transfers*700)
	}
}
