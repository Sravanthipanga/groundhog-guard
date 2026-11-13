package chaos_test

import (
	"context"
	"testing"
	"time"

	"github.com/Sravanthipanga/onceonly/internal/broker"
	"github.com/Sravanthipanga/onceonly/internal/chaos"
	"github.com/Sravanthipanga/onceonly/internal/dedup"
	"github.com/Sravanthipanga/onceonly/internal/domain"
	"github.com/Sravanthipanga/onceonly/internal/ledger"
	"github.com/Sravanthipanga/onceonly/internal/relay"
	"github.com/Sravanthipanga/onceonly/internal/service"
	"github.com/Sravanthipanga/onceonly/internal/store"
)

// world is the full pipeline in miniature: service + relay + chaos all
// publishing into one fake broker, and a processor draining it.
type world struct {
	ctx  context.Context
	st   *store.MemoryStore
	svc  *service.Service
	pub  *broker.FakeProducer
	proc *ledger.Processor
	rly  *relay.Relay
	rnr  *chaos.Runner
}

func newWorld(t *testing.T) *world {
	t.Helper()
	ctx := context.Background()
	st := store.NewMemoryStore()
	pub := broker.NewFakeProducer()
	svc := service.New(st, pub)
	if _, err := svc.Seed(ctx); err != nil {
		t.Fatal(err)
	}
	return &world{
		ctx:  ctx,
		st:   st,
		svc:  svc,
		pub:  pub,
		proc: ledger.NewProcessor(st, dedup.NewMemoryStore(), pub),
		rly:  relay.New(st, pub, nil),
		rnr:  chaos.New(svc, pub),
	}
}

// drain relays the outbox, then feeds every message on the transfers
// topic to the processor, in publish order. Returns outcome tallies.
func (w *world) drain(t *testing.T) (applied, duplicates, dead int) {
	t.Helper()
	if _, err := w.rly.PublishOnce(w.ctx); err != nil {
		t.Fatalf("relay: %v", err)
	}
	for _, m := range w.pub.MessagesFor(domain.TopicTransfers) {
		outcome, err := w.proc.Process(w.ctx, m.Event())
		if err != nil {
			t.Fatalf("process %s: %v", m.Event().EventID, err)
		}
		switch outcome {
		case domain.OutcomeApplied:
			applied++
		case domain.OutcomeDuplicate:
			duplicates++
		case domain.OutcomeDeadLettered:
			dead++
		}
	}
	return applied, duplicates, dead
}

func (w *world) total(t *testing.T) int64 {
	t.Helper()
	_, total, err := w.svc.ListAccounts(w.ctx)
	if err != nil {
		t.Fatal(err)
	}
	return total
}

const seedTotal = 139_150_000

func TestDuplicateStormAppliesOnce(t *testing.T) {
	w := newWorld(t)
	summary, err := w.rnr.Run(w.ctx, chaos.ModeDuplicateStorm, 50)
	if err != nil {
		t.Fatal(err)
	}
	if summary.TransfersCreated != 1 || summary.EventsPublished != 50 {
		t.Fatalf("summary = %+v", summary)
	}
	applied, duplicates, _ := w.drain(t)
	// 50 raw copies + 1 relay publish of the same event = 51 deliveries.
	if applied != 1 {
		t.Fatalf("applied = %d, want 1", applied)
	}
	if duplicates != 50 {
		t.Fatalf("duplicates = %d, want 50", duplicates)
	}
	if got := w.total(t); got != seedTotal {
		t.Fatalf("total = %d, want %d", got, seedTotal)
	}
}

func TestHurricaneInvariant(t *testing.T) {
	w := newWorld(t)
	const n = 20
	summary, err := w.rnr.Run(w.ctx, chaos.ModeHurricane, n)
	if err != nil {
		t.Fatal(err)
	}
	if summary.EventsPublished < 3*n || summary.EventsPublished > 5*n {
		t.Fatalf("hurricane published %d events, want 3–5 per transfer", summary.EventsPublished)
	}
	applied, duplicates, _ := w.drain(t)
	if applied != n {
		t.Fatalf("applied = %d, want %d", applied, n)
	}
	if duplicates == 0 {
		t.Fatal("hurricane produced no duplicates — the test proves nothing")
	}
	if got := w.total(t); got != seedTotal {
		t.Fatalf("INVARIANT BROKEN: total = %d, want %d", got, seedTotal)
	}
	st, err := w.st.Stats(w.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.TransfersApplied != n {
		t.Fatalf("stats applied = %d, want %d", st.TransfersApplied, n)
	}
}

func TestShuffleAppliesAllOnce(t *testing.T) {
	w := newWorld(t)
	const n = 20
	if _, err := w.rnr.Run(w.ctx, chaos.ModeShuffle, n); err != nil {
		t.Fatal(err)
	}
	applied, _, _ := w.drain(t)
	if applied != n {
		t.Fatalf("applied = %d, want %d", applied, n)
	}
	if got := w.total(t); got != seedTotal {
		t.Fatalf("total = %d, want %d", got, seedTotal)
	}
	// Every created transfer must be in applied state.
	accounts, _, err := w.svc.ListAccounts(w.ctx)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, acc := range accounts {
		trs, err := w.st.ListTransfersByAccount(w.ctx, acc.ID, 100)
		if err != nil {
			t.Fatal(err)
		}
		for _, tr := range trs {
			if seen[tr.ID] {
				continue
			}
			seen[tr.ID] = true
			if tr.Status != domain.StatusApplied {
				t.Fatalf("transfer %s status = %q, want applied", tr.ID, tr.Status)
			}
		}
	}
	if len(seen) != n {
		t.Fatalf("found %d unique transfers, want %d", len(seen), n)
	}
}

func TestPoisonModeLandsInDLQ(t *testing.T) {
	w := newWorld(t)
	if _, err := w.rnr.Run(w.ctx, chaos.ModePoison, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := w.rly.PublishOnce(w.ctx); err != nil {
		t.Fatal(err)
	}
	// Drive both poison events with the real retry driver.
	for _, m := range w.pub.MessagesFor(domain.TopicTransfers) {
		outcome, err := w.proc.ProcessWithRetries(w.ctx, m.Event(), 3, noSleep)
		if err != nil {
			t.Fatal(err)
		}
		if outcome != domain.OutcomeDeadLettered {
			t.Fatalf("poison outcome = %v, want dead_lettered", outcome)
		}
	}
	dlq, err := w.st.ListDLQ(w.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(dlq) != 2 {
		t.Fatalf("dlq entries = %d, want 2", len(dlq))
	}
	if got := w.total(t); got != seedTotal {
		t.Fatalf("total = %d, want %d (poison must never move money)", got, seedTotal)
	}
}

func noSleep(time.Duration) {}
