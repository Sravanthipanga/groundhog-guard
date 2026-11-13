package ledger_test

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/Sravanthipanga/onceonly/internal/broker"
	"github.com/Sravanthipanga/onceonly/internal/dedup"
	"github.com/Sravanthipanga/onceonly/internal/domain"
	"github.com/Sravanthipanga/onceonly/internal/ledger"
	"github.com/Sravanthipanga/onceonly/internal/service"
	"github.com/Sravanthipanga/onceonly/internal/store"
)

type harness struct {
	st   *store.MemoryStore
	dd   *dedup.MemoryStore
	dlq  *broker.FakeProducer
	proc *ledger.Processor
	ctx  context.Context
	a, b domain.Account
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	st := store.NewMemoryStore()
	ctx := context.Background()
	a, err := st.EnsureAccount(ctx, "Aisha Kapoor", 1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	b, err := st.EnsureAccount(ctx, "Rohan Mehta", 500_000)
	if err != nil {
		t.Fatal(err)
	}
	dd := dedup.NewMemoryStore()
	fp := broker.NewFakeProducer()
	return &harness{st: st, dd: dd, dlq: fp, proc: ledger.NewProcessor(st, dd, fp), ctx: ctx, a: a, b: b}
}

func (h *harness) createTransfer(t *testing.T, key string, from, to domain.Account, amount int64, ref string) store.CreateTransferResult {
	t.Helper()
	res, err := h.st.CreateTransfer(h.ctx, store.CreateTransferRequest{
		IdempotencyKey: key, FromAccount: from.ID, ToAccount: to.ID,
		AmountCents: amount, Reference: ref,
	})
	if err != nil {
		t.Fatalf("create transfer: %v", err)
	}
	return res
}

func (h *harness) balance(t *testing.T, id string) int64 {
	t.Helper()
	acc, err := h.st.GetAccount(h.ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return acc.BalanceCents
}

func (h *harness) total(t *testing.T) int64 {
	t.Helper()
	accounts, err := h.st.ListAccounts(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	var sum int64
	for _, a := range accounts {
		sum += a.BalanceCents
	}
	return sum
}

// THE test: the same event, delivered 100 times concurrently, applies once.
func TestApplyExactlyOnceUnderConcurrentDuplicates(t *testing.T) {
	h := newHarness(t)
	res := h.createTransfer(t, "k-storm", h.a, h.b, 2500, "storm")

	const deliveries = 100
	outcomes := make([]domain.Outcome, deliveries)
	var wg sync.WaitGroup
	for i := 0; i < deliveries; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			outcome, err := h.proc.Process(h.ctx, res.Event)
			if err != nil {
				t.Errorf("process: %v", err)
				return
			}
			outcomes[i] = outcome
		}(i)
	}
	wg.Wait()

	applied, duplicates := 0, 0
	for _, o := range outcomes {
		switch o {
		case domain.OutcomeApplied:
			applied++
		case domain.OutcomeDuplicate:
			duplicates++
		}
	}
	if applied != 1 {
		t.Fatalf("applied = %d, want exactly 1", applied)
	}
	if duplicates != deliveries-1 {
		t.Fatalf("duplicates = %d, want %d", duplicates, deliveries-1)
	}
	if got := h.balance(t, h.a.ID); got != 1_000_000-2500 {
		t.Fatalf("sender balance = %d, want %d", got, 1_000_000-2500)
	}
	if got := h.balance(t, h.b.ID); got != 500_000+2500 {
		t.Fatalf("receiver balance = %d, want %d", got, 500_000+2500)
	}
	st, err := h.st.Stats(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.DuplicatesDropped != int64(deliveries-1) {
		t.Fatalf("stats duplicates_dropped = %d, want %d", st.DuplicatesDropped, deliveries-1)
	}
	if st.TransfersApplied != 1 {
		t.Fatalf("stats transfers_applied = %d, want 1", st.TransfersApplied)
	}
	if st.EventsReceived != int64(deliveries) {
		t.Fatalf("stats events_received = %d, want %d", st.EventsReceived, deliveries)
	}
}

func TestInsufficientFundsGoesToDLQAndStaysThere(t *testing.T) {
	h := newHarness(t)
	// B only has 500_000; try to send 900_000 from B to A.
	res := h.createTransfer(t, "k-broke", h.b, h.a, 900_000, "too-big")

	outcome, err := h.proc.Process(h.ctx, res.Event)
	if err != nil {
		t.Fatal(err)
	}
	if outcome != domain.OutcomeDeadLettered {
		t.Fatalf("outcome = %v, want dead_lettered", outcome)
	}
	if got := h.balance(t, h.b.ID); got != 500_000 {
		t.Fatalf("balance changed on insufficient funds: %d", got)
	}
	dlq, err := h.st.ListDLQ(h.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(dlq) != 1 || dlq[0].EventID != res.Event.EventID {
		t.Fatalf("dlq = %+v, want the one event", dlq)
	}
	tr, err := h.st.GetTransfer(h.ctx, res.Transfer.ID)
	if err != nil {
		t.Fatal(err)
	}
	if tr.Status != domain.StatusFailedInsufficientFunds {
		t.Fatalf("transfer status = %q, want %q", tr.Status, domain.StatusFailedInsufficientFunds)
	}
	// Redelivery: dropped as a duplicate, no second DLQ entry, no drama.
	outcome, err = h.proc.Process(h.ctx, res.Event)
	if err != nil {
		t.Fatal(err)
	}
	if outcome != domain.OutcomeDuplicate {
		t.Fatalf("redelivery outcome = %v, want duplicate", outcome)
	}
	dlq, _ = h.st.ListDLQ(h.ctx)
	if len(dlq) != 1 {
		t.Fatalf("dlq grew on redelivery: %d entries", len(dlq))
	}
	if msgs := h.dlq.MessagesFor(domain.TopicDLQ); len(msgs) != 1 {
		t.Fatalf("dlq topic messages = %d, want 1", len(msgs))
	}
}

func TestPoisonRetriesExhaustToDLQThenReplayApplies(t *testing.T) {
	h := newHarness(t)
	res := h.createTransfer(t, "k-poison", h.a, h.b, 4200, domain.PoisonReference)

	outcome, err := h.proc.ProcessWithRetries(h.ctx, res.Event, 3, noSleep)
	if err != nil {
		t.Fatal(err)
	}
	if outcome != domain.OutcomeDeadLettered {
		t.Fatalf("outcome = %v, want dead_lettered", outcome)
	}
	if got := h.balance(t, h.a.ID); got != 1_000_000 {
		t.Fatalf("poison moved money: balance = %d", got)
	}
	dlq, _ := h.st.ListDLQ(h.ctx)
	if len(dlq) != 1 {
		t.Fatalf("dlq entries = %d, want 1", len(dlq))
	}
	if dlq[0].Attempts != 3 {
		t.Fatalf("dlq attempts = %d, want 3", dlq[0].Attempts)
	}

	// Replay through the service (the same path as POST /dlq/{id}/replay):
	// the poison flag is fixed and the ORIGINAL event id is republished.
	pub := broker.NewFakeProducer()
	svc := service.New(h.st, pub)
	fixed, err := svc.ReplayDLQ(h.ctx, res.Event.EventID)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if fixed.Reference != domain.FixedReference {
		t.Fatalf("replayed reference = %q, want %q", fixed.Reference, domain.FixedReference)
	}
	if fixed.EventID != res.Event.EventID {
		t.Fatalf("replay changed the event id: %q vs %q", fixed.EventID, res.Event.EventID)
	}
	msgs := pub.MessagesFor(domain.TopicTransfers)
	if len(msgs) != 1 {
		t.Fatalf("replay published %d messages, want 1", len(msgs))
	}

	outcome, err = h.proc.Process(h.ctx, msgs[0].Event())
	if err != nil {
		t.Fatal(err)
	}
	if outcome != domain.OutcomeApplied {
		t.Fatalf("post-replay outcome = %v, want applied", outcome)
	}
	if got := h.balance(t, h.b.ID); got != 500_000+4200 {
		t.Fatalf("receiver balance = %d, want %d", got, 500_000+4200)
	}
	// And the replayed event is itself deduped on redelivery.
	outcome, err = h.proc.Process(h.ctx, msgs[0].Event())
	if err != nil {
		t.Fatal(err)
	}
	if outcome != domain.OutcomeDuplicate {
		t.Fatalf("replayed-event redelivery outcome = %v, want duplicate", outcome)
	}
}

func noSleep(time.Duration) {}

func TestInvalidEventsAreDeadLettered(t *testing.T) {
	h := newHarness(t)
	res := h.createTransfer(t, "k-valid", h.a, h.b, 100, "ok")

	// A raw-published corruption of the event: negative amount.
	bad := res.Event
	bad.AmountCents = -5
	outcome, err := h.proc.Process(h.ctx, bad)
	if err != nil {
		t.Fatal(err)
	}
	if outcome != domain.OutcomeDeadLettered {
		t.Fatalf("negative amount outcome = %v, want dead_lettered", outcome)
	}

	// Self-transfer.
	self := res.Event
	self.EventID = "11111111-2222-3333-4444-555555555555"
	self.ToAccount = self.FromAccount
	outcome, err = h.proc.Process(h.ctx, self)
	if err != nil {
		t.Fatal(err)
	}
	if outcome != domain.OutcomeDeadLettered {
		t.Fatalf("self-transfer outcome = %v, want dead_lettered", outcome)
	}

	if got := h.balance(t, h.a.ID); got != 1_000_000 {
		t.Fatalf("invalid events moved money: %d", got)
	}
	dlq, _ := h.st.ListDLQ(h.ctx)
	if len(dlq) != 2 {
		t.Fatalf("dlq entries = %d, want 2", len(dlq))
	}
}

// The chaos invariant: a seeded random storm of transfers, each delivered
// 1–5 times in shuffled order, must preserve the total and apply every
// reachable transfer exactly once. A shadow ledger computed from the
// outcomes is the oracle.
func TestInvariantUnderRandomChaos(t *testing.T) {
	h := newHarness(t)
	ctx := h.ctx
	// More accounts for a richer storm.
	extra := []string{"Meera Nair", "Kabir Singh", "Priya Sharma", "Arjun Rao"}
	balances := map[string]int64{h.a.ID: 1_000_000, h.b.ID: 500_000}
	accounts := []domain.Account{h.a, h.b}
	for i, name := range extra {
		acc, err := h.st.EnsureAccount(ctx, name, int64(200_000*(i+1)))
		if err != nil {
			t.Fatal(err)
		}
		accounts = append(accounts, acc)
		balances[acc.ID] = acc.BalanceCents
	}
	initialTotal := h.total(t)

	rng := rand.New(rand.NewSource(42))
	var events []domain.Event
	for i := 0; i < 300; i++ {
		from := accounts[rng.Intn(len(accounts))]
		to := accounts[rng.Intn(len(accounts))]
		if from.ID == to.ID {
			continue
		}
		// Amounts up to 300_000: some transfers will hit insufficient
		// funds depending on delivery order — that's part of the chaos.
		amount := int64(1 + rng.Intn(300_000))
		res, err := h.st.CreateTransfer(ctx, store.CreateTransferRequest{
			IdempotencyKey: fmt.Sprintf("chaos-%d", i),
			FromAccount:    from.ID, ToAccount: to.ID,
			AmountCents: amount, Reference: "chaos",
		})
		if err != nil {
			t.Fatalf("create %d: %v", i, err)
		}
		events = append(events, res.Event)
	}

	// Build the delivery deck: every event 1–5 times, shuffled.
	var deck []domain.Event
	for _, ev := range events {
		for c, copies := 0, 1+rng.Intn(5); c < copies; c++ {
			deck = append(deck, ev)
		}
	}
	rng.Shuffle(len(deck), func(i, j int) { deck[i], deck[j] = deck[j], deck[i] })

	shadow := make(map[string]int64, len(balances))
	for k, v := range balances {
		shadow[k] = v
	}
	appliedCount := map[string]int{}
	for _, ev := range deck {
		outcome, err := h.proc.Process(ctx, ev)
		if err != nil {
			t.Fatalf("process %s: %v", ev.EventID, err)
		}
		if outcome == domain.OutcomeApplied {
			appliedCount[ev.EventID]++
			shadow[ev.FromAccount] -= ev.AmountCents
			shadow[ev.ToAccount] += ev.AmountCents
		}
	}

	for id, n := range appliedCount {
		if n != 1 {
			t.Fatalf("event %s applied %d times — exactly-once violated", id, n)
		}
	}
	if got := h.total(t); got != initialTotal {
		t.Fatalf("INVARIANT BROKEN: total = %d, want %d", got, initialTotal)
	}
	for id, want := range shadow {
		if got := h.balance(t, id); got != want {
			t.Fatalf("account %s balance = %d, shadow says %d", id, got, want)
		}
	}
	if len(appliedCount) == 0 {
		t.Fatal("nothing applied — the storm was vacuous")
	}
	t.Logf("deliveries=%d unique_events=%d applied=%d", len(deck), len(events), len(appliedCount))
}
