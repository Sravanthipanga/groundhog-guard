// Package chaos is the built-in adversary: it attacks the pipeline with
// the failure modes that break naive event consumers — duplicate
// deliveries, out-of-order delivery, poison events — so the idempotency
// machinery can be watched doing its job.
//
// Two entry points, honestly distinguished:
//   - Ingest goes through the service layer, i.e. the exact code path of
//     the public POST /transfers API (idempotency keys, outbox, relay).
//   - Raw publishes go straight to the Kafka topic with pre-built
//     events, bypassing API idempotency on purpose: this simulates a
//     producer that retries blindly, a relay that republishes after a
//     crash, or a second system re-sending history. The duplicates are
//     real duplicates — same event_id, same payload.
package chaos

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/Sravanthipanga/groundhog-guard/internal/broker"
	"github.com/Sravanthipanga/groundhog-guard/internal/domain"
	"github.com/Sravanthipanga/groundhog-guard/internal/service"
	"github.com/Sravanthipanga/groundhog-guard/internal/store"
)

// Mode names accepted by POST /chaos/{mode} and the chaos CLI.
const (
	ModeDuplicateStorm = "duplicate-storm"
	ModeShuffle        = "shuffle"
	ModePoison         = "poison"
	ModeHurricane      = "hurricane"
)

// Summary describes what a chaos run did.
type Summary struct {
	Mode             string `json:"mode"`
	TransfersCreated int    `json:"transfers_created"`
	EventsPublished  int    `json:"events_published"`
	Note             string `json:"note"`
}

// Runner executes chaos modes against a Service + raw producer.
type Runner struct {
	svc *service.Service
	pub broker.Producer
	rng *rand.Rand
}

// New builds a Runner. pub may be nil, in which case modes that need raw
// publishes fail with a clear error (poison still works — it only needs
// the ingest path).
func New(svc *service.Service, pub broker.Producer) *Runner {
	return &Runner{svc: svc, pub: pub, rng: rand.New(rand.NewSource(time.Now().UnixNano()))}
}

// Run executes one chaos mode. count semantics per mode:
//   - duplicate-storm: how many extra copies of one event to publish
//   - shuffle: how many transfers to create and publish out of order
//   - poison: how many poison transfers to create
//   - hurricane: how many transfers to create (each published 3–5×)
func (r *Runner) Run(ctx context.Context, mode string, count int) (Summary, error) {
	switch mode {
	case ModeDuplicateStorm:
		if count <= 0 {
			count = 100
		}
		return r.duplicateStorm(ctx, count)
	case ModeShuffle:
		if count <= 0 {
			count = 20
		}
		return r.shuffle(ctx, count)
	case ModePoison:
		if count <= 0 {
			count = 1
		}
		return r.poison(ctx, count)
	case ModeHurricane:
		if count <= 0 {
			count = 25
		}
		return r.hurricane(ctx, count)
	default:
		return Summary{}, fmt.Errorf("unknown chaos mode %q (want %s, %s, %s, %s)",
			mode, ModeDuplicateStorm, ModeShuffle, ModePoison, ModeHurricane)
	}
}

func (r *Runner) twoAccounts(ctx context.Context) (from, to domain.Account, err error) {
	accounts, _, err := r.svc.ListAccounts(ctx)
	if err != nil {
		return from, to, err
	}
	if len(accounts) < 2 {
		return from, to, fmt.Errorf("need at least 2 accounts — run `groundhog seed` first")
	}
	return accounts[0], accounts[1], nil
}

func (r *Runner) create(ctx context.Context, from, to domain.Account, amount int64, ref string) (store.CreateTransferResult, error) {
	return r.svc.CreateTransfer(ctx, store.CreateTransferRequest{
		IdempotencyKey: fmt.Sprintf("chaos-%s-%d", ref, time.Now().UnixNano()),
		FromAccount:    from.ID,
		ToAccount:      to.ID,
		AmountCents:    amount,
		Reference:      ref,
	})
}

func (r *Runner) duplicateStorm(ctx context.Context, count int) (Summary, error) {
	if r.pub == nil {
		return Summary{}, fmt.Errorf("duplicate-storm needs a Kafka producer")
	}
	from, to, err := r.twoAccounts(ctx)
	if err != nil {
		return Summary{}, err
	}
	res, err := r.create(ctx, from, to, 100, "CHAOS-STORM")
	if err != nil {
		return Summary{}, err
	}
	for i := 0; i < count; i++ {
		if err := r.pub.Publish(ctx, domain.TopicTransfers, res.Event.FromAccount, res.Event); err != nil {
			return Summary{}, fmt.Errorf("publish copy %d: %w", i, err)
		}
	}
	return Summary{
		Mode:             ModeDuplicateStorm,
		TransfersCreated: 1,
		EventsPublished:  count,
		Note: fmt.Sprintf("event %s published %d extra times (plus the relay's own publish). "+
			"Watch /stats: transfers_applied grows by 1, duplicates_dropped by ~%d.", res.Event.EventID, count, count),
	}, nil
}

func (r *Runner) shuffle(ctx context.Context, count int) (Summary, error) {
	if r.pub == nil {
		return Summary{}, fmt.Errorf("shuffle needs a Kafka producer")
	}
	accounts, _, err := r.svc.ListAccounts(ctx)
	if err != nil {
		return Summary{}, err
	}
	if len(accounts) < 2 {
		return Summary{}, fmt.Errorf("need at least 2 accounts — run `groundhog seed` first")
	}
	var events []domain.Event
	for i := 0; i < count; i++ {
		from := accounts[i%len(accounts)]
		to := accounts[(i+1)%len(accounts)]
		res, err := r.create(ctx, from, to, 100+int64(i), "CHAOS-SHUFFLE")
		if err != nil {
			return Summary{}, err
		}
		events = append(events, res.Event)
	}
	r.rng.Shuffle(len(events), func(i, j int) { events[i], events[j] = events[j], events[i] })
	for _, ev := range events {
		if err := r.pub.Publish(ctx, domain.TopicTransfers, ev.FromAccount, ev); err != nil {
			return Summary{}, err
		}
	}
	return Summary{
		Mode:             ModeShuffle,
		TransfersCreated: count,
		EventsPublished:  count,
		Note: "transfers created via the API, then raw-published in shuffled order " +
			"(the relay's in-order copies race them — the ledger doesn't care: " +
			"balance effects commute, and dedup absorbs the second delivery).",
	}, nil
}

func (r *Runner) poison(ctx context.Context, count int) (Summary, error) {
	from, to, err := r.twoAccounts(ctx)
	if err != nil {
		return Summary{}, err
	}
	for i := 0; i < count; i++ {
		if _, err := r.create(ctx, from, to, 500, domain.PoisonReference); err != nil {
			return Summary{}, err
		}
	}
	return Summary{
		Mode:             ModePoison,
		TransfersCreated: count,
		EventsPublished:  0,
		Note: "poison transfers created via the API with reference POISON. The relay publishes them; " +
			"the consumer retries 3 times, then dead-letters. See GET /dlq, then POST /dlq/{event_id}/replay " +
			"to apply the fix (POISON → POISON-FIXED) and watch it succeed.",
	}, nil
}

func (r *Runner) hurricane(ctx context.Context, count int) (Summary, error) {
	if r.pub == nil {
		return Summary{}, fmt.Errorf("hurricane needs a Kafka producer")
	}
	accounts, _, err := r.svc.ListAccounts(ctx)
	if err != nil {
		return Summary{}, err
	}
	if len(accounts) < 2 {
		return Summary{}, fmt.Errorf("need at least 2 accounts — run `groundhog seed` first")
	}
	var events []domain.Event
	for i := 0; i < count; i++ {
		from := accounts[i%len(accounts)]
		to := accounts[(i+1)%len(accounts)]
		res, err := r.create(ctx, from, to, 200+int64(i), "CHAOS-HURRICANE")
		if err != nil {
			return Summary{}, err
		}
		events = append(events, res.Event)
	}
	// Interleave 3–5 copies of every event, then shuffle the whole deck.
	var deck []domain.Event
	for _, ev := range events {
		copies := 3 + r.rng.Intn(3)
		for c := 0; c < copies; c++ {
			deck = append(deck, ev)
		}
	}
	r.rng.Shuffle(len(deck), func(i, j int) { deck[i], deck[j] = deck[j], deck[i] })
	for _, ev := range deck {
		if err := r.pub.Publish(ctx, domain.TopicTransfers, ev.FromAccount, ev); err != nil {
			return Summary{}, err
		}
	}
	return Summary{
		Mode:             ModeHurricane,
		TransfersCreated: count,
		EventsPublished:  len(deck),
		Note: fmt.Sprintf("%d transfers, %d total deliveries (3–5 copies each, interleaved). "+
			"Invariant: exactly %d applications, total money unchanged.", count, len(deck), count),
	}, nil
}
