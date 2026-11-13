// Package service holds the business logic behind the HTTP API, so the
// handlers stay thin and the chaos generator can drive the exact same
// code paths as external clients.
package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/Sravanthipanga/onceonly/internal/broker"
	"github.com/Sravanthipanga/onceonly/internal/domain"
	"github.com/Sravanthipanga/onceonly/internal/store"
)

// DemoAccounts are created by `onceonly seed`. Balances are cents
// (₹1 = 100). Total: 139,150,000 cents — the invariant the whole
// system exists to protect.
var DemoAccounts = []struct {
	Name    string
	Balance int64
}{
	{"Aisha Kapoor", 25_000_000},
	{"Rohan Mehta", 18_500_000},
	{"Meera Nair", 32_750_000},
	{"Kabir Singh", 9_400_000},
	{"Priya Sharma", 41_200_000},
	{"Arjun Rao", 12_300_000},
}

// Service is the API's business-logic layer.
type Service struct {
	store    store.Store
	producer broker.Producer // used for DLQ replay + chaos raw publishes; may be nil
}

// New builds a Service.
func New(st store.Store, producer broker.Producer) *Service {
	return &Service{store: st, producer: producer}
}

// Store exposes the underlying store (chaos + diagnostics).
func (s *Service) Store() store.Store { return s.store }

// Producer exposes the configured producer (may be nil).
func (s *Service) Producer() broker.Producer { return s.producer }

// Seed creates the demo accounts if they do not already exist.
func (s *Service) Seed(ctx context.Context) ([]domain.Account, error) {
	var out []domain.Account
	for _, da := range DemoAccounts {
		acc, err := s.store.EnsureAccount(ctx, da.Name, da.Balance)
		if err != nil {
			return nil, err
		}
		out = append(out, acc)
	}
	return out, nil
}

// CreateTransfer validates and atomically creates a transfer + outbox
// event, honouring the idempotency key.
func (s *Service) CreateTransfer(ctx context.Context, req store.CreateTransferRequest) (store.CreateTransferResult, error) {
	switch {
	case req.IdempotencyKey == "":
		return store.CreateTransferResult{}, fmt.Errorf("%w: Idempotency-Key header is required", domain.ErrInvalidEvent)
	case req.AmountCents <= 0:
		return store.CreateTransferResult{}, fmt.Errorf("%w: amount_cents must be positive", domain.ErrInvalidEvent)
	case req.FromAccount == "" || req.ToAccount == "":
		return store.CreateTransferResult{}, fmt.Errorf("%w: from_account and to_account are required", domain.ErrInvalidEvent)
	case req.FromAccount == req.ToAccount:
		return store.CreateTransferResult{}, fmt.Errorf("%w: from_account and to_account must differ", domain.ErrInvalidEvent)
	}
	return s.store.CreateTransfer(ctx, req)
}

// GetAccount returns one account.
func (s *Service) GetAccount(ctx context.Context, id string) (domain.Account, error) {
	return s.store.GetAccount(ctx, id)
}

// ListAccounts returns all accounts plus the invariant total.
func (s *Service) ListAccounts(ctx context.Context) ([]domain.Account, int64, error) {
	accounts, err := s.store.ListAccounts(ctx)
	if err != nil {
		return nil, 0, err
	}
	var total int64
	for _, a := range accounts {
		total += a.BalanceCents
	}
	return accounts, total, nil
}

// RecentTransfers lists recent transfers touching an account.
func (s *Service) RecentTransfers(ctx context.Context, accountID string, limit int) ([]domain.Transfer, error) {
	if _, err := s.store.GetAccount(ctx, accountID); err != nil {
		return nil, err
	}
	return s.store.ListTransfersByAccount(ctx, accountID, limit)
}

// GetTransfer returns one transfer (with its event id).
func (s *Service) GetTransfer(ctx context.Context, id string) (domain.Transfer, error) {
	return s.store.GetTransfer(ctx, id)
}

// Stats returns the cross-process counters.
func (s *Service) Stats(ctx context.Context) (domain.Stats, error) {
	return s.store.Stats(ctx)
}

// ListDLQ returns all dead-letter entries.
func (s *Service) ListDLQ(ctx context.Context) ([]domain.DLQEntry, error) {
	return s.store.ListDLQ(ctx)
}

// ReplayDLQ gives a dead-lettered event a second life:
//
//  1. PrepareReplay removes its processed marker (fresh chance) and
//     resets the transfer to pending.
//  2. The "fix": if the payload carries the POISON reference, it is
//     rewritten to POISON-FIXED — this models an operator fixing the
//     root cause (or the downstream recovering) before replay.
//  3. The event is re-published to the main topic with its ORIGINAL
//     event_id — replay safety comes from the same dedup machinery as
//     everything else.
//  4. MarkReplayed closes the DLQ entry.
//
// A crash between steps can cause the event to be published twice or an
// entry to look un-replayed while its event is queued — both are safe:
// duplicates are dropped by the consumer, and replay itself is
// idempotent. That is rather the point of the whole system.
func (s *Service) ReplayDLQ(ctx context.Context, eventID string) (domain.Event, error) {
	if s.producer == nil {
		return domain.Event{}, errors.New("no Kafka producer configured in this process")
	}
	entry, err := s.store.PrepareReplay(ctx, eventID)
	if err != nil {
		return domain.Event{}, err
	}
	ev := entry.Payload
	if ev.Reference == domain.PoisonReference {
		ev.Reference = domain.FixedReference
	}
	if err := s.producer.Publish(ctx, domain.TopicTransfers, ev.FromAccount, ev); err != nil {
		return domain.Event{}, fmt.Errorf("republish event: %w", err)
	}
	if err := s.store.MarkReplayed(ctx, eventID); err != nil {
		return domain.Event{}, err
	}
	return ev, nil
}
