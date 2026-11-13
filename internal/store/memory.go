package store

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/Sravanthipanga/groundhog-guard/internal/domain"
)

// MemoryStore is an in-memory Store whose semantics mirror the Postgres
// implementation (unique constraints, atomic multi-row updates). A single
// mutex serialises everything, which models the serialisable behaviour
// the SQL gets from row locks and unique constraints.
type MemoryStore struct {
	mu           sync.Mutex
	accounts     map[string]*domain.Account // by id
	accountNames map[string]string          // name -> id
	transfers    map[string]*domain.Transfer
	keyIndex     map[string]string // idempotency_key -> transfer id
	hashIndex    map[string]string // transfer id -> request hash
	outbox       []*outboxEntry
	processed    map[string]string // event_id -> transfer_id
	dlq          map[string]*domain.DLQEntry
	dlqOrder     []string // event_ids in insertion order
	counters     map[string]int64
	nextOutboxID int64
	nextDLQID    int64
}

type outboxEntry struct {
	row       domain.OutboxRow
	published bool
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		accounts:     make(map[string]*domain.Account),
		accountNames: make(map[string]string),
		transfers:    make(map[string]*domain.Transfer),
		keyIndex:     make(map[string]string),
		hashIndex:    make(map[string]string),
		processed:    make(map[string]string),
		dlq:          make(map[string]*domain.DLQEntry),
		counters:     map[string]int64{CounterEventsReceived: 0, CounterDuplicatesDropped: 0},
	}
}

// Migrate is a no-op for the in-memory store.
func (s *MemoryStore) Migrate(_ context.Context) error { return nil }

func (s *MemoryStore) EnsureAccount(_ context.Context, name string, openingBalanceCents int64) (domain.Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id, ok := s.accountNames[name]; ok {
		return *s.accounts[id], nil
	}
	acc := &domain.Account{
		ID:           uuid.NewString(),
		Name:         name,
		BalanceCents: openingBalanceCents,
		UpdatedAt:    time.Now(),
	}
	s.accounts[acc.ID] = acc
	s.accountNames[name] = acc.ID
	return *acc, nil
}

func (s *MemoryStore) GetAccount(_ context.Context, id string) (domain.Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	acc, ok := s.accounts[id]
	if !ok {
		return domain.Account{}, domain.ErrNotFound
	}
	return *acc, nil
}

func (s *MemoryStore) ListAccounts(_ context.Context) ([]domain.Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]domain.Account, 0, len(s.accounts))
	for _, acc := range s.accounts {
		out = append(out, *acc)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (s *MemoryStore) ListTransfersByAccount(_ context.Context, accountID string, limit int) ([]domain.Transfer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.Transfer
	for _, tr := range s.transfers {
		if tr.FromAccount == accountID || tr.ToAccount == accountID {
			out = append(out, *tr)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *MemoryStore) CreateTransfer(_ context.Context, req CreateTransferRequest) (CreateTransferResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.accounts[req.FromAccount]; !ok {
		return CreateTransferResult{}, domain.ErrNotFound
	}
	if _, ok := s.accounts[req.ToAccount]; !ok {
		return CreateTransferResult{}, domain.ErrNotFound
	}

	if existingID, ok := s.keyIndex[req.IdempotencyKey]; ok {
		existing := s.transfers[existingID]
		if s.hashIndex[existingID] != Fingerprint(req) {
			return CreateTransferResult{}, domain.ErrConflict
		}
		ev := s.eventForTransferLocked(existingID)
		return CreateTransferResult{Transfer: *existing, Event: ev, Replay: true}, nil
	}

	tr := &domain.Transfer{
		ID:             uuid.NewString(),
		IdempotencyKey: req.IdempotencyKey,
		FromAccount:    req.FromAccount,
		ToAccount:      req.ToAccount,
		AmountCents:    req.AmountCents,
		Reference:      req.Reference,
		Status:         domain.StatusPending,
		CreatedAt:      time.Now(),
	}
	ev := domain.Event{
		EventID:     uuid.NewString(),
		TransferID:  tr.ID,
		Type:        domain.EventTransferRequested,
		FromAccount: req.FromAccount,
		ToAccount:   req.ToAccount,
		AmountCents: req.AmountCents,
		Reference:   req.Reference,
		OccurredAt:  time.Now(),
	}
	payload, err := json.Marshal(ev)
	if err != nil {
		return CreateTransferResult{}, err
	}
	tr.EventID = ev.EventID
	s.transfers[tr.ID] = tr
	s.keyIndex[req.IdempotencyKey] = tr.ID
	s.hashIndex[tr.ID] = Fingerprint(req)
	s.nextOutboxID++
	s.outbox = append(s.outbox, &outboxEntry{row: domain.OutboxRow{
		ID:         s.nextOutboxID,
		EventID:    ev.EventID,
		TransferID: tr.ID,
		Topic:      domain.TopicTransfers,
		Key:        req.FromAccount,
		Payload:    payload,
	}})
	return CreateTransferResult{Transfer: *tr, Event: ev, Replay: false}, nil
}

func (s *MemoryStore) eventForTransferLocked(transferID string) domain.Event {
	for _, e := range s.outbox {
		if e.row.TransferID == transferID {
			var ev domain.Event
			_ = json.Unmarshal(e.row.Payload, &ev)
			return ev
		}
	}
	return domain.Event{}
}

func (s *MemoryStore) GetTransfer(_ context.Context, id string) (domain.Transfer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tr, ok := s.transfers[id]
	if !ok {
		return domain.Transfer{}, domain.ErrNotFound
	}
	return *tr, nil
}

func (s *MemoryStore) WithOutboxBatch(_ context.Context, limit int, fn func(rows []domain.OutboxRow) error) error {
	s.mu.Lock()
	var batch []*outboxEntry
	for _, e := range s.outbox {
		if !e.published {
			batch = append(batch, e)
			if len(batch) >= limit {
				break
			}
		}
	}
	rows := make([]domain.OutboxRow, len(batch))
	for i, e := range batch {
		rows[i] = e.row
	}
	s.mu.Unlock()

	if err := fn(rows); err != nil {
		return err // nothing marked: the batch will be published again
	}

	s.mu.Lock()
	for _, e := range batch {
		e.published = true
	}
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) OutboxPending(_ context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int64
	for _, e := range s.outbox {
		if !e.published {
			n++
		}
	}
	return n, nil
}

func (s *MemoryStore) ApplyEvent(_ context.Context, ev domain.Event) (ApplyResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, dup := s.processed[ev.EventID]; dup {
		return ApplyDuplicate, nil
	}
	from, okFrom := s.accounts[ev.FromAccount]
	to, okTo := s.accounts[ev.ToAccount]
	if !okFrom || !okTo {
		return 0, domain.ErrNotFound
	}

	// The marker goes in first, inside the same "transaction" (mutex
	// hold) as the balance update — exactly like the SQL.
	s.processed[ev.EventID] = ev.TransferID

	if from.BalanceCents < ev.AmountCents {
		if tr, ok := s.transfers[ev.TransferID]; ok {
			tr.Status = domain.StatusFailedInsufficientFunds
		}
		s.addDLQLocked(ev, domain.ErrInsufficientFunds.Error(), 1)
		return ApplyInsufficientFunds, nil
	}

	from.BalanceCents -= ev.AmountCents
	from.UpdatedAt = time.Now()
	to.BalanceCents += ev.AmountCents
	to.UpdatedAt = time.Now()
	if tr, ok := s.transfers[ev.TransferID]; ok {
		tr.Status = domain.StatusApplied
	}
	return ApplyApplied, nil
}

func (s *MemoryStore) RecordDeadLetter(_ context.Context, ev domain.Event, errMsg string, attempts int) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.dlq[ev.EventID]; exists {
		return false, nil
	}
	s.addDLQLocked(ev, errMsg, attempts)
	if tr, ok := s.transfers[ev.TransferID]; ok {
		tr.Status = domain.StatusDeadLettered
	}
	return true, nil
}

// addDLQLocked inserts a DLQ entry if absent. Caller must hold s.mu.
func (s *MemoryStore) addDLQLocked(ev domain.Event, errMsg string, attempts int) {
	if _, exists := s.dlq[ev.EventID]; exists {
		return
	}
	s.nextDLQID++
	s.dlq[ev.EventID] = &domain.DLQEntry{
		ID:         s.nextDLQID,
		EventID:    ev.EventID,
		TransferID: ev.TransferID,
		Payload:    ev,
		Error:      errMsg,
		Attempts:   attempts,
		CreatedAt:  time.Now(),
	}
	s.dlqOrder = append(s.dlqOrder, ev.EventID)
}

func (s *MemoryStore) ListDLQ(_ context.Context) ([]domain.DLQEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]domain.DLQEntry, 0, len(s.dlqOrder))
	for _, id := range s.dlqOrder {
		out = append(out, *s.dlq[id])
	}
	return out, nil
}

func (s *MemoryStore) PrepareReplay(_ context.Context, eventID string) (domain.DLQEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.dlq[eventID]
	if !ok {
		return domain.DLQEntry{}, domain.ErrNotFound
	}
	if entry.ReplayedAt != nil {
		return domain.DLQEntry{}, domain.ErrAlreadyReplayed
	}
	// Give the event a fresh chance: drop its processed marker (present
	// for insufficient-funds entries) so the consumer will apply it,
	// and reset the transfer to pending so its status can move again.
	delete(s.processed, eventID)
	if tr, ok := s.transfers[entry.TransferID]; ok {
		tr.Status = domain.StatusPending
	}
	return *entry, nil
}

func (s *MemoryStore) MarkReplayed(_ context.Context, eventID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.dlq[eventID]
	if !ok {
		return domain.ErrNotFound
	}
	now := time.Now()
	entry.ReplayedAt = &now
	return nil
}

func (s *MemoryStore) IncrementCounter(_ context.Context, name string, delta int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.counters[name] += delta
	return nil
}

func (s *MemoryStore) Stats(_ context.Context) (domain.Stats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := domain.Stats{
		EventsReceived:    s.counters[CounterEventsReceived],
		DuplicatesDropped: s.counters[CounterDuplicatesDropped],
	}
	for _, tr := range s.transfers {
		if tr.Status == domain.StatusApplied {
			st.TransfersApplied++
		}
	}
	for _, e := range s.dlq {
		if e.ReplayedAt == nil {
			st.DLQCount++
		}
	}
	for _, e := range s.outbox {
		if !e.published {
			st.OutboxPending++
		}
	}
	return st, nil
}
