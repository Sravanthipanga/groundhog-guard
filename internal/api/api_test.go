package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Sravanthipanga/groundhog-guard/internal/api"
	"github.com/Sravanthipanga/groundhog-guard/internal/broker"
	"github.com/Sravanthipanga/groundhog-guard/internal/chaos"
	"github.com/Sravanthipanga/groundhog-guard/internal/dedup"
	"github.com/Sravanthipanga/groundhog-guard/internal/domain"
	"github.com/Sravanthipanga/groundhog-guard/internal/ledger"
	"github.com/Sravanthipanga/groundhog-guard/internal/service"
	"github.com/Sravanthipanga/groundhog-guard/internal/store"
)

type fixture struct {
	handler http.Handler
	svc     *service.Service
	st      *store.MemoryStore
	pub     *broker.FakeProducer
	proc    *ledger.Processor
	ctx     context.Context
	accs    []domain.Account
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx := context.Background()
	st := store.NewMemoryStore()
	pub := broker.NewFakeProducer()
	svc := service.New(st, pub)
	accs, err := svc.Seed(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{
		handler: api.New(svc, chaos.New(svc, pub)),
		svc:     svc,
		st:      st,
		pub:     pub,
		proc:    ledger.NewProcessor(st, dedup.NewMemoryStore(), pub),
		ctx:     ctx,
		accs:    accs,
	}
}

func (f *fixture) do(method, path, key string, body any) *httptest.ResponseRecorder {
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req := httptest.NewRequest(method, path, &buf)
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	rec := httptest.NewRecorder()
	f.handler.ServeHTTP(rec, req)
	return rec
}

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.NewDecoder(rec.Body).Decode(&v); err != nil {
		t.Fatalf("decode response: %v (body: %s)", err, rec.Body.String())
	}
	return v
}

func noSleep(time.Duration) {}

func (f *fixture) transferBody(amount int64, ref string) map[string]any {
	return map[string]any{
		"from_account": f.accs[0].ID,
		"to_account":   f.accs[1].ID,
		"amount_cents": amount,
		"reference":    ref,
	}
}

func TestCreateTransferThenIdempotentReplay(t *testing.T) {
	f := newFixture(t)
	body := f.transferBody(5000, "rent")

	first := f.do(http.MethodPost, "/transfers", "key-1", body)
	if first.Code != http.StatusCreated {
		t.Fatalf("first POST status = %d, body %s", first.Code, first.Body.String())
	}
	if h := first.Header().Get("X-Idempotent-Replay"); h != "" {
		t.Fatalf("first POST unexpectedly marked replay: %q", h)
	}
	created := decode[domain.Transfer](t, first)

	second := f.do(http.MethodPost, "/transfers", "key-1", body)
	if second.Code != http.StatusOK {
		t.Fatalf("replay POST status = %d, want 200", second.Code)
	}
	if h := second.Header().Get("X-Idempotent-Replay"); h != "true" {
		t.Fatalf("replay header = %q, want \"true\"", h)
	}
	replayed := decode[domain.Transfer](t, second)
	if replayed.ID != created.ID {
		t.Fatalf("replay returned a different transfer: %s vs %s", replayed.ID, created.ID)
	}

	// Exactly one transfer and one outbox event exist.
	if pending, _ := f.st.OutboxPending(f.ctx); pending != 1 {
		t.Fatalf("outbox pending = %d, want 1", pending)
	}
	stats := decode[domain.Stats](t, f.do(http.MethodGet, "/stats", "", nil))
	if stats.OutboxPending != 1 {
		t.Fatalf("stats outbox_pending = %d, want 1", stats.OutboxPending)
	}
}

func TestCreateTransferSameKeyDifferentPayloadConflicts(t *testing.T) {
	f := newFixture(t)
	if rec := f.do(http.MethodPost, "/transfers", "key-x", f.transferBody(100, "a")); rec.Code != http.StatusCreated {
		t.Fatalf("setup POST status = %d", rec.Code)
	}
	rec := f.do(http.MethodPost, "/transfers", "key-x", f.transferBody(200, "a"))
	if rec.Code != http.StatusConflict {
		t.Fatalf("conflicting POST status = %d, want 409 (body %s)", rec.Code, rec.Body.String())
	}
}

func TestCreateTransferValidation(t *testing.T) {
	f := newFixture(t)
	cases := []struct {
		name string
		key  string
		body map[string]any
		want int
	}{
		{"missing idempotency key", "", f.transferBody(100, "a"), http.StatusBadRequest},
		{"zero amount", "k", f.transferBody(0, "a"), http.StatusBadRequest},
		{"negative amount", "k", f.transferBody(-50, "a"), http.StatusBadRequest},
		{"same account", "k", map[string]any{
			"from_account": f.accs[0].ID, "to_account": f.accs[0].ID,
			"amount_cents": 100, "reference": "a"}, http.StatusBadRequest},
		{"unknown account", "k", map[string]any{
			"from_account": "00000000-0000-0000-0000-000000000000", "to_account": f.accs[1].ID,
			"amount_cents": 100, "reference": "a"}, http.StatusNotFound},
	}
	for _, tc := range cases {
		rec := f.do(http.MethodPost, "/transfers", tc.key, tc.body)
		if rec.Code != tc.want {
			t.Errorf("%s: status = %d, want %d (body %s)", tc.name, rec.Code, tc.want, rec.Body.String())
		}
	}
}

func TestAccountsReportInvariantTotal(t *testing.T) {
	f := newFixture(t)
	rec := f.do(http.MethodGet, "/accounts", "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /accounts status = %d", rec.Code)
	}
	resp := decode[struct {
		Accounts          []domain.Account `json:"accounts"`
		TotalBalanceCents int64            `json:"total_balance_cents"`
	}](t, rec)
	if len(resp.Accounts) != len(service.DemoAccounts) {
		t.Fatalf("accounts = %d, want %d", len(resp.Accounts), len(service.DemoAccounts))
	}
	// 25,000,000 + 18,500,000 + 32,750,000 + 9,400,000 + 41,200,000 + 12,300,000
	const wantTotal = 139_150_000
	if resp.TotalBalanceCents != wantTotal {
		t.Fatalf("total = %d, want %d", resp.TotalBalanceCents, wantTotal)
	}
	// A single-account view includes recent transfers (none yet).
	one := f.do(http.MethodGet, "/accounts/"+f.accs[0].ID, "", nil)
	if one.Code != http.StatusOK {
		t.Fatalf("GET /accounts/{id} status = %d", one.Code)
	}
	missing := f.do(http.MethodGet, "/accounts/00000000-0000-0000-0000-000000000000", "", nil)
	if missing.Code != http.StatusNotFound {
		t.Fatalf("GET unknown account status = %d, want 404", missing.Code)
	}
}

func TestDashboardServedAtRoot(t *testing.T) {
	f := newFixture(t)
	rec := f.do(http.MethodGet, "/", "", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET / status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("GET / Content-Type = %q, want text/html", ct)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"Groundhog Guard",
		"Send it twice. Process it once.",
		"Duplicate Storm",
		"Shuffle",
		"Poison Pill",
		"Hurricane",
		"Send the SAME request again",
		"/stats", "/accounts", "/dlq", "/chaos/",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("dashboard body missing %q", want)
		}
	}
}

func TestDLQListAndReplayOverHTTP(t *testing.T) {
	f := newFixture(t)

	// Create a poison transfer and drive it into the DLQ with the real
	// processor (the consumer's brain), as the consumer service would.
	res, err := f.svc.CreateTransfer(f.ctx, store.CreateTransferRequest{
		IdempotencyKey: "poison-http",
		FromAccount:    f.accs[0].ID, ToAccount: f.accs[1].ID,
		AmountCents: 777, Reference: domain.PoisonReference,
	})
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := f.proc.ProcessWithRetries(f.ctx, res.Event, 3, noSleep)
	if err != nil {
		t.Fatal(err)
	}
	if outcome != domain.OutcomeDeadLettered {
		t.Fatalf("outcome = %v, want dead_lettered", outcome)
	}

	list := f.do(http.MethodGet, "/dlq", "", nil)
	if list.Code != http.StatusOK {
		t.Fatalf("GET /dlq status = %d", list.Code)
	}
	dlqResp := decode[struct {
		Entries []domain.DLQEntry `json:"entries"`
	}](t, list)
	if len(dlqResp.Entries) != 1 {
		t.Fatalf("dlq entries = %d, want 1", len(dlqResp.Entries))
	}
	entry := dlqResp.Entries[0]
	if entry.EventID != res.Event.EventID || entry.Attempts != 3 {
		t.Fatalf("dlq entry = %+v", entry)
	}

	// Replay: 200, event republished with the fix applied.
	replay := f.do(http.MethodPost, fmt.Sprintf("/dlq/%s/replay", entry.EventID), "", nil)
	if replay.Code != http.StatusOK {
		t.Fatalf("replay status = %d, body %s", replay.Code, replay.Body.String())
	}
	msgs := f.pub.MessagesFor(domain.TopicTransfers)
	if len(msgs) != 1 {
		t.Fatalf("replayed publishes = %d, want 1", len(msgs))
	}
	if got := msgs[0].Event().Reference; got != domain.FixedReference {
		t.Fatalf("replayed reference = %q, want %q", got, domain.FixedReference)
	}

	// Second replay of the same entry: 409.
	again := f.do(http.MethodPost, fmt.Sprintf("/dlq/%s/replay", entry.EventID), "", nil)
	if again.Code != http.StatusConflict {
		t.Fatalf("second replay status = %d, want 409", again.Code)
	}
	// Unknown event: 404.
	unknown := f.do(http.MethodPost, "/dlq/00000000-0000-0000-0000-000000000000/replay", "", nil)
	if unknown.Code != http.StatusNotFound {
		t.Fatalf("unknown replay status = %d, want 404", unknown.Code)
	}

	// The consumer applies the replayed event; money finally moves.
	outcome, err = f.proc.Process(f.ctx, msgs[0].Event())
	if err != nil {
		t.Fatal(err)
	}
	if outcome != domain.OutcomeApplied {
		t.Fatalf("post-replay outcome = %v, want applied", outcome)
	}
	acc, err := f.svc.GetAccount(f.ctx, f.accs[1].ID)
	if err != nil {
		t.Fatal(err)
	}
	if acc.BalanceCents != f.accs[1].BalanceCents+777 {
		t.Fatalf("receiver balance = %d, want %d", acc.BalanceCents, f.accs[1].BalanceCents+777)
	}
}
