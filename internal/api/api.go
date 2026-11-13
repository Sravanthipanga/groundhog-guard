// Package api is the HTTP surface of Groundhog Guard: ingest with idempotency
// keys, account/transfer queries, stats, DLQ inspection + replay, and
// the chaos triggers. Handlers are thin adapters over internal/service.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/Sravanthipanga/groundhog-guard/internal/chaos"
	"github.com/Sravanthipanga/groundhog-guard/internal/domain"
	"github.com/Sravanthipanga/groundhog-guard/internal/metrics"
	"github.com/Sravanthipanga/groundhog-guard/internal/service"
	"github.com/Sravanthipanga/groundhog-guard/internal/store"
)

// Server wires the service + chaos runner into an http.Handler.
type Server struct {
	svc   *service.Service
	chaos *chaos.Runner
}

// New builds the HTTP handler.
func New(svc *service.Service, chaosRunner *chaos.Runner) http.Handler {
	s := &Server{svc: svc, chaos: chaosRunner}
	r := chi.NewRouter()
	r.Use(s.observe)
	r.Post("/transfers", s.createTransfer)
	r.Get("/transfers/{id}", s.getTransfer)
	r.Get("/accounts", s.listAccounts)
	r.Get("/accounts/{id}", s.getAccount)
	r.Get("/stats", s.getStats)
	r.Get("/dlq", s.listDLQ)
	r.Post("/dlq/{event_id}/replay", s.replayDLQ)
	r.Post("/chaos/{mode}", s.runChaos)
	r.Get("/metrics", metrics.Handler().ServeHTTP)
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return r
}

// observe counts requests by route pattern and status for Prometheus.
func (s *Server) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		route := chi.RouteContext(r.Context()).RoutePattern()
		if route == "" {
			route = "unmatched"
		}
		metrics.HTTPRequests.WithLabelValues(route, strconv.Itoa(sw.status)).Inc()
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

type createTransferBody struct {
	FromAccount string `json:"from_account"`
	ToAccount   string `json:"to_account"`
	AmountCents int64  `json:"amount_cents"`
	Reference   string `json:"reference"`
}

func (s *Server) createTransfer(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "missing Idempotency-Key header")
		return
	}
	var body createTransferBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	res, err := s.svc.CreateTransfer(r.Context(), store.CreateTransferRequest{
		IdempotencyKey: key,
		FromAccount:    body.FromAccount,
		ToAccount:      body.ToAccount,
		AmountCents:    body.AmountCents,
		Reference:      body.Reference,
	})
	if err != nil {
		writeServiceError(w, err)
		return
	}
	if res.Replay {
		// The original transfer, returned as-is. The header (and the
		// 200 vs 201) is how clients know this was a replay.
		w.Header().Set("X-Idempotent-Replay", "true")
		writeJSON(w, http.StatusOK, res.Transfer)
		return
	}
	writeJSON(w, http.StatusCreated, res.Transfer)
}

func (s *Server) getTransfer(w http.ResponseWriter, r *http.Request) {
	tr, err := s.svc.GetTransfer(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, tr)
}

func (s *Server) listAccounts(w http.ResponseWriter, r *http.Request) {
	accounts, total, err := s.svc.ListAccounts(r.Context())
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"accounts":            accounts,
		"total_balance_cents": total,
	})
}

func (s *Server) getAccount(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	acc, err := s.svc.GetAccount(r.Context(), id)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	recent, err := s.svc.RecentTransfers(r.Context(), id, 20)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"account":          acc,
		"recent_transfers": recent,
	})
}

func (s *Server) getStats(w http.ResponseWriter, r *http.Request) {
	st, err := s.svc.Stats(r.Context())
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) listDLQ(w http.ResponseWriter, r *http.Request) {
	entries, err := s.svc.ListDLQ(r.Context())
	if err != nil {
		writeServiceError(w, err)
		return
	}
	if entries == nil {
		entries = []domain.DLQEntry{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": entries})
}

func (s *Server) replayDLQ(w http.ResponseWriter, r *http.Request) {
	eventID := chi.URLParam(r, "event_id")
	ev, err := s.svc.ReplayDLQ(r.Context(), eventID)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"replayed": true,
		"event":    ev,
	})
}

func (s *Server) runChaos(w http.ResponseWriter, r *http.Request) {
	if s.chaos == nil {
		writeError(w, http.StatusServiceUnavailable, "chaos runner not configured")
		return
	}
	var body struct {
		Count int `json:"count"`
	}
	// Body is optional; ignore decode errors for an empty body.
	_ = json.NewDecoder(r.Body).Decode(&body)
	summary, err := s.chaos.Run(r.Context(), chi.URLParam(r, "mode"), body.Count)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, summary)
}

func writeServiceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		writeError(w, http.StatusNotFound, "not found")
	case errors.Is(err, domain.ErrConflict):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, domain.ErrAlreadyReplayed):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, domain.ErrInvalidEvent):
		writeError(w, http.StatusBadRequest, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, fmt.Sprintf("internal error: %v", err))
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
