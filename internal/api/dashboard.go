package api

import (
	_ "embed"
	"net/http"
)

// dashboardHTML is the live web dashboard: a single self-contained page
// (all CSS/JS inline, no external assets, no build step) embedded into
// the binary and served by the api process at GET /. It polls /stats,
// /accounts and /dlq, fires the chaos modes, replays DLQ entries, and
// demos the idempotency-key replay with a double-submit button.
//
//go:embed web/index.html
var dashboardHTML []byte

func (s *Server) dashboard(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write(dashboardHTML)
}
