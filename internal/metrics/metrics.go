// Package metrics defines the Prometheus metrics exported by every
// onceonly process. The consumer is the process that moves the ledger
// counters; other processes register them too (they simply stay at zero),
// so Grafana sums across instances.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	EventsReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "onceonly_events_received_total",
		Help: "Event deliveries seen by the consumer (including duplicates).",
	})
	DuplicatesDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "onceonly_duplicates_dropped_total",
		Help: "Deliveries dropped because the event was already processed.",
	})
	TransfersApplied = promauto.NewCounter(prometheus.CounterOpts{
		Name: "onceonly_transfers_applied_total",
		Help: "Transfers applied to the ledger (exactly once per event).",
	})
	DLQTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "onceonly_dlq_total",
		Help: "Events written to the dead letter queue.",
	})
	OutboxPending = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "onceonly_outbox_pending",
		Help: "Outbox rows not yet published to Kafka (set by the relay).",
	})
	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "onceonly_http_requests_total",
		Help: "HTTP requests handled by the API, by route and status.",
	}, []string{"route", "status"})
)

// Handler returns the /metrics HTTP handler.
func Handler() http.Handler {
	return promhttp.Handler()
}

// Serve starts a tiny standalone metrics HTTP server (used by the relay
// and consumer processes, which have no other HTTP surface). It blocks
// until the server fails; callers run it in a goroutine.
func Serve(addr string) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return http.ListenAndServe(addr, mux)
}
