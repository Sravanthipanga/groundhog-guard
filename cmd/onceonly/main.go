// OnceOnly — send it twice, process it once.
//
// One binary, several subcommands, so Docker Compose can run each role
// as its own service from a single image:
//
//	onceonly api       HTTP ingest + query + ops API
//	onceonly relay     transactional-outbox relay (Postgres -> Kafka)
//	onceonly consumer  Kafka consumer group applying transfers exactly once
//	onceonly seed      create the demo accounts
//	onceonly chaos     trigger a chaos mode via the API (thin HTTP client)
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Sravanthipanga/onceonly/internal/api"
	"github.com/Sravanthipanga/onceonly/internal/broker"
	"github.com/Sravanthipanga/onceonly/internal/chaos"
	"github.com/Sravanthipanga/onceonly/internal/config"
	"github.com/Sravanthipanga/onceonly/internal/consumer"
	"github.com/Sravanthipanga/onceonly/internal/dedup"
	"github.com/Sravanthipanga/onceonly/internal/ledger"
	"github.com/Sravanthipanga/onceonly/internal/metrics"
	"github.com/Sravanthipanga/onceonly/internal/relay"
	"github.com/Sravanthipanga/onceonly/internal/service"
	"github.com/Sravanthipanga/onceonly/internal/store"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(log)

	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cfg := config.FromEnv()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var err error
	switch os.Args[1] {
	case "api":
		err = runAPI(ctx, cfg, log)
	case "relay":
		err = runRelay(ctx, cfg, log)
	case "consumer":
		err = runConsumer(ctx, cfg, log)
	case "seed":
		err = runSeed(ctx, cfg, log)
	case "chaos":
		err = runChaosCLI(cfg)
	case "version":
		fmt.Println("onceonly dev")
	default:
		usage()
		os.Exit(2)
	}
	if err != nil && ctx.Err() == nil {
		log.Error("fatal", "cmd", os.Args[1], "err", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: onceonly <api|relay|consumer|seed|chaos|version>")
	fmt.Fprintln(os.Stderr, "  chaos usage: onceonly chaos <duplicate-storm|shuffle|poison|hurricane> [count]")
}

// openStore connects to Postgres (with a short retry loop for compose
// startup races) and applies migrations.
func openStore(ctx context.Context, cfg config.Config, log *slog.Logger) (*store.PostgresStore, error) {
	var lastErr error
	for attempt := 0; attempt < 30; attempt++ {
		st, err := store.NewPostgresStore(ctx, cfg.DatabaseURL)
		if err == nil {
			if err := st.Migrate(ctx); err != nil {
				st.Close()
				return nil, fmt.Errorf("migrate: %w", err)
			}
			return st, nil
		}
		lastErr = err
		log.Info("waiting for postgres", "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(1 * time.Second):
		}
	}
	return nil, fmt.Errorf("postgres unreachable: %w", lastErr)
}

func runAPI(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	st, err := openStore(ctx, cfg, log)
	if err != nil {
		return err
	}
	defer st.Close()

	producer, err := broker.NewProducer(cfg.Brokers)
	if err != nil {
		return err
	}
	defer producer.Close()
	// Best effort: relay/consumer also provision topics.
	go func() {
		tctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := broker.EnsureTopics(tctx, cfg.Brokers); err != nil {
			log.Warn("topic provisioning (api)", "err", err)
		}
	}()

	svc := service.New(st, producer)
	handler := api.New(svc, chaos.New(svc, producer))

	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: handler}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	log.Info("api listening", "addr", cfg.HTTPAddr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func runRelay(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	st, err := openStore(ctx, cfg, log)
	if err != nil {
		return err
	}
	defer st.Close()

	producer, err := broker.NewProducer(cfg.Brokers)
	if err != nil {
		return err
	}
	defer producer.Close()
	if err := broker.EnsureTopics(ctx, cfg.Brokers); err != nil {
		log.Warn("topic provisioning (relay)", "err", err)
	}

	go func() {
		if err := metrics.Serve(cfg.MetricsAddr); err != nil {
			log.Error("metrics server", "err", err)
		}
	}()
	log.Info("relay started", "metrics", cfg.MetricsAddr)
	r := relay.New(st, producer, log)
	if err := r.Run(ctx); err != nil && ctx.Err() == nil {
		return err
	}
	return nil
}

func runConsumer(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	st, err := openStore(ctx, cfg, log)
	if err != nil {
		return err
	}
	defer st.Close()

	producer, err := broker.NewProducer(cfg.Brokers)
	if err != nil {
		return err
	}
	defer producer.Close()
	if err := broker.EnsureTopics(ctx, cfg.Brokers); err != nil {
		log.Warn("topic provisioning (consumer)", "err", err)
	}

	dedupStore := dedup.NewRedisStore(dedup.NewRedisClient(cfg.ValkeyAddr))
	processor := ledger.NewProcessor(st, dedupStore, producer)

	cons, err := consumer.New(cfg.Brokers, processor, log)
	if err != nil {
		return err
	}
	defer cons.Close()

	// Consumer metrics live on their own port (9091 by default).
	metricsAddr := cfg.MetricsAddr
	go func() {
		if err := metrics.Serve(metricsAddr); err != nil {
			log.Error("metrics server", "err", err)
		}
	}()
	log.Info("consumer started", "group", consumer.GroupName, "metrics", metricsAddr)
	if err := cons.Run(ctx); err != nil && ctx.Err() == nil {
		return err
	}
	return nil
}

func runSeed(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	st, err := openStore(ctx, cfg, log)
	if err != nil {
		return err
	}
	defer st.Close()
	svc := service.New(st, nil)
	accounts, err := svc.Seed(ctx)
	if err != nil {
		return err
	}
	var total int64
	for _, acc := range accounts {
		total += acc.BalanceCents
		fmt.Printf("%s  %-14s  %d cents\n", acc.ID, acc.Name, acc.BalanceCents)
	}
	fmt.Printf("total: %d cents across %d accounts (this number must never change)\n", total, len(accounts))
	return nil
}

// runChaosCLI is a thin client over POST /chaos/{mode} — the generator
// itself lives in the API process, next to the producer it attacks with.
func runChaosCLI(cfg config.Config) error {
	if len(os.Args) < 3 {
		usage()
		return fmt.Errorf("chaos mode required")
	}
	mode := os.Args[2]
	count := 0
	if len(os.Args) > 3 {
		n, err := strconv.Atoi(os.Args[3])
		if err != nil {
			return fmt.Errorf("count must be a number: %w", err)
		}
		count = n
	}
	body, _ := json.Marshal(map[string]int{"count": count})
	req, err := http.NewRequest(http.MethodPost, cfg.APIURL+"/chaos/"+mode, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	fmt.Printf("HTTP %d\n%s\n", resp.StatusCode, string(out))
	if resp.StatusCode >= 300 {
		return fmt.Errorf("chaos request failed")
	}
	return nil
}
