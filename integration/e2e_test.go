//go:build integration

// End-to-end integration tests: real Kafka (apache/kafka in KRaft mode),
// real Postgres 17, real Valkey — wired together exactly like the Docker
// Compose stack, with the app's own relay + consumer doing the work.
//
// Run with:  go test -tags integration -timeout 15m ./integration/...
//
// Without a reachable Docker daemon the tests skip cleanly.
package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/Sravanthipanga/onceonly/internal/broker"
	"github.com/Sravanthipanga/onceonly/internal/consumer"
	"github.com/Sravanthipanga/onceonly/internal/dedup"
	"github.com/Sravanthipanga/onceonly/internal/domain"
	"github.com/Sravanthipanga/onceonly/internal/ledger"
	"github.com/Sravanthipanga/onceonly/internal/relay"
	"github.com/Sravanthipanga/onceonly/internal/service"
	"github.com/Sravanthipanga/onceonly/internal/store"
)

func startContainer(t *testing.T, ctx context.Context, req testcontainers.ContainerRequest) testcontainers.Container {
	t.Helper()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start container %s: %v", req.Image, err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	return c
}

func hostPort(t *testing.T, ctx context.Context, c testcontainers.Container, port string) string {
	t.Helper()
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mapped, err := c.MappedPort(ctx, port)
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%s:%s", host, mapped.Port())
}

// TestEndToEndDuplicateStorm is the money test, against real infra:
// one transfer, its event published 51 times to a real Kafka topic, a
// real consumer group — and the balance moves exactly once.
func TestEndToEndDuplicateStorm(t *testing.T) {
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()

	// --- Postgres 17 ---
	pgContainer, err := tcpostgres.Run(ctx, "postgres:17",
		tcpostgres.WithDatabase("onceonly"),
		tcpostgres.WithUsername("onceonly"),
		tcpostgres.WithPassword("onceonly"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		t.Fatalf("postgres: %v", err)
	}
	t.Cleanup(func() { _ = pgContainer.Terminate(context.Background()) })
	dsn, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}

	// --- Valkey (protocol-compatible with Redis; go-redis speaks to it) ---
	valkey := startContainer(t, ctx, testcontainers.ContainerRequest{
		Image:        "valkey/valkey:8.0",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForListeningPort("6379/tcp").WithStartupTimeout(60 * time.Second),
	})
	valkeyAddr := hostPort(t, ctx, valkey, "6379/tcp")

	// --- Kafka (apache/kafka, KRaft — via the testcontainers module,
	// which wires the advertised listeners to the mapped host port;
	// hand-rolling that with a generic container is a classic trap) ---
	kafkaContainer, err := tckafka.Run(ctx, "apache/kafka:3.9.0",
		tckafka.WithClusterID("MkU3OEVBNTcwNTJENDM2Qk"),
	)
	if err != nil {
		t.Fatalf("kafka: %v", err)
	}
	t.Cleanup(func() { _ = kafkaContainer.Terminate(context.Background()) })
	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// --- App wiring (the real code paths) ---
	st, err := store.NewPostgresStore(ctx, dsn)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	producer, err := broker.NewProducer(brokers)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer producer.Close()
	if err := broker.EnsureTopics(ctx, brokers); err != nil {
		t.Fatalf("topics: %v", err)
	}

	svc := service.New(st, producer)
	accounts, err := svc.Seed(ctx)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	from, to := accounts[0], accounts[1]
	initialTotal := int64(0)
	for _, a := range accounts {
		initialTotal += a.BalanceCents
	}

	// One transfer via the service (API code path).
	res, err := svc.CreateTransfer(ctx, store.CreateTransferRequest{
		IdempotencyKey: "e2e-storm",
		FromAccount:    from.ID, ToAccount: to.ID,
		AmountCents: 12345, Reference: "e2e duplicate storm",
	})
	if err != nil {
		t.Fatalf("create transfer: %v", err)
	}

	// Relay: publish the outbox event for real.
	rly := relay.New(st, producer, nil)
	if n, err := rly.PublishOnce(ctx); err != nil || n != 1 {
		t.Fatalf("relay published %d, err %v; want 1", n, err)
	}

	// The storm: 50 extra copies of the same event, straight to the topic.
	for i := 0; i < 50; i++ {
		if err := producer.Publish(ctx, domain.TopicTransfers, res.Event.FromAccount, res.Event); err != nil {
			t.Fatalf("storm publish %d: %v", i, err)
		}
	}

	// Consumer: the real group, the real processor.
	processor := ledger.NewProcessor(st,
		dedup.NewRedisStore(dedup.NewRedisClient(valkeyAddr)), producer)
	cons, err := consumer.New(brokers, processor, nil)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	consCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = cons.Run(consCtx) }()
	defer cons.Close()

	// Wait for the dust to settle: 51 deliveries, 1 application.
	deadline := time.Now().Add(90 * time.Second)
	var stats domain.Stats
	for time.Now().Before(deadline) {
		stats, err = st.Stats(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if stats.TransfersApplied == 1 && stats.EventsReceived >= 51 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if stats.TransfersApplied != 1 {
		t.Fatalf("applied = %d, want 1 (stats %+v)", stats.TransfersApplied, stats)
	}
	if stats.DuplicatesDropped != 50 {
		t.Fatalf("duplicates dropped = %d, want 50 (received %d)", stats.DuplicatesDropped, stats.EventsReceived)
	}

	gotFrom, err := st.GetAccount(ctx, from.ID)
	if err != nil {
		t.Fatal(err)
	}
	gotTo, err := st.GetAccount(ctx, to.ID)
	if err != nil {
		t.Fatal(err)
	}
	if gotFrom.BalanceCents != from.BalanceCents-12345 {
		t.Fatalf("sender balance = %d, want %d", gotFrom.BalanceCents, from.BalanceCents-12345)
	}
	if gotTo.BalanceCents != to.BalanceCents+12345 {
		t.Fatalf("receiver balance = %d, want %d", gotTo.BalanceCents, to.BalanceCents+12345)
	}
	_, total, err := svc.ListAccounts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if total != initialTotal {
		t.Fatalf("INVARIANT BROKEN: total = %d, want %d", total, initialTotal)
	}
}
