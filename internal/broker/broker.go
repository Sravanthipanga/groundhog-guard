// Package broker wraps franz-go: a synchronous producer and topic
// provisioning. The consumer lives in internal/consumer because its
// poll/commit loop is application logic, not plumbing.
package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Sravanthipanga/onceonly/internal/domain"
)

// Producer publishes events to Kafka. Implementations: FranzProducer
// (real) and FakeProducer (tests).
type Producer interface {
	Publish(ctx context.Context, topic, key string, ev domain.Event) error
	PublishRaw(ctx context.Context, topic, key string, payload []byte, headers map[string]string) error
	Close()
}

// FranzProducer is a franz-go producer with idempotent-producer semantics
// for the produce path itself (Kafka-side dedup of producer retries).
type FranzProducer struct {
	client *kgo.Client
}

// NewProducer dials the brokers and returns a producer.
func NewProducer(brokers []string) (*FranzProducer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	return &FranzProducer{client: cl}, nil
}

// Client exposes the underlying client (the consumer shares broker config
// but builds its own client; this is for admin/diagnostics).
func (p *FranzProducer) Client() *kgo.Client { return p.client }

func (p *FranzProducer) Publish(ctx context.Context, topic, key string, ev domain.Event) error {
	payload, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	return p.PublishRaw(ctx, topic, key, payload, nil)
}

func (p *FranzProducer) PublishRaw(ctx context.Context, topic, key string, payload []byte, headers map[string]string) error {
	rec := &kgo.Record{
		Topic: topic,
		Key:   []byte(key),
		Value: payload,
	}
	for k, v := range headers {
		rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	// Synchronous produce: the relay only marks an outbox row published
	// after this returns, which is what makes the relay at-least-once.
	if err := p.client.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return fmt.Errorf("produce to %s: %w", topic, err)
	}
	return nil
}

func (p *FranzProducer) Close() { p.client.Close() }

// EnsureTopics creates the application topics if they do not exist.
// Safe to call from every process at startup.
func EnsureTopics(ctx context.Context, brokers []string) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	createCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	resp, err := adm.CreateTopics(createCtx, 6, 1, nil,
		domain.TopicTransfers, domain.TopicDLQ)
	if err != nil {
		return fmt.Errorf("create topics: %w", err)
	}
	for _, r := range resp.Sorted() {
		if r.Err != nil && !isAlreadyExists(r.Err) {
			return fmt.Errorf("create topic %s: %w", r.Topic, r.Err)
		}
	}
	return nil
}

func isAlreadyExists(err error) bool {
	return err != nil && (strings.Contains(err.Error(), "already exists") ||
		strings.Contains(err.Error(), "TOPIC_ALREADY_EXISTS"))
}
