package broker

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/Sravanthipanga/onceonly/internal/domain"
)

// RecordedMessage is one message captured by FakeProducer.
type RecordedMessage struct {
	Topic   string
	Key     string
	Payload []byte
	Headers map[string]string
}

// Event decodes the payload as a domain.Event.
func (m RecordedMessage) Event() domain.Event {
	var ev domain.Event
	_ = json.Unmarshal(m.Payload, &ev)
	return ev
}

// FakeProducer is an in-memory Producer for tests. It can be told to fail
// (FailWith) to simulate a broker outage or a crash between the Kafka
// publish and the outbox mark — the scenario that creates real duplicates.
type FakeProducer struct {
	mu       sync.Mutex
	Messages []RecordedMessage
	Err      error // if set, every publish fails with this error
}

// NewFakeProducer returns a working fake producer.
func NewFakeProducer() *FakeProducer { return &FakeProducer{} }

func (p *FakeProducer) Publish(ctx context.Context, topic, key string, ev domain.Event) error {
	payload, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	return p.PublishRaw(ctx, topic, key, payload, nil)
}

func (p *FakeProducer) PublishRaw(_ context.Context, topic, key string, payload []byte, headers map[string]string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.Err != nil {
		return p.Err
	}
	cp := make([]byte, len(payload))
	copy(cp, payload)
	p.Messages = append(p.Messages, RecordedMessage{Topic: topic, Key: key, Payload: cp, Headers: headers})
	return nil
}

// Close is a no-op.
func (p *FakeProducer) Close() {}

// MessagesFor returns the recorded messages for one topic.
func (p *FakeProducer) MessagesFor(topic string) []RecordedMessage {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []RecordedMessage
	for _, m := range p.Messages {
		if m.Topic == topic {
			out = append(out, m)
		}
	}
	return out
}
