// Package dedup is the consumer's fast-path duplicate check, backed by
// Valkey/Redis.
//
// Design note (important): the key for an event is only written AFTER the
// Postgres transaction that applied the event has committed. A naive
// "SETNX before processing" is a footgun — if processing then fails, the
// retry would be dropped as a false duplicate and the event would be lost.
// Here Redis is purely an accelerator: a hit means "definitely processed"
// (because keys are only set post-commit), a miss means "ask Postgres",
// and Postgres — via the processed_events unique constraint inside the
// apply transaction — is always the source of truth.
package dedup

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const processedTTL = 24 * time.Hour

// Store is the fast-path dedup contract.
type Store interface {
	// Seen reports whether eventID is known to have been fully processed.
	Seen(ctx context.Context, eventID string) (bool, error)
	// MarkProcessed records eventID as processed. Called only after the
	// authoritative Postgres commit has succeeded.
	MarkProcessed(ctx context.Context, eventID string) error
}

func key(eventID string) string { return "processed:" + eventID }

// RedisStore implements Store against Valkey/Redis.
type RedisStore struct {
	client *redis.Client
	ttl    time.Duration
}

// NewRedisStore builds a RedisStore from an existing client.
func NewRedisStore(client *redis.Client) *RedisStore {
	return &RedisStore{client: client, ttl: processedTTL}
}

// NewRedisClient dials Valkey/Redis at addr.
func NewRedisClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{Addr: addr})
}

func (s *RedisStore) Seen(ctx context.Context, eventID string) (bool, error) {
	n, err := s.client.Exists(ctx, key(eventID)).Result()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func (s *RedisStore) MarkProcessed(ctx context.Context, eventID string) error {
	// SETNX semantics: set only if absent, with a TTL. The value is
	// irrelevant; existence is the signal.
	return s.client.SetNX(ctx, key(eventID), "1", s.ttl).Err()
}

// MemoryStore is an in-memory Store for tests and local hacking.
type MemoryStore struct {
	mu   sync.Mutex
	seen map[string]bool
}

// NewMemoryStore returns an empty in-memory dedup store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{seen: make(map[string]bool)}
}

func (s *MemoryStore) Seen(_ context.Context, eventID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seen[eventID], nil
}

func (s *MemoryStore) MarkProcessed(_ context.Context, eventID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seen[eventID] = true
	return nil
}
