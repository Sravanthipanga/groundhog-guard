package dedup_test

import (
	"context"
	"testing"

	"github.com/Sravanthipanga/onceonly/internal/dedup"
)

func TestMemoryStoreSemantics(t *testing.T) {
	s := dedup.NewMemoryStore()
	ctx := context.Background()

	seen, err := s.Seen(ctx, "evt-1")
	if err != nil {
		t.Fatal(err)
	}
	if seen {
		t.Fatal("unknown event reported seen")
	}
	if err := s.MarkProcessed(ctx, "evt-1"); err != nil {
		t.Fatal(err)
	}
	seen, _ = s.Seen(ctx, "evt-1")
	if !seen {
		t.Fatal("marked event not seen")
	}
	// Marking is idempotent, and other events are unaffected.
	if err := s.MarkProcessed(ctx, "evt-1"); err != nil {
		t.Fatal(err)
	}
	seen, _ = s.Seen(ctx, "evt-2")
	if seen {
		t.Fatal("unrelated event reported seen")
	}
}
