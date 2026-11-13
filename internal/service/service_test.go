package service_test

import (
	"context"
	"testing"

	"github.com/Sravanthipanga/onceonly/internal/service"
	"github.com/Sravanthipanga/onceonly/internal/store"
)

func TestSeedIsIdempotentAndTotalIsConstant(t *testing.T) {
	ctx := context.Background()
	svc := service.New(store.NewMemoryStore(), nil)

	first, err := svc.Seed(ctx)
	if err != nil {
		t.Fatal(err)
	}
	second, err := svc.Seed(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != len(service.DemoAccounts) || len(second) != len(service.DemoAccounts) {
		t.Fatalf("seeded %d then %d accounts, want %d", len(first), len(second), len(service.DemoAccounts))
	}
	for i := range first {
		if first[i].ID != second[i].ID {
			t.Fatalf("re-seed created a new account for %q", first[i].Name)
		}
	}
	accounts, total, err := svc.ListAccounts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(accounts) != len(service.DemoAccounts) {
		t.Fatalf("accounts = %d", len(accounts))
	}
	if total != 139_150_000 {
		t.Fatalf("total = %d, want 139150000", total)
	}
}
