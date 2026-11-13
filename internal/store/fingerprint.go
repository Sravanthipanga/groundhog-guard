package store

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// Fingerprint hashes the payload an idempotency key was first used with.
// A later submission with the same key but a different fingerprint is a
// 409 Conflict; the same fingerprint is an idempotent replay.
func Fingerprint(req CreateTransferRequest) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%d|%s",
		req.FromAccount, req.ToAccount, req.AmountCents, req.Reference)))
	return hex.EncodeToString(sum[:])
}
