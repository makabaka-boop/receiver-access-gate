package store

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
)

// newID returns a random, URL-safe grant identifier.
func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return "g_" + hex.EncodeToString(b[:])
}

// newToken returns a plaintext owner token (shown to the caller exactly once)
// and its persisted SHA-256 hex digest.
func newToken() (plaintext, digest string) {
	var b [32]byte // 256 bits of entropy
	_, _ = rand.Read(b[:])
	plaintext = "tkn_" + hex.EncodeToString(b[:])
	return plaintext, digestToken(plaintext)
}

// digestToken returns the stored form of a token.
func digestToken(plaintext string) string {
	sum := sha256.Sum256([]byte(plaintext))
	return hex.EncodeToString(sum[:])
}

// tokenMatches compares a stored digest against a computed one in constant
// time.
func tokenMatches(stored, computed string) bool {
	a, errA := hex.DecodeString(stored)
	b, errB := hex.DecodeString(computed)
	if errA != nil || errB != nil {
		return false
	}
	return subtle.ConstantTimeCompare(a, b) == 1
}
