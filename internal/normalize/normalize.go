// Package normalize cleans notification text before it is queued, the same
// way the API does, so repeats collapse early and payloads stay small.
package normalize

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"unicode"
)

// Text trims, drops zero-width characters, turns every kind of space into a
// plain one and collapses runs of whitespace.
func Text(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	pendingSpace := false
	for _, r := range s {
		switch {
		case r == 0x200B || r == 0x200C || r == 0x200D || r == 0xFEFF:
			continue
		case unicode.IsSpace(r):
			pendingSpace = true
		default:
			if pendingSpace && b.Len() > 0 {
				b.WriteByte(' ')
			}
			pendingSpace = false
			b.WriteRune(r)
		}
	}
	return b.String()
}

// Clip cuts a string to at most n runes.
func Clip(s string, n int) string {
	runes := []rune(s)
	if len(runes) <= n {
		return s
	}
	return string(runes[:n])
}

// Fingerprint identifies "the same notification on the same day" for the
// early duplicate filter. The API computes its own, stricter one.
func Fingerprint(userID, title, text, day string) string {
	key := userID + "|" + strings.ToLower(Text(title)) + "|" + strings.ToLower(Text(text)) + "|" + day
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}
