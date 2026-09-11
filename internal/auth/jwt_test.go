package auth

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"
)

var secret = []byte("a-very-long-secret-of-at-least-32-chars")

func TestRoundTrip(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	token := Sign(Claims{Subject: "user-1", Email: "a@b.mx", ExpiresAt: now.Unix() + 60}, secret)
	claims, err := Verify(token, secret, now)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if claims.Subject != "user-1" || claims.Email != "a@b.mx" {
		t.Fatalf("claims = %+v", claims)
	}
}

func TestExpired(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	token := Sign(Claims{Subject: "user-1", ExpiresAt: now.Unix() - 1}, secret)
	if _, err := Verify(token, secret, now); !errors.Is(err, ErrExpired) {
		t.Fatalf("want ErrExpired, got %v", err)
	}
}

func TestTamperedPayload(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	token := Sign(Claims{Subject: "user-1", ExpiresAt: now.Unix() + 60}, secret)
	parts := strings.Split(token, ".")
	parts[1] = base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"user-2"}`))
	if _, err := Verify(strings.Join(parts, "."), secret, now); !errors.Is(err, ErrSignature) {
		t.Fatalf("want ErrSignature, got %v", err)
	}
	if _, err := Verify(token, []byte("another-secret-that-is-long-enough-too"), now); !errors.Is(err, ErrSignature) {
		t.Fatalf("want ErrSignature with wrong secret, got %v", err)
	}
}

func TestRejectsOtherAlgorithmsAndGarbage(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`))
	payload := base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"user-1"}`))
	if _, err := Verify(header+"."+payload+".", secret, now); !errors.Is(err, ErrMalformed) {
		t.Fatalf("want ErrMalformed for alg none, got %v", err)
	}
	if _, err := Verify("not.a.jwt.at.all", secret, now); !errors.Is(err, ErrMalformed) {
		t.Fatalf("want ErrMalformed, got %v", err)
	}
	if _, err := Verify("", secret, now); !errors.Is(err, ErrMalformed) {
		t.Fatalf("want ErrMalformed for empty, got %v", err)
	}
}

func TestPaddedSegmentsAreAccepted(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	token := Sign(Claims{Subject: "user-1"}, secret)
	parts := strings.Split(token, ".")
	parts[2] += "=="
	if _, err := Verify(strings.Join(parts, "."), secret, now); err != nil {
		t.Fatalf("padding should be tolerated: %v", err)
	}
}
