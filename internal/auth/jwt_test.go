package auth

import (
	"crypto/hmac"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

var (
	secret = []byte("a-very-long-secret-of-at-least-32-chars")
	now    = time.Unix(1_800_000_000, 0)
)

// valid returns the claims of a capture token the way the API issues them.
func valid() Claims {
	return CaptureClaims("user-1", "tok-1", now.Add(-time.Minute), 365*24*time.Hour)
}

func TestAcceptsACaptureToken(t *testing.T) {
	claims, err := Verify(Sign(valid(), secret), secret, now)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if claims.Subject != "user-1" || claims.ID != "tok-1" || claims.Scope != CaptureScope {
		t.Fatalf("claims = %+v", claims)
	}
}

func TestAcceptsTheAudienceAsAList(t *testing.T) {
	c := valid()
	c.Audience = Audience{"something-else", CaptureAudience}
	if _, err := Verify(Sign(c, secret), secret, now); err != nil {
		t.Fatalf("aud list containing the capture audience: %v", err)
	}
}

func TestRejectsTokensThatAreNotCaptureTokens(t *testing.T) {
	cases := map[string]func(*Claims){
		"missing exp":   func(c *Claims) { c.ExpiresAt = 0 },
		"wrong aud":     func(c *Claims) { c.Audience = Audience{"centli-api"} },
		"missing aud":   func(c *Claims) { c.Audience = nil },
		"wrong iss":     func(c *Claims) { c.Issuer = "someone-else" },
		"missing iss":   func(c *Claims) { c.Issuer = "" },
		"wrong scope":   func(c *Claims) { c.Scope = "admin" },
		"missing scope": func(c *Claims) { c.Scope = "" },
		"missing jti":   func(c *Claims) { c.ID = "" },
		"missing sub":   func(c *Claims) { c.Subject = "" },
	}
	for name, mutate := range cases {
		c := valid()
		mutate(&c)
		if _, err := Verify(Sign(c, secret), secret, now); !errors.Is(err, ErrClaims) {
			t.Errorf("%s: want ErrClaims, got %v", name, err)
		}
	}
}

func TestRejectsAnAPIAccessToken(t *testing.T) {
	// What the API issues on login: its own secret, audience centli-api, no scope.
	access := Claims{
		Subject:   "user-1",
		ID:        "jti-1",
		Issuer:    Issuer,
		Audience:  Audience{"centli-api"},
		IssuedAt:  now.Unix(),
		ExpiresAt: now.Add(15 * time.Minute).Unix(),
	}
	accessSecret := []byte("the-access-token-secret-is-another-one")
	if _, err := Verify(Sign(access, accessSecret), secret, now); !errors.Is(err, ErrSignature) {
		t.Fatalf("access token with its own secret: want ErrSignature, got %v", err)
	}
	if _, err := Verify(Sign(access, secret), secret, now); !errors.Is(err, ErrClaims) {
		t.Fatalf("access-token claims even under the capture secret: want ErrClaims, got %v", err)
	}
}

func TestTimeWindow(t *testing.T) {
	expired := valid()
	expired.ExpiresAt = now.Unix()
	if _, err := Verify(Sign(expired, secret), secret, now); !errors.Is(err, ErrExpired) {
		t.Fatalf("expired: want ErrExpired, got %v", err)
	}
	early := valid()
	early.NotBefore = now.Add(time.Minute).Unix()
	if _, err := Verify(Sign(early, secret), secret, now); !errors.Is(err, ErrExpired) {
		t.Fatalf("not yet valid: want ErrExpired, got %v", err)
	}
}

func TestTamperedOrWronglySigned(t *testing.T) {
	token := Sign(valid(), secret)
	parts := strings.Split(token, ".")
	other := valid()
	other.Subject = "user-2"
	forged, _ := json.Marshal(other)
	parts[1] = base64.RawURLEncoding.EncodeToString(forged)
	if _, err := Verify(strings.Join(parts, "."), secret, now); !errors.Is(err, ErrSignature) {
		t.Fatalf("tampered payload: want ErrSignature, got %v", err)
	}
	if _, err := Verify(token, []byte("another-secret-that-is-long-enough-too"), now); !errors.Is(err, ErrSignature) {
		t.Fatalf("wrong secret: want ErrSignature, got %v", err)
	}
	if _, err := Verify(token, nil, now); !errors.Is(err, ErrSignature) {
		t.Fatalf("no secret configured: want ErrSignature, got %v", err)
	}
}

func TestRejectsOtherAlgorithms(t *testing.T) {
	payload, _ := json.Marshal(valid())
	unsigned := func(header string) string {
		return base64.RawURLEncoding.EncodeToString([]byte(header)) + "." + base64.RawURLEncoding.EncodeToString(payload)
	}

	if _, err := Verify(unsigned(`{"alg":"none","typ":"JWT"}`)+".", secret, now); !errors.Is(err, ErrMalformed) {
		t.Fatalf("alg none: want ErrMalformed, got %v", err)
	}

	// A correct HMAC-SHA512 signature still does not make HS512 acceptable.
	hs512 := unsigned(`{"alg":"HS512","typ":"JWT"}`)
	mac := hmac.New(sha512.New, secret)
	mac.Write([]byte(hs512))
	if _, err := Verify(hs512+"."+base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), secret, now); !errors.Is(err, ErrMalformed) {
		t.Fatalf("HS512: want ErrMalformed, got %v", err)
	}

	for _, alg := range []string{"hs256", "HS384", "RS256", ""} {
		if _, err := Verify(unsigned(`{"alg":"`+alg+`"}`)+".c2ln", secret, now); !errors.Is(err, ErrMalformed) {
			t.Errorf("alg %q: want ErrMalformed, got %v", alg, err)
		}
	}
}

func TestRejectsGarbage(t *testing.T) {
	for _, token := range []string{"", "a.b", "not.a.jwt.at.all", "!!!.???.***"} {
		if _, err := Verify(token, secret, now); !errors.Is(err, ErrMalformed) {
			t.Errorf("%q: want ErrMalformed, got %v", token, err)
		}
	}
}

func TestPaddedSegmentsAreAccepted(t *testing.T) {
	parts := strings.Split(Sign(valid(), secret), ".")
	parts[2] += "=="
	if _, err := Verify(strings.Join(parts, "."), secret, now); err != nil {
		t.Fatalf("padding should be tolerated: %v", err)
	}
}
