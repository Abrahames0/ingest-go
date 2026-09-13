// Package auth verifies the capture tokens the Centli API issues for the iOS
// Shortcut and the Android listener: HS256 JWTs signed with
// CAPTURE_TOKEN_SECRET, a secret of their own that never signs access tokens.
// Standard library only.
package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"
)

// Every capture token carries these values. They are what tells it apart
// from an API access token (audience centli-api, no scope).
const (
	Issuer          = "centli"
	CaptureAudience = "centli-capture"
	CaptureScope    = "capture"
)

// Claims mirrors the API's payload: `sub` is the user id and `jti` the id of
// the capture token, which the API lists as active in Redis.
type Claims struct {
	Subject   string   `json:"sub"`
	Email     string   `json:"email,omitempty"`
	Scope     string   `json:"scope,omitempty"`
	ID        string   `json:"jti,omitempty"`
	Issuer    string   `json:"iss,omitempty"`
	Audience  Audience `json:"aud,omitempty"`
	IssuedAt  int64    `json:"iat,omitempty"`
	NotBefore int64    `json:"nbf,omitempty"`
	ExpiresAt int64    `json:"exp,omitempty"`
}

// Audience is the `aud` claim, which RFC 7519 allows as one string or a list.
type Audience []string

func (a *Audience) UnmarshalJSON(data []byte) error {
	var one string
	if err := json.Unmarshal(data, &one); err == nil {
		*a = Audience{one}
		return nil
	}
	var many []string
	if err := json.Unmarshal(data, &many); err != nil {
		return err
	}
	*a = many
	return nil
}

func (a Audience) MarshalJSON() ([]byte, error) {
	if len(a) == 1 {
		return json.Marshal(a[0])
	}
	return json.Marshal([]string(a))
}

var (
	ErrMalformed = errors.New("malformed token")
	ErrSignature = errors.New("bad signature")
	ErrClaims    = errors.New("not a capture token")
	ErrExpired   = errors.New("token expired or not yet valid")
)

func decodeSegment(s string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(strings.TrimRight(s, "="))
}

// Verify accepts only a capture token: HS256 signed with secret, from the
// Centli issuer, for the capture audience and scope, naming a user and a
// token id, and inside its validity window. Whether the API still lists the
// token as active is the caller's check.
func Verify(token string, secret []byte, now time.Time) (Claims, error) {
	if len(secret) == 0 {
		return Claims{}, ErrSignature
	}
	parts := strings.Split(strings.TrimSpace(token), ".")
	if len(parts) != 3 {
		return Claims{}, ErrMalformed
	}
	headerJSON, err := decodeSegment(parts[0])
	if err != nil {
		return Claims{}, ErrMalformed
	}
	var header struct {
		Alg string `json:"alg"`
	}
	// The algorithm is fixed here, never taken from the token: "none", HS512
	// or RS256 are refused before the signature is even looked at.
	if err := json.Unmarshal(headerJSON, &header); err != nil || header.Alg != "HS256" {
		return Claims{}, ErrMalformed
	}
	signature, err := decodeSegment(parts[2])
	if err != nil {
		return Claims{}, ErrMalformed
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(parts[0] + "." + parts[1]))
	if !hmac.Equal(mac.Sum(nil), signature) {
		return Claims{}, ErrSignature
	}
	payload, err := decodeSegment(parts[1])
	if err != nil {
		return Claims{}, ErrMalformed
	}
	var claims Claims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return Claims{}, ErrMalformed
	}
	if claims.Subject == "" || claims.ID == "" || claims.ExpiresAt == 0 ||
		claims.Scope != CaptureScope || claims.Issuer != Issuer ||
		!slices.Contains(claims.Audience, CaptureAudience) {
		return Claims{}, ErrClaims
	}
	unix := now.Unix()
	if unix >= claims.ExpiresAt {
		return Claims{}, ErrExpired
	}
	if claims.NotBefore != 0 && unix < claims.NotBefore {
		return Claims{}, ErrExpired
	}
	return claims, nil
}

// CaptureClaims are the claims of a capture token as the API issues it.
func CaptureClaims(subject, id string, issuedAt time.Time, ttl time.Duration) Claims {
	return Claims{
		Subject:   subject,
		ID:        id,
		Scope:     CaptureScope,
		Issuer:    Issuer,
		Audience:  Audience{CaptureAudience},
		IssuedAt:  issuedAt.Unix(),
		ExpiresAt: issuedAt.Add(ttl).Unix(),
	}
}

// Sign issues an HS256 token with exactly these claims. The service never
// signs in production; this backs the tests and cmd/token.
func Sign(claims Claims, secret []byte) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payload, _ := json.Marshal(claims)
	body := header + "." + base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(body))
	return body + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
