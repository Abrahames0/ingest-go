// Package auth verifies the HS256 access tokens the Centli API issues
// (@nestjs/jwt with JWT_ACCESS_SECRET), using the standard library only.
package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// Claims mirrors the API's JwtPayload: `sub` is the user id. Capture tokens
// (the iOS Shortcut, the Android listener) carry scope "capture" and a jti
// the API can revoke through Redis.
type Claims struct {
	Subject   string `json:"sub"`
	Email     string `json:"email,omitempty"`
	Scope     string `json:"scope,omitempty"`
	ID        string `json:"jti,omitempty"`
	IssuedAt  int64  `json:"iat,omitempty"`
	NotBefore int64  `json:"nbf,omitempty"`
	ExpiresAt int64  `json:"exp,omitempty"`
}

var (
	ErrMalformed = errors.New("malformed token")
	ErrSignature = errors.New("bad signature")
	ErrExpired   = errors.New("token expired or not yet valid")
)

func decodeSegment(s string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(strings.TrimRight(s, "="))
}

// Verify checks the HS256 signature and the time claims.
func Verify(token string, secret []byte, now time.Time) (Claims, error) {
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
	if err := json.Unmarshal(payload, &claims); err != nil || claims.Subject == "" {
		return Claims{}, ErrMalformed
	}
	unix := now.Unix()
	if claims.ExpiresAt != 0 && unix >= claims.ExpiresAt {
		return Claims{}, ErrExpired
	}
	if claims.NotBefore != 0 && unix < claims.NotBefore {
		return Claims{}, ErrExpired
	}
	return claims, nil
}

// Sign issues an HS256 token. The service never signs in production; this
// backs the tests and manual checks.
func Sign(claims Claims, secret []byte) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payload, _ := json.Marshal(claims)
	body := header + "." + base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(body))
	return body + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
