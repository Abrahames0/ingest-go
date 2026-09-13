package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xonix/centli-ingest/internal/auth"
	"github.com/xonix/centli-ingest/internal/config"
	"github.com/xonix/centli-ingest/internal/queue"
)

// ── Fake store ────────────────────────────────────────

type fakeStore struct {
	entries   []queue.Entry
	seen      map[string]bool
	count     map[string]int
	active    map[string]string // capture token id → user id, like capture:active:{jti}
	pingErr   error
	downErr   error // every Redis call fails
	activeErr error // only the allow-list lookup fails
}

func newFakeStore() *fakeStore {
	return &fakeStore{seen: map[string]bool{}, count: map[string]int{}, active: map[string]string{}}
}

func (f *fakeStore) Ping(context.Context) error { return f.pingErr }

func (f *fakeStore) Allow(_ context.Context, bucket string, limit int) (bool, error) {
	if f.downErr != nil {
		return false, f.downErr
	}
	f.count[bucket]++
	return f.count[bucket] <= limit, nil
}

func (f *fakeStore) FirstSeen(_ context.Context, key string, _ time.Duration) (bool, error) {
	if f.seen[key] {
		return false, nil
	}
	f.seen[key] = true
	return true, nil
}

func (f *fakeStore) Enqueue(_ context.Context, e queue.Entry) (string, error) {
	f.entries = append(f.entries, e)
	return fmt.Sprintf("%d-0", len(f.entries)), nil
}

func (f *fakeStore) IsActive(_ context.Context, jti, sub string) (bool, error) {
	if f.downErr != nil {
		return false, f.downErr
	}
	if f.activeErr != nil {
		return false, f.activeErr
	}
	return sub != "" && f.active[jti] == sub, nil
}

func (f *fakeStore) Close() error { return nil }

// ── Helpers ───────────────────────────────────────────

const secret = "a-test-secret-that-is-long-enough-32"

var frozen = time.Date(2026, 9, 10, 15, 0, 0, 0, time.UTC)

func newTestServer() (*Server, *fakeStore) {
	cfg := config.Config{
		Port:               "0",
		APIKey:             "api-key",
		CaptureTokenSecret: secret,
		StreamKey:          "capture:test",
		MaxTextLength:      2000,
		RatePerMinute:      3,
		IPRatePerMinute:    100,
		DedupeTTL:          time.Hour,
	}
	store := newFakeStore()
	srv := New(cfg, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv.Now = func() time.Time { return frozen }
	return srv, store
}

// captureToken signs a token like the ones POST /api/v1/capture/tokens returns.
func captureToken(sub, jti string, exp time.Time) string {
	claims := auth.CaptureClaims(sub, jti, frozen.Add(-time.Hour), time.Hour)
	claims.ExpiresAt = exp.Unix()
	return auth.Sign(claims, []byte(secret))
}

func bearer(token string) map[string]string {
	return map[string]string{"x-api-key": "api-key", "Authorization": "Bearer " + token}
}

// authed returns the headers of a request with a capture token that the store
// lists as active for sub.
func authed(store *fakeStore, sub string) map[string]string {
	jti := "tok-" + sub
	store.active[jti] = sub
	return bearer(captureToken(sub, jti, frozen.Add(365*24*time.Hour)))
}

func do(srv *Server, method, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	return rec
}

func decodeBody[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("invalid JSON %q: %v", rec.Body.String(), err)
	}
	return v
}

// ── Tests ─────────────────────────────────────────────

func TestHealth(t *testing.T) {
	srv, store := newTestServer()
	if rec := do(srv, "GET", "/healthz", "", nil); rec.Code != 200 || !strings.Contains(rec.Body.String(), `"ok"`) {
		t.Fatalf("health: %d %s", rec.Code, rec.Body.String())
	}
	store.pingErr = fmt.Errorf("down")
	if rec := do(srv, "GET", "/healthz", "", nil); rec.Code != 503 {
		t.Fatalf("degraded health: %d", rec.Code)
	}
}

func TestAuth(t *testing.T) {
	srv, store := newTestServer()
	body := `{"text":"Compra por $350"}`
	live := authed(store, "u1")

	if rec := do(srv, "POST", "/ingest/notification", body, nil); rec.Code != 401 {
		t.Fatalf("no api key: %d", rec.Code)
	}
	if rec := do(srv, "POST", "/ingest/notification", body, map[string]string{"x-api-key": "api-key"}); rec.Code != 401 {
		t.Fatalf("no token: %d", rec.Code)
	}
	if rec := do(srv, "POST", "/ingest/notification", body, bearer("nope")); rec.Code != 401 {
		t.Fatalf("bad token: %d", rec.Code)
	}
	if rec := do(srv, "POST", "/ingest/notification", body, bearer(captureToken("u1", "tok-u1", frozen.Add(-time.Second)))); rec.Code != 401 {
		t.Fatalf("expired token: %d", rec.Code)
	}
	wrongKey := map[string]string{"x-api-key": "other", "Authorization": live["Authorization"]}
	if rec := do(srv, "POST", "/ingest/notification", body, wrongKey); rec.Code != 401 {
		t.Fatalf("wrong api key: %d", rec.Code)
	}
	if rec := do(srv, "POST", "/ingest/notification", body, live); rec.Code != 202 {
		t.Fatalf("live capture token: %d %s", rec.Code, rec.Body.String())
	}
}

func TestRejectsAPIAccessTokens(t *testing.T) {
	srv, store := newTestServer()
	store.active["jti-1"] = "u1" // even if its id happened to be listed
	access := auth.Claims{
		Subject:   "u1",
		ID:        "jti-1",
		Issuer:    auth.Issuer,
		Audience:  auth.Audience{"centli-api"},
		IssuedAt:  frozen.Unix(),
		ExpiresAt: frozen.Add(15 * time.Minute).Unix(),
	}
	for name, key := range map[string]string{
		"signed with the access secret":  "the-api-access-token-secret-is-other",
		"signed with the capture secret": secret,
	} {
		rec := do(srv, "POST", "/ingest/notification", `{"text":"Compra por $350"}`, bearer(auth.Sign(access, []byte(key))))
		if rec.Code != 401 {
			t.Errorf("%s: want 401, got %d", name, rec.Code)
		}
	}
	if len(store.entries) != 0 {
		t.Fatalf("nothing may be queued: %+v", store.entries)
	}
}

func TestCaptureTokens(t *testing.T) {
	srv, store := newTestServer()
	body := `{"text":"Compra por $350 en OXXO","title":"BBVA"}`
	headers := bearer(captureToken("u1", "tok-1", frozen.Add(365*24*time.Hour)))

	// Signed correctly but not listed: revoked, never issued or Redis was wiped.
	if rec := do(srv, "POST", "/ingest/notification", body, headers); rec.Code != 401 {
		t.Fatalf("unlisted capture token: %d", rec.Code)
	}
	store.active["tok-1"] = "u2"
	if rec := do(srv, "POST", "/ingest/notification", body, headers); rec.Code != 401 {
		t.Fatalf("token id listed for another user: %d", rec.Code)
	}
	store.active["tok-1"] = "u1"
	if rec := do(srv, "POST", "/ingest/notification", body, headers); rec.Code != 202 {
		t.Fatalf("listed capture token: %d %s", rec.Code, rec.Body.String())
	}
	if len(store.entries) != 1 || store.entries[0].UserID != "u1" {
		t.Fatalf("entry not queued for the token's user: %+v", store.entries)
	}
	delete(store.active, "tok-1")
	if rec := do(srv, "POST", "/ingest/notification", body, headers); rec.Code != 401 {
		t.Fatalf("revoked capture token: %d", rec.Code)
	}
	store.active["tok-1"] = "u1"
	store.activeErr = fmt.Errorf("redis down")
	if rec := do(srv, "POST", "/ingest/notification", body, headers); rec.Code != 503 {
		t.Fatalf("allow-list check without redis: %d", rec.Code)
	}
}

func TestIngestQueuesNormalizedEntry(t *testing.T) {
	srv, store := newTestServer()
	body := `{"text":"  Compra   por $350 en  OXXO ","title":" BBVA ","packageName":"com.bancomer.mbanking","postedAt":"2026-09-10T14:32:00-06:00","deviceId":"pixel"}`
	rec := do(srv, "POST", "/ingest/notification", body, authed(store, "user-1"))
	if rec.Code != 202 {
		t.Fatalf("want 202, got %d %s", rec.Code, rec.Body.String())
	}
	out := decodeBody[outcome](t, rec)
	if !out.Queued || out.ID != "1-0" {
		t.Fatalf("outcome = %+v", out)
	}
	if len(store.entries) != 1 {
		t.Fatalf("entries = %d", len(store.entries))
	}
	e := store.entries[0]
	want := queue.Entry{
		UserID:      "user-1",
		Source:      "NOTIFICATION",
		PackageName: "com.bancomer.mbanking",
		Title:       "BBVA",
		Text:        "Compra por $350 en OXXO",
		PostedAt:    "2026-09-10T20:32:00.000Z",
		ReceivedAt:  "2026-09-10T15:00:00.000Z",
		DeviceID:    "pixel",
	}
	if e != want {
		t.Fatalf("entry = %+v, want %+v", e, want)
	}
}

func TestIngestDuplicateIsNotQueuedTwice(t *testing.T) {
	srv, store := newTestServer()
	body := `{"text":"Compra por $350 en OXXO","postedAt":"2026-09-10T14:32:00-06:00"}`
	if rec := do(srv, "POST", "/ingest/notification", body, authed(store, "user-1")); rec.Code != 202 {
		t.Fatalf("first: %d", rec.Code)
	}
	rec := do(srv, "POST", "/ingest/notification", body, authed(store, "user-1"))
	if rec.Code != 200 {
		t.Fatalf("second: want 200, got %d", rec.Code)
	}
	if out := decodeBody[outcome](t, rec); !out.Duplicate || out.Queued {
		t.Fatalf("outcome = %+v", out)
	}
	if len(store.entries) != 1 {
		t.Fatalf("entries = %d", len(store.entries))
	}
	// Same text for another user is not a duplicate.
	if rec := do(srv, "POST", "/ingest/notification", body, authed(store, "user-2")); rec.Code != 202 {
		t.Fatalf("other user: %d", rec.Code)
	}
}

func TestIngestValidation(t *testing.T) {
	srv, store := newTestServer()
	cases := map[string]string{
		"empty text":   `{"text":"   "}`,
		"bad date":     `{"text":"Compra por $350","postedAt":"ayer"}`,
		"future date":  `{"text":"Compra por $350","postedAt":"2027-01-01T00:00:00Z"}`,
		"bad source":   `{"text":"Compra por $350","source":"EMAIL"}`,
		"long text":    fmt.Sprintf(`{"text":"%s"}`, strings.Repeat("a", 2001)),
		"invalid json": `{"text":`,
	}
	for name, body := range cases {
		if rec := do(srv, "POST", "/ingest/notification", body, authed(store, "user-1")); rec.Code != 400 {
			t.Errorf("%s: want 400, got %d %s", name, rec.Code, rec.Body.String())
		}
	}
}

func TestRateLimit(t *testing.T) {
	srv, store := newTestServer() // limit 3 per minute
	for i := 0; i < 3; i++ {
		body := fmt.Sprintf(`{"text":"Compra por $%d"}`, i)
		if rec := do(srv, "POST", "/ingest/notification", body, authed(store, "user-1")); rec.Code != 202 {
			t.Fatalf("request %d: %d", i, rec.Code)
		}
	}
	if rec := do(srv, "POST", "/ingest/notification", `{"text":"Compra por $9"}`, authed(store, "user-1")); rec.Code != 429 {
		t.Fatalf("want 429, got %d", rec.Code)
	}
	if rec := do(srv, "POST", "/ingest/notification", `{"text":"Compra por $9"}`, authed(store, "user-2")); rec.Code != 202 {
		t.Fatalf("other user should pass: %d", rec.Code)
	}
}

func TestQueueDown(t *testing.T) {
	srv, store := newTestServer()
	headers := authed(store, "user-1")
	store.downErr = fmt.Errorf("redis down")
	if rec := do(srv, "POST", "/ingest/notification", `{"text":"Compra por $1"}`, headers); rec.Code != 503 {
		t.Fatalf("want 503, got %d", rec.Code)
	}
}

func TestBatch(t *testing.T) {
	srv, store := newTestServer()
	body := `{"items":[{"text":"Compra por $1"},{"text":"Compra por $2"},{"text":"   "},{"text":"Compra por $1"}]}`
	rec := do(srv, "POST", "/ingest/notifications", body, authed(store, "user-1"))
	if rec.Code != 200 {
		t.Fatalf("want 200, got %d %s", rec.Code, rec.Body.String())
	}
	resp := decodeBody[batchResponse](t, rec)
	if resp.Received != 4 || resp.Queued != 2 || resp.Rejected != 1 || resp.Duplicates != 1 {
		t.Fatalf("batch = %+v", resp)
	}
	if len(resp.Results) != 4 || resp.Results[2].Error == "" {
		t.Fatalf("results = %+v", resp.Results)
	}
	if len(store.entries) != 2 {
		t.Fatalf("entries = %d", len(store.entries))
	}
	if rec := do(srv, "POST", "/ingest/notifications", `{"items":[]}`, authed(store, "user-1")); rec.Code != 400 {
		t.Fatalf("empty batch: %d", rec.Code)
	}
}

func TestPayloadTooLarge(t *testing.T) {
	srv, store := newTestServer()
	body := fmt.Sprintf(`{"text":"%s"}`, strings.Repeat("a", maxBody+1))
	if rec := do(srv, "POST", "/ingest/notification", body, authed(store, "user-1")); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("want 413, got %d %s", rec.Code, rec.Body.String())
	}
}

func TestPostedAtTooOld(t *testing.T) {
	srv, store := newTestServer()
	old := frozen.Add(-91 * 24 * time.Hour).Format(time.RFC3339)
	if rec := do(srv, "POST", "/ingest/notification", `{"text":"Compra por $1","postedAt":"`+old+`"}`, authed(store, "user-1")); rec.Code != 400 {
		t.Fatalf("91 days old: want 400, got %d", rec.Code)
	}
	recent := frozen.Add(-30 * 24 * time.Hour).Format(time.RFC3339)
	if rec := do(srv, "POST", "/ingest/notification", `{"text":"Compra por $1","postedAt":"`+recent+`"}`, authed(store, "user-1")); rec.Code != 202 {
		t.Fatalf("30 days old: want 202, got %d %s", rec.Code, rec.Body.String())
	}
	if len(store.entries) != 1 {
		t.Fatalf("entries = %d", len(store.entries))
	}
}

func TestIPRateLimitRunsBeforeAuth(t *testing.T) {
	srv, _ := newTestServer()
	srv.cfg.IPRatePerMinute = 2
	srv.cfg.TrustProxyHops = 1 // behind Traefik
	attempt := func(xff string) int {
		headers := map[string]string{"x-api-key": "wrong", "X-Forwarded-For": xff}
		return do(srv, "POST", "/ingest/notification", `{"text":"x"}`, headers).Code
	}
	for i := 0; i < 2; i++ {
		if code := attempt("6.6.6.6, 203.0.113.9"); code != 401 {
			t.Fatalf("attempt %d: want 401, got %d", i, code)
		}
	}
	// Rewriting the left entry, which the client controls, does not dodge the brake.
	if code := attempt("7.7.7.7, 203.0.113.9"); code != 429 {
		t.Fatalf("third attempt from the same address: want 429, got %d", code)
	}
	if code := attempt("6.6.6.6, 198.51.100.4"); code != 401 {
		t.Fatalf("another address is not affected: %d", code)
	}
}

func TestClientIPTrustsOnlyTheConfiguredHops(t *testing.T) {
	cases := []struct {
		name   string
		hops   int
		xff    []string
		remote string
		want   string
	}{
		{"no proxy ignores the header", 0, []string{"6.6.6.6"}, "198.51.100.20:5000", "198.51.100.20"},
		{"behind Traefik", 1, []string{"6.6.6.6, 203.0.113.9"}, "10.0.1.5:40000", "203.0.113.9"},
		{"Cloudflare then Traefik", 2, []string{"6.6.6.6, 203.0.113.9, 172.70.4.2"}, "10.0.1.5:40000", "203.0.113.9"},
		{"several header lines", 1, []string{"6.6.6.6", "203.0.113.9"}, "10.0.1.5:40000", "203.0.113.9"},
		{"more hops than entries", 3, []string{"203.0.113.9"}, "10.0.1.5:40000", "203.0.113.9"},
		{"proxy expected but no header", 1, nil, "198.51.100.20:5000", "198.51.100.20"},
	}
	for _, c := range cases {
		req := httptest.NewRequest("POST", "/ingest/notification", nil)
		req.RemoteAddr = c.remote
		for _, v := range c.xff {
			req.Header.Add("X-Forwarded-For", v)
		}
		if got := clientIP(req, c.hops); got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}

func TestHealthReportsVersion(t *testing.T) {
	srv, _ := newTestServer()
	Version, Commit = "1.2.3", "abc1234"
	defer func() { Version, Commit = "dev", "unknown" }()
	rec := do(srv, "GET", "/healthz", "", nil)
	body := decodeBody[map[string]any](t, rec)
	if body["version"] != "1.2.3" || body["commit"] != "abc1234" || body["status"] != "ok" {
		t.Fatalf("health = %v", body)
	}
}

func TestRejectsNonJSON(t *testing.T) {
	srv, store := newTestServer()
	req := httptest.NewRequest("POST", "/ingest/notification", strings.NewReader("text=hola"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	for k, v := range authed(store, "user-1") {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("want 415, got %d", rec.Code)
	}
}
