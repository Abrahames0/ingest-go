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
	entries []queue.Entry
	seen    map[string]bool
	count   map[string]int
	revoked map[string]bool
	pingErr error
	downErr error
}

func newFakeStore() *fakeStore {
	return &fakeStore{seen: map[string]bool{}, count: map[string]int{}, revoked: map[string]bool{}}
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

func (f *fakeStore) IsRevoked(_ context.Context, jti string) (bool, error) {
	if f.downErr != nil {
		return false, f.downErr
	}
	return f.revoked[jti], nil
}

func (f *fakeStore) Close() error { return nil }

// ── Helpers ───────────────────────────────────────────

const secret = "a-test-secret-that-is-long-enough-32"

var frozen = time.Date(2026, 9, 10, 15, 0, 0, 0, time.UTC)

func newTestServer() (*Server, *fakeStore) {
	cfg := config.Config{
		Port:            "0",
		APIKey:          "api-key",
		JWTSecret:       secret,
		StreamKey:       "capture:test",
		MaxTextLength:   2000,
		RatePerMinute:   3,
		IPRatePerMinute: 100,
		DedupeTTL:       time.Hour,
	}
	store := newFakeStore()
	srv := New(cfg, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv.Now = func() time.Time { return frozen }
	return srv, store
}

func token(sub string, exp time.Time) string {
	return auth.Sign(auth.Claims{Subject: sub, ExpiresAt: exp.Unix()}, []byte(secret))
}

func captureToken(sub, jti string, exp time.Time) string {
	return auth.Sign(auth.Claims{Subject: sub, Scope: "capture", ID: jti, ExpiresAt: exp.Unix()}, []byte(secret))
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

func authed(sub string) map[string]string {
	return map[string]string{
		"x-api-key":     "api-key",
		"Authorization": "Bearer " + token(sub, frozen.Add(time.Hour)),
	}
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
	srv, _ := newTestServer()
	body := `{"text":"Compra por $350"}`

	if rec := do(srv, "POST", "/ingest/notification", body, nil); rec.Code != 401 {
		t.Fatalf("no api key: %d", rec.Code)
	}
	if rec := do(srv, "POST", "/ingest/notification", body, map[string]string{"x-api-key": "api-key"}); rec.Code != 401 {
		t.Fatalf("no token: %d", rec.Code)
	}
	bad := map[string]string{"x-api-key": "api-key", "Authorization": "Bearer nope"}
	if rec := do(srv, "POST", "/ingest/notification", body, bad); rec.Code != 401 {
		t.Fatalf("bad token: %d", rec.Code)
	}
	expired := map[string]string{"x-api-key": "api-key", "Authorization": "Bearer " + token("u1", frozen.Add(-time.Second))}
	if rec := do(srv, "POST", "/ingest/notification", body, expired); rec.Code != 401 {
		t.Fatalf("expired token: %d", rec.Code)
	}
	wrongKey := map[string]string{"x-api-key": "other", "Authorization": "Bearer " + token("u1", frozen.Add(time.Hour))}
	if rec := do(srv, "POST", "/ingest/notification", body, wrongKey); rec.Code != 401 {
		t.Fatalf("wrong api key: %d", rec.Code)
	}
}

func TestCaptureTokens(t *testing.T) {
	srv, store := newTestServer()
	body := `{"text":"Compra por $350 en OXXO","title":"BBVA"}`
	headers := map[string]string{"x-api-key": "api-key", "Authorization": "Bearer " + captureToken("u1", "tok-1", frozen.Add(365*24*time.Hour))}

	if rec := do(srv, "POST", "/ingest/notification", body, headers); rec.Code != 202 {
		t.Fatalf("capture token accepted: %d %s", rec.Code, rec.Body.String())
	}
	if len(store.entries) != 1 || store.entries[0].UserID != "u1" {
		t.Fatalf("entry not queued for the token's user: %+v", store.entries)
	}
	store.revoked["tok-1"] = true
	if rec := do(srv, "POST", "/ingest/notification", body, headers); rec.Code != 401 {
		t.Fatalf("revoked capture token: %d", rec.Code)
	}
	store.revoked["tok-1"] = false
	store.downErr = fmt.Errorf("redis down")
	if rec := do(srv, "POST", "/ingest/notification", body, headers); rec.Code != 503 {
		t.Fatalf("revocation check without redis: %d", rec.Code)
	}
}

func TestIngestQueuesNormalizedEntry(t *testing.T) {
	srv, store := newTestServer()
	body := `{"text":"  Compra   por $350 en  OXXO ","title":" BBVA ","packageName":"com.bancomer.mbanking","postedAt":"2026-09-10T14:32:00-06:00","deviceId":"pixel"}`
	rec := do(srv, "POST", "/ingest/notification", body, authed("user-1"))
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
	if rec := do(srv, "POST", "/ingest/notification", body, authed("user-1")); rec.Code != 202 {
		t.Fatalf("first: %d", rec.Code)
	}
	rec := do(srv, "POST", "/ingest/notification", body, authed("user-1"))
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
	if rec := do(srv, "POST", "/ingest/notification", body, authed("user-2")); rec.Code != 202 {
		t.Fatalf("other user: %d", rec.Code)
	}
}

func TestIngestValidation(t *testing.T) {
	srv, _ := newTestServer()
	cases := map[string]string{
		"empty text":   `{"text":"   "}`,
		"bad date":     `{"text":"Compra por $350","postedAt":"ayer"}`,
		"future date":  `{"text":"Compra por $350","postedAt":"2027-01-01T00:00:00Z"}`,
		"bad source":   `{"text":"Compra por $350","source":"EMAIL"}`,
		"long text":    fmt.Sprintf(`{"text":"%s"}`, strings.Repeat("a", 2001)),
		"invalid json": `{"text":`,
	}
	for name, body := range cases {
		if rec := do(srv, "POST", "/ingest/notification", body, authed("user-1")); rec.Code != 400 {
			t.Errorf("%s: want 400, got %d %s", name, rec.Code, rec.Body.String())
		}
	}
}

func TestRateLimit(t *testing.T) {
	srv, _ := newTestServer() // limit 3 per minute
	for i := 0; i < 3; i++ {
		body := fmt.Sprintf(`{"text":"Compra por $%d"}`, i)
		if rec := do(srv, "POST", "/ingest/notification", body, authed("user-1")); rec.Code != 202 {
			t.Fatalf("request %d: %d", i, rec.Code)
		}
	}
	if rec := do(srv, "POST", "/ingest/notification", `{"text":"Compra por $9"}`, authed("user-1")); rec.Code != 429 {
		t.Fatalf("want 429, got %d", rec.Code)
	}
	if rec := do(srv, "POST", "/ingest/notification", `{"text":"Compra por $9"}`, authed("user-2")); rec.Code != 202 {
		t.Fatalf("other user should pass: %d", rec.Code)
	}
}

func TestQueueDown(t *testing.T) {
	srv, store := newTestServer()
	store.downErr = fmt.Errorf("redis down")
	if rec := do(srv, "POST", "/ingest/notification", `{"text":"Compra por $1"}`, authed("user-1")); rec.Code != 503 {
		t.Fatalf("want 503, got %d", rec.Code)
	}
}

func TestBatch(t *testing.T) {
	srv, store := newTestServer()
	body := `{"items":[{"text":"Compra por $1"},{"text":"Compra por $2"},{"text":"   "},{"text":"Compra por $1"}]}`
	rec := do(srv, "POST", "/ingest/notifications", body, authed("user-1"))
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
	if rec := do(srv, "POST", "/ingest/notifications", `{"items":[]}`, authed("user-1")); rec.Code != 400 {
		t.Fatalf("empty batch: %d", rec.Code)
	}
}

func TestPayloadTooLarge(t *testing.T) {
	srv, _ := newTestServer()
	body := fmt.Sprintf(`{"text":"%s"}`, strings.Repeat("a", maxBody+1))
	if rec := do(srv, "POST", "/ingest/notification", body, authed("user-1")); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("want 413, got %d %s", rec.Code, rec.Body.String())
	}
}

func TestPostedAtTooOld(t *testing.T) {
	srv, store := newTestServer()
	old := frozen.Add(-91 * 24 * time.Hour).Format(time.RFC3339)
	if rec := do(srv, "POST", "/ingest/notification", `{"text":"Compra por $1","postedAt":"`+old+`"}`, authed("user-1")); rec.Code != 400 {
		t.Fatalf("91 days old: want 400, got %d", rec.Code)
	}
	recent := frozen.Add(-30 * 24 * time.Hour).Format(time.RFC3339)
	if rec := do(srv, "POST", "/ingest/notification", `{"text":"Compra por $1","postedAt":"`+recent+`"}`, authed("user-1")); rec.Code != 202 {
		t.Fatalf("30 days old: want 202, got %d %s", rec.Code, rec.Body.String())
	}
	if len(store.entries) != 1 {
		t.Fatalf("entries = %d", len(store.entries))
	}
}

func TestIPRateLimitRunsBeforeAuth(t *testing.T) {
	srv, _ := newTestServer()
	srv.cfg.IPRatePerMinute = 2
	bad := map[string]string{"x-api-key": "wrong", "X-Forwarded-For": "203.0.113.9, 10.0.0.1"}
	for i := 0; i < 2; i++ {
		if rec := do(srv, "POST", "/ingest/notification", `{"text":"x"}`, bad); rec.Code != 401 {
			t.Fatalf("attempt %d: want 401, got %d", i, rec.Code)
		}
	}
	if rec := do(srv, "POST", "/ingest/notification", `{"text":"x"}`, bad); rec.Code != 429 {
		t.Fatalf("third attempt from the same address: want 429, got %d", rec.Code)
	}
	other := map[string]string{"x-api-key": "wrong", "X-Forwarded-For": "198.51.100.4"}
	if rec := do(srv, "POST", "/ingest/notification", `{"text":"x"}`, other); rec.Code != 401 {
		t.Fatalf("another address is not affected: %d", rec.Code)
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
	srv, _ := newTestServer()
	req := httptest.NewRequest("POST", "/ingest/notification", strings.NewReader("text=hola"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	for k, v := range authed("user-1") {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("want 415, got %d", rec.Code)
	}
}
