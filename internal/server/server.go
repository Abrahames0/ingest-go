// Package server is the HTTP layer: API key + JWT, validation, rate limit,
// duplicate filter, enqueue.
package server

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
	_ "time/tzdata" // Mexico City day boundaries without relying on the host tz database

	"github.com/xonix/centli-ingest/internal/auth"
	"github.com/xonix/centli-ingest/internal/config"
	"github.com/xonix/centli-ingest/internal/normalize"
	"github.com/xonix/centli-ingest/internal/queue"
)

const (
	maxBody    = 256 << 10 // a batch of 50 notifications of 2 KB fits comfortably
	maxBatch   = 50
	timeLayout = "2006-01-02T15:04:05.000Z07:00"
)

type Server struct {
	cfg   config.Config
	store queue.Store
	log   *slog.Logger
	// Now is swappable for tests.
	Now func() time.Time
	tz  *time.Location
}

func New(cfg config.Config, store queue.Store, log *slog.Logger) *Server {
	tz, err := time.LoadLocation("America/Mexico_City")
	if err != nil {
		tz = time.FixedZone("CST", -6*3600)
	}
	return &Server{cfg: cfg, store: store, log: log, Now: time.Now, tz: tz}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.Handle("POST /ingest/notification", s.authenticated(s.ingestOne))
	mux.Handle("POST /ingest/notifications", s.authenticated(s.ingestBatch))
	return s.recovered(s.logged(mux))
}

// ── Payloads ──────────────────────────────────────────

// notification is the same shape the API accepts on POST /capture/notifications.
type notification struct {
	Text        string `json:"text"`
	Title       string `json:"title"`
	PackageName string `json:"packageName"`
	AppName     string `json:"appName"`
	PostedAt    string `json:"postedAt"`
	DeviceID    string `json:"deviceId"`
	Source      string `json:"source"`
}

type outcome struct {
	Queued    bool   `json:"queued"`
	Duplicate bool   `json:"duplicate"`
	ID        string `json:"id,omitempty"`
	Error     string `json:"error,omitempty"`
}

type batchRequest struct {
	Items []notification `json:"items"`
}

type batchResponse struct {
	Received   int       `json:"received"`
	Queued     int       `json:"queued"`
	Duplicates int       `json:"duplicates"`
	Rejected   int       `json:"rejected"`
	Results    []outcome `json:"results"`
}

// ── Handlers ──────────────────────────────────────────

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.store.Ping(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "degraded", "redis": "down"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "redis": "up", "stream": s.cfg.StreamKey})
}

func (s *Server) ingestOne(w http.ResponseWriter, r *http.Request, userID string) {
	var n notification
	if err := decode(w, r, &n); err != nil {
		fail(w, http.StatusBadRequest, err.Error())
		return
	}
	code, out := s.ingest(r.Context(), userID, n)
	if code >= 400 {
		fail(w, code, out.Error)
		return
	}
	writeJSON(w, code, out)
}

func (s *Server) ingestBatch(w http.ResponseWriter, r *http.Request, userID string) {
	var b batchRequest
	if err := decode(w, r, &b); err != nil {
		fail(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(b.Items) == 0 {
		fail(w, http.StatusBadRequest, "items is required")
		return
	}
	if len(b.Items) > maxBatch {
		fail(w, http.StatusBadRequest, fmt.Sprintf("at most %d items per batch", maxBatch))
		return
	}
	resp := batchResponse{Received: len(b.Items), Results: make([]outcome, 0, len(b.Items))}
	for _, n := range b.Items {
		code, out := s.ingest(r.Context(), userID, n)
		switch {
		case code == http.StatusAccepted:
			resp.Queued++
		case out.Duplicate:
			resp.Duplicates++
		default:
			resp.Rejected++
		}
		resp.Results = append(resp.Results, out)
	}
	writeJSON(w, http.StatusOK, resp)
}

// ingest validates, rate-limits, filters repeats and enqueues one notification.
func (s *Server) ingest(ctx context.Context, userID string, n notification) (int, outcome) {
	e, err := s.entry(userID, n)
	if err != nil {
		return http.StatusBadRequest, outcome{Error: err.Error()}
	}
	allowed, err := s.store.Allow(ctx, userID, s.cfg.RatePerMinute)
	if err != nil {
		return http.StatusServiceUnavailable, outcome{Error: "queue unavailable"}
	}
	if !allowed {
		return http.StatusTooManyRequests, outcome{Error: "too many notifications this minute"}
	}
	fingerprint := normalize.Fingerprint(userID, e.Title, e.Text, s.dayOf(e.PostedAt))
	fresh, err := s.store.FirstSeen(ctx, fingerprint, s.cfg.DedupeTTL)
	if err != nil {
		return http.StatusServiceUnavailable, outcome{Error: "queue unavailable"}
	}
	if !fresh {
		return http.StatusOK, outcome{Queued: false, Duplicate: true}
	}
	id, err := s.store.Enqueue(ctx, e)
	if err != nil {
		return http.StatusServiceUnavailable, outcome{Error: "queue unavailable"}
	}
	return http.StatusAccepted, outcome{Queued: true, ID: id}
}

// entry normalizes and validates the payload into the stream contract.
func (s *Server) entry(userID string, n notification) (queue.Entry, error) {
	text := normalize.Text(n.Text)
	if text == "" {
		return queue.Entry{}, errors.New("text is required")
	}
	if len([]rune(text)) > s.cfg.MaxTextLength {
		return queue.Entry{}, fmt.Errorf("text is longer than %d characters", s.cfg.MaxTextLength)
	}
	now := s.Now().UTC()
	posted := now
	if raw := strings.TrimSpace(n.PostedAt); raw != "" {
		t, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return queue.Entry{}, errors.New("postedAt must be an RFC 3339 timestamp")
		}
		if t.After(now.Add(24 * time.Hour)) {
			return queue.Entry{}, errors.New("postedAt is in the future")
		}
		posted = t.UTC()
	}
	source := strings.ToUpper(strings.TrimSpace(n.Source))
	if source == "" {
		source = "NOTIFICATION"
	}
	if source != "NOTIFICATION" && source != "SMS" {
		return queue.Entry{}, errors.New("source must be NOTIFICATION or SMS")
	}
	return queue.Entry{
		UserID:      userID,
		Source:      source,
		PackageName: normalize.Clip(normalize.Text(n.PackageName), 200),
		AppName:     normalize.Clip(normalize.Text(n.AppName), 100),
		Title:       normalize.Clip(normalize.Text(n.Title), 200),
		Text:        text,
		PostedAt:    posted.Format(timeLayout),
		ReceivedAt:  now.Format(timeLayout),
		DeviceID:    normalize.Clip(normalize.Text(n.DeviceID), 100),
	}, nil
}

// dayOf is the Mexico City calendar day of an RFC 3339 instant.
func (s *Server) dayOf(rfc3339 string) string {
	t, err := time.Parse(time.RFC3339, rfc3339)
	if err != nil {
		t = s.Now()
	}
	return t.In(s.tz).Format("2006-01-02")
}

// ── Middleware ────────────────────────────────────────

type authedHandler func(w http.ResponseWriter, r *http.Request, userID string)

// authenticated requires the shared API key and a valid access token from the API.
func (s *Server) authenticated(next authedHandler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("x-api-key")), []byte(s.cfg.APIKey)) != 1 {
			fail(w, http.StatusUnauthorized, "Invalid or missing API key")
			return
		}
		header := r.Header.Get("Authorization")
		if !strings.HasPrefix(header, "Bearer ") {
			fail(w, http.StatusUnauthorized, "Missing bearer token")
			return
		}
		claims, err := auth.Verify(strings.TrimPrefix(header, "Bearer "), []byte(s.cfg.JWTSecret), s.Now())
		if err != nil {
			fail(w, http.StatusUnauthorized, "Invalid or expired token")
			return
		}
		// A capture token stays valid for a year; revocation lives in Redis.
		if claims.Scope == "capture" && claims.ID != "" {
			revoked, err := s.store.IsRevoked(r.Context(), claims.ID)
			if err != nil {
				fail(w, http.StatusServiceUnavailable, "Queue unavailable")
				return
			}
			if revoked {
				fail(w, http.StatusUnauthorized, "Capture token revoked")
				return
			}
		}
		next(w, r, claims.Subject)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// logged records method, path, status and latency. Never the body: it is
// the user's financial data.
func (s *Server) logged(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		s.log.Info("request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"ms", time.Since(started).Milliseconds(),
		)
	})
}

func (s *Server) recovered(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if p := recover(); p != nil {
				s.log.Error("panic", "err", fmt.Sprint(p), "path", r.URL.Path)
				fail(w, http.StatusInternalServerError, "Internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// ── Helpers ───────────────────────────────────────────

func decode(w http.ResponseWriter, r *http.Request, v any) error {
	if ct := r.Header.Get("Content-Type"); ct != "" && !strings.HasPrefix(ct, "application/json") {
		return errors.New("content-type must be application/json")
	}
	body := http.MaxBytesReader(w, r.Body, maxBody)
	defer body.Close()
	if err := json.NewDecoder(body).Decode(v); err != nil {
		return errors.New("invalid JSON body")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// fail mirrors the API's error envelope so the app handles both the same way.
func fail(w http.ResponseWriter, code int, message string) {
	writeJSON(w, code, map[string]any{"statusCode": code, "message": message})
}
