// Package config reads the service settings from the environment (and an
// optional .env file for local runs). API_KEY and CAPTURE_TOKEN_SECRET are the
// same values the NestJS API uses, so a capture token issued by the API is
// valid here.
package config

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"time"
)

// maxProxyHops bounds TRUST_PROXY_HOPS the same way the API does.
const maxProxyHops = 5

type Config struct {
	Port               string
	RedisURL           string
	APIKey             string // same API_KEY as api-fin (x-api-key header)
	CaptureTokenSecret string // same CAPTURE_TOKEN_SECRET as api-fin: signs capture tokens only (HS256)
	TrustProxyHops     int    // proxies in front of the service that append to X-Forwarded-For
	StreamKey          string // Redis Stream the API consumes
	StreamMaxLen       int64  // approximate cap on the stream length
	MaxTextLength      int    // characters accepted per notification
	RatePerMinute      int    // per user, after authentication
	IPRatePerMinute    int    // per client IP, before authentication (brute-force brake)
	DedupeTTL          time.Duration
}

func Load() (Config, error) {
	cfg := Config{
		Port:               env("PORT", "8080"),
		RedisURL:           env("REDIS_URL", "redis://localhost:6379"),
		APIKey:             os.Getenv("API_KEY"),
		CaptureTokenSecret: os.Getenv("CAPTURE_TOKEN_SECRET"),
		StreamKey:          env("CAPTURE_STREAM_KEY", "capture:notifications"),
		StreamMaxLen:       envInt64("CAPTURE_STREAM_MAXLEN", 50_000),
		MaxTextLength:      envInt("MAX_TEXT_LENGTH", 2000),
		RatePerMinute:      envInt("RATE_PER_MINUTE", 120),
		IPRatePerMinute:    envInt("IP_RATE_PER_MINUTE", 600),
		DedupeTTL:          24 * time.Hour,
	}
	if cfg.APIKey == "" {
		return cfg, errors.New("API_KEY is required (same value as api-fin)")
	}
	if len(cfg.CaptureTokenSecret) < 32 {
		return cfg, errors.New("CAPTURE_TOKEN_SECRET is required (same value as api-fin, 32+ characters)")
	}
	if cfg.CaptureTokenSecret == cfg.APIKey {
		return cfg, errors.New("CAPTURE_TOKEN_SECRET must be different from API_KEY")
	}
	hops, err := proxyHops(os.Getenv("TRUST_PROXY_HOPS"))
	if err != nil {
		return cfg, err
	}
	cfg.TrustProxyHops = hops
	return cfg, nil
}

// proxyHops parses TRUST_PROXY_HOPS. Unset means no proxy: the socket address
// is the client. A wrong value is an error rather than a silent default,
// because it decides which address the brute-force brake counts.
func proxyHops(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 || n > maxProxyHops {
		return 0, errors.New("TRUST_PROXY_HOPS must be a whole number from 0 to 5")
	}
	return n, nil
}

// LoadDotEnv sets the variables of a .env file that are not already set in
// the environment. A missing file is not an error.
func LoadDotEnv(path string) error {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for line := range strings.SplitSeq(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.Trim(strings.TrimSpace(value), `"'`)
		if _, exists := os.LookupEnv(key); !exists {
			_ = os.Setenv(key, value)
		}
	}
	return nil
}

func env(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v, err := strconv.Atoi(strings.TrimSpace(os.Getenv(key))); err == nil && v > 0 {
		return v
	}
	return fallback
}

func envInt64(key string, fallback int64) int64 {
	if v, err := strconv.ParseInt(strings.TrimSpace(os.Getenv(key)), 10, 64); err == nil && v > 0 {
		return v
	}
	return fallback
}
