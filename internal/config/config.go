// Package config reads the service settings from the environment (and an
// optional .env file for local runs). The secrets are the same ones the
// NestJS API uses, so a token issued by the API is valid here.
package config

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Port          string
	RedisURL      string
	APIKey        string // same API_KEY as api-fin (x-api-key header)
	JWTSecret     string // same JWT_ACCESS_SECRET as api-fin (HS256)
	StreamKey     string // Redis Stream the API consumes
	StreamMaxLen  int64  // approximate cap on the stream length
	MaxTextLength int    // characters accepted per notification
	RatePerMinute int    // per user
	DedupeTTL     time.Duration
}

func Load() (Config, error) {
	cfg := Config{
		Port:          env("PORT", "8080"),
		RedisURL:      env("REDIS_URL", "redis://localhost:6379"),
		APIKey:        os.Getenv("API_KEY"),
		JWTSecret:     os.Getenv("JWT_ACCESS_SECRET"),
		StreamKey:     env("CAPTURE_STREAM_KEY", "capture:notifications"),
		StreamMaxLen:  envInt64("CAPTURE_STREAM_MAXLEN", 50_000),
		MaxTextLength: envInt("MAX_TEXT_LENGTH", 2000),
		RatePerMinute: envInt("RATE_PER_MINUTE", 120),
		DedupeTTL:     24 * time.Hour,
	}
	if cfg.APIKey == "" {
		return cfg, errors.New("API_KEY is required (same value as api-fin)")
	}
	if len(cfg.JWTSecret) < 32 {
		return cfg, errors.New("JWT_ACCESS_SECRET is required (same value as api-fin, 32+ characters)")
	}
	return cfg, nil
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
	for _, line := range strings.Split(string(data), "\n") {
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
