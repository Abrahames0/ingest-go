package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var captureSecret = strings.Repeat("c", 32)

func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{"PORT", "REDIS_URL", "API_KEY", "CAPTURE_TOKEN_SECRET", "JWT_ACCESS_SECRET", "TRUST_PROXY_HOPS", "CAPTURE_STREAM_KEY", "CAPTURE_STREAM_MAXLEN", "MAX_TEXT_LENGTH", "RATE_PER_MINUTE", "IP_RATE_PER_MINUTE"} {
		t.Setenv(k, "")
		os.Unsetenv(k)
	}
}

func TestLoadRequiresSecrets(t *testing.T) {
	clearEnv(t)
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "API_KEY") {
		t.Fatalf("want API_KEY error, got %v", err)
	}
	t.Setenv("API_KEY", "k")
	// The access-token secret is not enough: capture tokens have their own.
	t.Setenv("JWT_ACCESS_SECRET", strings.Repeat("s", 32))
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "CAPTURE_TOKEN_SECRET") {
		t.Fatalf("want CAPTURE_TOKEN_SECRET error, got %v", err)
	}
	t.Setenv("CAPTURE_TOKEN_SECRET", "short")
	if _, err := Load(); err == nil {
		t.Fatal("a short secret must be rejected")
	}
	t.Setenv("API_KEY", captureSecret)
	t.Setenv("CAPTURE_TOKEN_SECRET", captureSecret)
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "different") {
		t.Fatalf("the secret must differ from the API key, got %v", err)
	}
}

func TestLoadDefaultsAndOverrides(t *testing.T) {
	clearEnv(t)
	t.Setenv("API_KEY", "k")
	t.Setenv("CAPTURE_TOKEN_SECRET", captureSecret)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Port != "8080" || cfg.TrustProxyHops != 0 || cfg.RatePerMinute != 120 || cfg.IPRatePerMinute != 600 || cfg.StreamMaxLen != 50_000 || cfg.MaxTextLength != 2000 {
		t.Fatalf("defaults = %+v", cfg)
	}
	t.Setenv("PORT", " 9090 ")
	t.Setenv("TRUST_PROXY_HOPS", " 2 ")
	t.Setenv("RATE_PER_MINUTE", "5")
	t.Setenv("IP_RATE_PER_MINUTE", "-1") // invalid: keeps the default
	t.Setenv("CAPTURE_STREAM_MAXLEN", "abc")
	cfg, err = Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Port != "9090" || cfg.TrustProxyHops != 2 || cfg.RatePerMinute != 5 || cfg.IPRatePerMinute != 600 || cfg.StreamMaxLen != 50_000 {
		t.Fatalf("overrides = %+v", cfg)
	}
}

func TestLoadRejectsBadProxyHops(t *testing.T) {
	clearEnv(t)
	t.Setenv("API_KEY", "k")
	t.Setenv("CAPTURE_TOKEN_SECRET", captureSecret)
	for _, bad := range []string{"-1", "6", "one", "1.5"} {
		t.Setenv("TRUST_PROXY_HOPS", bad)
		if _, err := Load(); err == nil || !strings.Contains(err.Error(), "TRUST_PROXY_HOPS") {
			t.Errorf("TRUST_PROXY_HOPS=%q: want an error, got %v", bad, err)
		}
	}
}

func TestLoadDotEnv(t *testing.T) {
	clearEnv(t)
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")
	content := "# comment\nAPI_KEY=\"from-file\"\nRATE_PER_MINUTE = 7\nJUNK LINE\n\nPORT='1234'\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PORT", "5555") // the environment wins over the file
	if err := LoadDotEnv(path); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("API_KEY") != "from-file" || os.Getenv("RATE_PER_MINUTE") != "7" || os.Getenv("PORT") != "5555" {
		t.Fatalf("env after .env: API_KEY=%q RATE=%q PORT=%q", os.Getenv("API_KEY"), os.Getenv("RATE_PER_MINUTE"), os.Getenv("PORT"))
	}
	if err := LoadDotEnv(filepath.Join(dir, "missing.env")); err != nil {
		t.Fatalf("a missing file is not an error: %v", err)
	}
}
