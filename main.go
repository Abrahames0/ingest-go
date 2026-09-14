// ingest-go receives the bank notifications the Centli app captures on
// the phone, normalizes them, drops obvious repeats and appends them to a
// Redis Stream. The NestJS API consumes that stream and turns each entry into
// a movement waiting for confirmation. The phone fires and forgets: even if
// the API is restarting or the model is slow, the notification is already
// safe in the queue.
//
//	ingest          run the service
//	ingest -check   exit 0 when the running service answers /healthz
//	                (the container HEALTHCHECK; distroless has no curl).
//	                200 and 503 both count: the process is serving, and a
//	                Redis outage must not get it restarted in a loop
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/xonix/centli-ingest/internal/config"
	"github.com/xonix/centli-ingest/internal/queue"
	"github.com/xonix/centli-ingest/internal/server"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	if err := config.LoadDotEnv(".env"); err != nil {
		logger.Error("reading .env", "err", err)
		os.Exit(1)
	}

	if len(os.Args) > 1 && (os.Args[1] == "-check" || os.Args[1] == "--check") {
		os.Exit(healthCheck())
	}

	cfg, err := config.Load()
	if err != nil {
		logger.Error("config", "err", err)
		os.Exit(1)
	}

	store, err := queue.NewRedis(cfg.RedisURL, cfg.StreamKey, cfg.StreamMaxLen)
	if err != nil {
		logger.Error("redis", "err", err)
		os.Exit(1)
	}
	defer store.Close()

	// Redis may still be starting; say so instead of waiting for the first 503.
	pingCtx, cancelPing := context.WithTimeout(context.Background(), 3*time.Second)
	if err := store.Ping(pingCtx); err != nil {
		logger.Warn("redis not reachable yet; /healthz will report degraded until it is", "err", err)
	}
	cancelPing()

	httpServer := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           server.New(cfg, store, logger).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      20 * time.Second, // a full batch may take a while against a slow Redis
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		logger.Info("ingest listening",
			"port", cfg.Port, "stream", cfg.StreamKey,
			"version", server.Version, "commit", server.Commit,
		)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server", "err", err)
			os.Exit(1)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
	logger.Info("ingest stopped")
}

// healthCheck probes the local service the way the container runtime does.
func healthCheck() int {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	client := &http.Client{Timeout: 3 * time.Second}
	res, err := client.Get("http://127.0.0.1:" + port + "/healthz")
	if err != nil {
		return 1
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusServiceUnavailable {
		return 1
	}
	return 0
}
