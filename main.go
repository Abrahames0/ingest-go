// ingest-go receives the bank notifications the Centli app captures on
// the phone, normalizes them, drops obvious repeats and appends them to a
// Redis Stream. The NestJS API consumes that stream and turns each entry into
// a movement waiting for confirmation. The phone fires and forgets: even if
// the API is restarting or the model is slow, the notification is already
// safe in the queue.
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

	httpServer := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           server.New(cfg, store, logger).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		logger.Info("ingest listening", "port", cfg.Port, "stream", cfg.StreamKey)
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
