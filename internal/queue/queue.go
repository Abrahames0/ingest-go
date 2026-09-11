// Package queue is the Redis side of the service: the per-user rate limit,
// the early duplicate filter and the stream the API consumes.
package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Entry is the contract shared with the API's CaptureStreamConsumer
// (api-fin/src/modules/capture/capture-stream.consumer.ts). Every value is a
// string; optional ones may be empty.
type Entry struct {
	UserID      string
	Source      string // NOTIFICATION | SMS
	PackageName string
	AppName     string
	Title       string
	Text        string
	PostedAt    string // RFC 3339, when the phone showed it
	ReceivedAt  string // RFC 3339, when this service got it
	DeviceID    string
}

func (e Entry) values() map[string]any {
	return map[string]any{
		"userId":      e.UserID,
		"source":      e.Source,
		"packageName": e.PackageName,
		"appName":     e.AppName,
		"title":       e.Title,
		"text":        e.Text,
		"postedAt":    e.PostedAt,
		"receivedAt":  e.ReceivedAt,
		"deviceId":    e.DeviceID,
	}
}

// Store is what the HTTP layer needs. Redis implements it; tests fake it.
type Store interface {
	Ping(ctx context.Context) error
	// Allow counts one request for the user in the current minute and reports
	// whether it is still within limit.
	Allow(ctx context.Context, userID string, limit int) (bool, error)
	// FirstSeen marks key for ttl and reports whether it was new.
	FirstSeen(ctx context.Context, key string, ttl time.Duration) (bool, error)
	// Enqueue appends the entry to the stream and returns its id.
	Enqueue(ctx context.Context, e Entry) (string, error)
	// IsRevoked reports whether the API revoked the capture token with this id.
	IsRevoked(ctx context.Context, jti string) (bool, error)
	Close() error
}

type Redis struct {
	rdb    *redis.Client
	stream string
	maxLen int64
}

// NewRedis connects lazily; the first command opens the connection.
func NewRedis(url, stream string, maxLen int64) (*Redis, error) {
	opt, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("REDIS_URL: %w", err)
	}
	return &Redis{rdb: redis.NewClient(opt), stream: stream, maxLen: maxLen}, nil
}

func (r *Redis) Ping(ctx context.Context) error {
	return r.rdb.Ping(ctx).Err()
}

func (r *Redis) Allow(ctx context.Context, userID string, limit int) (bool, error) {
	key := "ingest:rl:" + userID + ":" + time.Now().UTC().Format("200601021504")
	n, err := r.rdb.Incr(ctx, key).Result()
	if err != nil {
		return false, err
	}
	if n == 1 {
		r.rdb.Expire(ctx, key, 2*time.Minute)
	}
	return n <= int64(limit), nil
}

func (r *Redis) FirstSeen(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return r.rdb.SetNX(ctx, "ingest:seen:"+key, "1", ttl).Result()
}

func (r *Redis) Enqueue(ctx context.Context, e Entry) (string, error) {
	return r.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: r.stream,
		MaxLen: r.maxLen,
		Approx: true,
		Values: e.values(),
	}).Result()
}

// RevokedSet is the Redis set the API writes revoked capture token ids to.
const RevokedSet = "capture:revoked"

func (r *Redis) IsRevoked(ctx context.Context, jti string) (bool, error) {
	return r.rdb.SIsMember(ctx, RevokedSet, jti).Result()
}

func (r *Redis) Close() error {
	return r.rdb.Close()
}
