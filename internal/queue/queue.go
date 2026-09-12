// Package queue is the Redis side of the service: the rate limits, the early
// duplicate filter, the revoked-token set and the stream the API consumes.
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
	// Allow counts one hit on the bucket (a user or an IP within a minute,
	// the caller builds the name) and reports whether it is still within limit.
	Allow(ctx context.Context, bucket string, limit int) (bool, error)
	// FirstSeen marks key for ttl and reports whether it was new.
	FirstSeen(ctx context.Context, key string, ttl time.Duration) (bool, error)
	// Enqueue appends the entry to the stream and returns its id.
	Enqueue(ctx context.Context, e Entry) (string, error)
	// IsRevoked reports whether the API revoked the capture token with this id.
	IsRevoked(ctx context.Context, jti string) (bool, error)
	Close() error
}

// RevokedSet is the Redis set the API writes revoked capture token ids to.
const RevokedSet = "capture:revoked"

// Rate-limit buckets are per minute; two minutes of life covers clock skew
// between instances and lets the key disappear on its own.
const bucketTTLSeconds = 120

// allowScript increments the bucket and sets its expiry in the same call, so
// a hit can never leave a counter behind without a TTL.
var allowScript = redis.NewScript(`
local n = redis.call('INCR', KEYS[1])
if n == 1 then
  redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return n
`)

type Redis struct {
	rdb    *redis.Client
	stream string
	maxLen int64
}

// NewRedis parses the URL and builds the client; the first command opens the
// connection. Callers that want to know early should Ping.
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

func (r *Redis) Allow(ctx context.Context, bucket string, limit int) (bool, error) {
	n, err := allowScript.Run(ctx, r.rdb, []string{"ingest:rl:" + bucket}, bucketTTLSeconds).Int64()
	if err != nil {
		return false, err
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

func (r *Redis) IsRevoked(ctx context.Context, jti string) (bool, error) {
	return r.rdb.SIsMember(ctx, RevokedSet, jti).Result()
}

func (r *Redis) Close() error {
	return r.rdb.Close()
}
