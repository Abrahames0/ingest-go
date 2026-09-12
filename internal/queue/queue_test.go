package queue

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// These run against a real Redis when REDIS_URL is set (CI starts one);
// otherwise they are skipped so `go test ./...` stays offline-friendly.
func testRedis(t *testing.T) (*Redis, context.Context) {
	t.Helper()
	url := os.Getenv("REDIS_URL")
	if url == "" {
		t.Skip("REDIS_URL not set")
	}
	r, err := NewRedis(url, "capture:test:"+t.Name(), 100)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(func() {
		r.rdb.Del(ctx, r.stream)
		cancel()
		r.Close()
	})
	if err := r.Ping(ctx); err != nil {
		t.Skipf("redis at REDIS_URL not reachable: %v", err)
	}
	return r, ctx
}

func TestAllowIsAtomicAndExpires(t *testing.T) {
	r, ctx := testRedis(t)
	bucket := "test:" + t.Name() + ":" + time.Now().Format("150405.000")
	for i := 1; i <= 3; i++ {
		ok, err := r.Allow(ctx, bucket, 3)
		if err != nil || !ok {
			t.Fatalf("hit %d: ok=%v err=%v", i, ok, err)
		}
	}
	if ok, _ := r.Allow(ctx, bucket, 3); ok {
		t.Fatal("fourth hit must be over the limit")
	}
	ttl, err := r.rdb.TTL(ctx, "ingest:rl:"+bucket).Result()
	if err != nil || ttl <= 0 || ttl > bucketTTLSeconds*time.Second {
		t.Fatalf("bucket must carry a TTL: %v %v", ttl, err)
	}
	r.rdb.Del(ctx, "ingest:rl:"+bucket)
}

func TestFirstSeenAndEnqueue(t *testing.T) {
	r, ctx := testRedis(t)
	key := "test:" + t.Name() + ":" + time.Now().Format("150405.000")
	fresh, err := r.FirstSeen(ctx, key, time.Minute)
	if err != nil || !fresh {
		t.Fatalf("first: fresh=%v err=%v", fresh, err)
	}
	if fresh, _ := r.FirstSeen(ctx, key, time.Minute); fresh {
		t.Fatal("second sighting must not be fresh")
	}
	r.rdb.Del(ctx, "ingest:seen:"+key)

	id, err := r.Enqueue(ctx, Entry{UserID: "u1", Source: "NOTIFICATION", Text: "Compra por $1"})
	if err != nil || id == "" {
		t.Fatalf("enqueue: id=%q err=%v", id, err)
	}
	msgs, err := r.rdb.XRange(ctx, r.stream, id, id).Result()
	if err != nil || len(msgs) != 1 || msgs[0].Values["userId"] != "u1" || msgs[0].Values["text"] != "Compra por $1" {
		t.Fatalf("stream entry: %+v err=%v", msgs, err)
	}
}

func TestIsRevoked(t *testing.T) {
	r, ctx := testRedis(t)
	jti := "test-" + t.Name() + "-" + time.Now().Format("150405.000")
	if revoked, err := r.IsRevoked(ctx, jti); err != nil || revoked {
		t.Fatalf("unknown id: revoked=%v err=%v", revoked, err)
	}
	r.rdb.SAdd(ctx, RevokedSet, jti)
	t.Cleanup(func() { r.rdb.SRem(ctx, RevokedSet, jti) })
	if revoked, _ := r.IsRevoked(ctx, jti); !revoked {
		t.Fatal("id in the set must count as revoked")
	}
}

var _ = redis.Nil // keep the import used if the tests above change
