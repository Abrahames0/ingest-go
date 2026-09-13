// token prints a capture token for manual tests of the ingest service:
//
//	go run ./cmd/token -sub <userId> [-jti <id>] [-days 365] [-activate]
//
// It signs with CAPTURE_TOKEN_SECRET (environment or ./.env) and the same
// claims as the tokens of POST /api/v1/capture/tokens. The service only takes
// a token the API lists as active, so -activate also writes that entry
// (capture:active:{jti} = userId, expiring with the token) to REDIS_URL, to
// try the service without the API. Development only: the API checks that the
// user exists and this tool does not.
package main

import (
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xonix/centli-ingest/internal/auth"
	"github.com/xonix/centli-ingest/internal/config"
	"github.com/xonix/centli-ingest/internal/queue"
)

func main() {
	sub := flag.String("sub", "", "user id the token belongs to (required)")
	jti := flag.String("jti", "", "capture token id (default: a random UUID)")
	days := flag.Int("days", 365, "days until the token expires")
	activate := flag.Bool("activate", false, "also list the token as active in REDIS_URL")
	flag.Parse()
	if *sub == "" || *days < 1 {
		flag.Usage()
		os.Exit(2)
	}
	_ = config.LoadDotEnv(".env")
	secret := os.Getenv("CAPTURE_TOKEN_SECRET")
	if len(secret) < 32 {
		fmt.Fprintln(os.Stderr, "CAPTURE_TOKEN_SECRET missing (32+ characters)")
		os.Exit(1)
	}
	id := *jti
	if id == "" {
		id = newUUID()
	}
	ttl := time.Duration(*days) * 24 * time.Hour
	token := auth.Sign(auth.CaptureClaims(*sub, id, time.Now(), ttl), []byte(secret))
	if *activate {
		if err := activateToken(id, *sub, ttl); err != nil {
			fmt.Fprintln(os.Stderr, "activate:", err)
			os.Exit(1)
		}
	}
	fmt.Println(token)
}

// activateToken writes the allow-list entry the API writes when it issues a token.
func activateToken(jti, sub string, ttl time.Duration) error {
	url := os.Getenv("REDIS_URL")
	if url == "" {
		return errors.New("REDIS_URL is required with -activate")
	}
	opt, err := redis.ParseURL(url)
	if err != nil {
		return errors.New("REDIS_URL is not a valid redis:// URL")
	}
	rdb := redis.NewClient(opt)
	defer rdb.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return rdb.Set(ctx, queue.ActiveKeyPrefix+jti, sub, ttl).Err()
}

// newUUID returns a random version 4 UUID, like the ids the API gives capture tokens.
func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
