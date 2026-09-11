// token prints an access token for manual tests of the ingest service:
//
//	go run ./cmd/token <userId> [ttl]
//
// It signs with JWT_ACCESS_SECRET (environment or ./.env), exactly like the
// API does. The API also checks that the user exists; this service does not.
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/xonix/centli-ingest/internal/auth"
	"github.com/xonix/centli-ingest/internal/config"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: token <userId> [ttl, e.g. 1h]")
		os.Exit(2)
	}
	_ = config.LoadDotEnv(".env")
	secret := os.Getenv("JWT_ACCESS_SECRET")
	if len(secret) < 32 {
		fmt.Fprintln(os.Stderr, "JWT_ACCESS_SECRET missing (32+ characters)")
		os.Exit(1)
	}
	ttl := time.Hour
	if len(os.Args) > 2 {
		d, err := time.ParseDuration(os.Args[2])
		if err != nil {
			fmt.Fprintln(os.Stderr, "bad ttl:", err)
			os.Exit(2)
		}
		ttl = d
	}
	now := time.Now()
	fmt.Println(auth.Sign(auth.Claims{
		Subject:   os.Args[1],
		IssuedAt:  now.Unix(),
		ExpiresAt: now.Add(ttl).Unix(),
	}, []byte(secret)))
}
