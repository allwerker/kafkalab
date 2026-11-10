// Package testkit provides test infrastructure: a real PostgreSQL per test, an in-process
// Kafka cluster (kfake) and a fake schema registry.
package testkit

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// PGImage is the PostgreSQL image used for tests (same major version as the cluster).
const PGImage = "postgres:18-alpine"

const (
	pgContainer = "kafkalab-test-pg"
	pgPort      = "55432"
)

var (
	pgOnce     sync.Once
	pgAdminDSN string
	pgErr      error
)

// PostgresDSN returns the DSN of a fresh, empty database that is dropped when the test ends.
// It uses LAB_TEST_PG_DSN (an admin DSN) when set, otherwise a reusable Docker container
// named kafkalab-test-pg on 127.0.0.1:55432 that is started on first use.
func PostgresDSN(t testing.TB) string {
	t.Helper()
	pgOnce.Do(func() { pgAdminDSN, pgErr = adminDSN() })
	if pgErr != nil {
		t.Fatalf("postgres for tests: %v", pgErr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, pgAdminDSN)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	defer admin.Close(ctx)
	name := "t_" + randomHex(8)
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if c, err := pgx.Connect(ctx, pgAdminDSN); err == nil {
			_, _ = c.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
			_ = c.Close(ctx)
		}
	})
	return replaceDatabase(pgAdminDSN, name)
}

func adminDSN() (string, error) {
	if dsn := os.Getenv("LAB_TEST_PG_DSN"); dsn != "" {
		return dsn, nil
	}
	dsn := "postgres://postgres:test@127.0.0.1:" + pgPort + "/postgres?sslmode=disable"
	if err := ping(dsn, time.Second); err == nil {
		return dsn, nil
	}
	if out, err := exec.Command("docker", "inspect", "-f", "{{.State.Running}}", pgContainer).Output(); err == nil {
		if strings.TrimSpace(string(out)) != "true" {
			_ = exec.Command("docker", "start", pgContainer).Run()
		}
	} else {
		cmd := exec.Command("docker", "run", "-d", "--name", pgContainer,
			"-e", "POSTGRES_PASSWORD=test", "-p", "127.0.0.1:"+pgPort+":5432",
			"--tmpfs", "/var/lib/postgresql", PGImage,
			"-c", "fsync=off", "-c", "synchronous_commit=off", "-c", "full_page_writes=off", "-c", "max_connections=300")
		if out, err := cmd.CombinedOutput(); err != nil {
			// Test packages run in parallel processes: another one may have started the shared
			// container a moment earlier ("name already in use"). Then just wait for it below.
			if !strings.Contains(string(out), "already in use") {
				return "", fmt.Errorf("docker run %s: %v: %s", PGImage, err, out)
			}
		}
	}
	if err := ping(dsn, 60*time.Second); err != nil {
		return "", fmt.Errorf("postgres test container not ready: %w", err)
	}
	return dsn, nil
}

func ping(dsn string, within time.Duration) error {
	deadline := time.Now().Add(within)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		c, err := pgx.Connect(ctx, dsn)
		if err == nil {
			err = c.Ping(ctx)
			_ = c.Close(ctx)
		}
		cancel()
		if err == nil || time.Now().After(deadline) {
			return err
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func replaceDatabase(dsn, db string) string {
	i := strings.LastIndex(dsn, "/")
	j := strings.Index(dsn[i:], "?")
	if j < 0 {
		return dsn[:i+1] + db
	}
	return dsn[:i+1] + db + dsn[i+j:]
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
