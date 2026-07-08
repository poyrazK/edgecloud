package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/config"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/rubenv/sql-migrate"
)

// migrationsDir resolves the migrations directory via runtime.Caller(0)
// so that the result is independent of the binary's CWD. Mirrors
// the pattern in migrations/roundtrip_test.go.
//
// Resolves to <repo>/edge-control-plane/migrations regardless of the
// binary's CWD.
func migrationsDir() (string, error) {
	_, here, _, ok := runtime.Caller(0)
	if !ok {
		return "", errors.New("runtime.Caller(0) failed")
	}
	// main.go lives at .../edge-control-plane/cmd/migrate/main.go.
	// The migrations directory is at .../edge-control-plane/migrations,
	// i.e. two levels up from main.go's directory.
	dir := filepath.Join(filepath.Dir(here), "..", "..", "migrations")
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", fmt.Errorf("absolute path for %s: %w", dir, err)
	}
	return abs, nil
}

func main() {
	up := flag.Bool("up", false, "Run all pending migrations")
	down := flag.Bool("down", false, "Rollback the last migration")
	flag.Parse()

	if !*up && !*down {
		fmt.Println("Usage: migrate -up | -down")
		flag.Usage()
		os.Exit(1)
	}

	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	db, err := repository.NewDB(cfg.Database.DSN())
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("Error closing database: %v", err)
		}
	}()

	src, err := migrationsDir()
	if err != nil {
		log.Fatalf("Failed to locate migrations directory: %v", err)
	}
	migrations := &migrate.FileMigrationSource{Dir: src}

	if *up {
		n, err := migrate.Exec(db.DB, "postgres", migrations, migrate.Up)
		if err != nil {
			log.Fatalf("Migration failed: %v", err)
		}
		fmt.Printf("Applied %d migration(s)\n", n)
	}

	if *down {
		n, err := migrate.Exec(db.DB, "postgres", migrations, migrate.Down)
		if err != nil {
			log.Fatalf("Migration rollback failed: %v", err)
		}
		fmt.Printf("Rolled back %d migration(s)\n", n)
	}
}
