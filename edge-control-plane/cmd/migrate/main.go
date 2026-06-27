package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/config"
	"github.com/edgeclouderz/edge-cloud/edge-control-plane/internal/repository"
	"github.com/rubenv/sql-migrate"
)

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

	migrations := &migrate.FileMigrationSource{
		Dir: "migrations",
	}

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
