package storage

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

type Migration struct {
	Version             int64
	Name, SQL, Checksum string
}

func migrations() ([]Migration, error) {
	entries, err := fs.Glob(migrationFiles, "migrations/*.sql")
	if err != nil {
		return nil, err
	}
	sort.Strings(entries)
	out := make([]Migration, 0, len(entries))
	for _, path := range entries {
		base := filepath.Base(path)
		var version int64
		if _, err := fmt.Sscanf(base, "%d_", &version); err != nil {
			return nil, fmt.Errorf("parse migration %q: %w", base, err)
		}
		b, err := migrationFiles.ReadFile(path)
		if err != nil {
			return nil, err
		}
		prefix := fmt.Sprintf("%03d_", version)
		out = append(out, Migration{version, strings.TrimSuffix(strings.TrimPrefix(base, prefix), ".sql"), string(b), checksum(b)})
	}
	return out, nil
}
func checksum(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

func EnsureMigrationTable(ctx context.Context, db *pgxpool.Pool) error {
	_, err := db.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version BIGINT PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL DEFAULT '', applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp())`)
	return err
}

// ApplyMigrations applies embedded migrations in order. The advisory lock is held
// in the same transaction as both the check and migration, preventing races.
func ApplyMigrations(ctx context.Context, db *pgxpool.Pool) error {
	if err := EnsureMigrationTable(ctx, db); err != nil {
		return fmt.Errorf("create migration table: %w", err)
	}
	ms, err := migrations()
	if err != nil {
		return err
	}
	for _, m := range ms {
		tx, err := db.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			return fmt.Errorf("begin migration %d: %w", m.Version, err)
		}
		if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('watchtower schema migrations'))`); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("lock migration %d: %w", m.Version, err)
		}
		var recorded string
		err = tx.QueryRow(ctx, `SELECT COALESCE(checksum,'') FROM schema_migrations WHERE version=$1`, m.Version).Scan(&recorded)
		if err == pgx.ErrNoRows {
			err = nil
			recorded = ""
		}
		if err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("check migration %d: %w", m.Version, err)
		}
		if recorded != "" {
			if recorded != m.Checksum {
				_ = tx.Rollback(ctx)
				return fmt.Errorf("migration %d checksum mismatch: database=%s file=%s", m.Version, recorded, m.Checksum)
			}
			if err = tx.Commit(ctx); err != nil {
				return fmt.Errorf("commit migration check %d: %w", m.Version, err)
			}
			continue
		}
		// An empty checksum denotes a legacy row; do not silently rerun its SQL.
		var exists bool
		err = tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version=$1)`, m.Version).Scan(&exists)
		if err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("check migration %d: %w", m.Version, err)
		}
		if exists {
			if err = tx.Commit(ctx); err != nil {
				return err
			}
			continue
		}
		if _, err = tx.Exec(ctx, m.SQL); err == nil {
			_, err = tx.Exec(ctx, `INSERT INTO schema_migrations(version,name,checksum) VALUES($1,$2,$3)`, m.Version, m.Name, m.Checksum)
		}
		if err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply migration %d (%s): %w", m.Version, m.Name, err)
		}
		if err = tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit migration %d: %w", m.Version, err)
		}
	}
	return nil
}

// Reset is intentionally explicit and destructive, for local review and tests.
func Reset(ctx context.Context, db *pgxpool.Pool) error {
	if _, err := db.Exec(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public`); err != nil {
		return fmt.Errorf("reset database: %w", err)
	}
	return ApplyMigrations(ctx, db)
}
