package locker

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

type PostgresLocker struct {
	db *sqlx.DB
}

type postgresLock struct {
	tx *sqlx.Tx
}

func (l *postgresLock) ReleaseLock(ctx context.Context) error {
	return l.tx.Rollback()
}

func NewPostgresLocker(dsn string) (*PostgresLocker, error) {
	db, err := sqlx.Connect("postgres", dsn)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to connect to postgres")
	}
	if db == nil {
		return nil, errors.New("database connection cannot be nil")
	}
	if err := db.Ping(); err != nil {
		return nil, errors.Wrapf(err, "failed to ping database")
	}
	return &PostgresLocker{db: db}, nil
}

func (p *PostgresLocker) AcquireLock(ctx context.Context, key string) (Lock, error) {
	keyHash := hashKey(key)
	tx, err := p.db.BeginTxx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}
	var lockAttempt bool
	if err := tx.GetContext(ctx, &lockAttempt, "SELECT pg_try_advisory_xact_lock($1)", keyHash); err != nil {
		return nil, errors.Wrapf(err, "failed to acquire lock for key '%s'", key)
	}
	if !lockAttempt {
		return nil, errors.Errorf("failed to acquire lock for key '%s'", key)
	}
	return &postgresLock{tx: tx}, nil
}

func hashKey(key string) int64 {
	hash := sha256.Sum256([]byte(key))            // Compute SHA-256 hash
	unsigned := binary.BigEndian.Uint64(hash[:8]) // Take the first 8 bytes
	return int64(unsigned)
}
