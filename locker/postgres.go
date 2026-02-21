package locker

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

type PostgresLocker struct {
	db              *sqlx.DB
	maxOpenConns    int
	maxIdleConns    int
	connMaxLifetime time.Duration
	connMaxIdleTime time.Duration
}

type postgresLock struct {
	tx *sqlx.Tx
}

type LockerOption func(*PostgresLocker)

func WithMaxOpenConns(n int) LockerOption {
	return func(l *PostgresLocker) {
		l.maxOpenConns = n
	}
}

func WithMaxIdleConns(n int) LockerOption {
	return func(l *PostgresLocker) {
		l.maxIdleConns = n
	}
}

func WithConnMaxLifetime(d time.Duration) LockerOption {
	return func(l *PostgresLocker) {
		l.connMaxLifetime = d
	}
}

func WithConnMaxIdleTime(d time.Duration) LockerOption {
	return func(l *PostgresLocker) {
		l.connMaxIdleTime = d
	}
}

func (l *postgresLock) ReleaseLock(ctx context.Context) error {
	return l.tx.Rollback()
}

func NewPostgresLocker(dsn string, opts ...LockerOption) (*PostgresLocker, error) {
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
	locker := &PostgresLocker{db: db}
	for _, opt := range opts {
		opt(locker)
	}
	// Apply connection pool settings
	if locker.maxOpenConns > 0 {
		db.SetMaxOpenConns(locker.maxOpenConns)
	}
	if locker.maxIdleConns > 0 {
		db.SetMaxIdleConns(locker.maxIdleConns)
	}
	if locker.connMaxLifetime > 0 {
		db.SetConnMaxLifetime(locker.connMaxLifetime)
	}
	if locker.connMaxIdleTime > 0 {
		db.SetConnMaxIdleTime(locker.connMaxIdleTime)
	}
	return locker, nil
}

func (p *PostgresLocker) AcquireLock(ctx context.Context, key string) (Lock, error) {
	keyHash := hashKey(key)
	tx, err := p.db.BeginTxx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}
	var lockAttempt bool
	if err := tx.GetContext(ctx, &lockAttempt, "SELECT pg_try_advisory_xact_lock($1)", keyHash); err != nil {
		_ = tx.Rollback()
		return nil, errors.Wrapf(err, "failed to acquire lock for key '%s'", key)
	}
	if !lockAttempt {
		_ = tx.Rollback()
		return nil, errors.Errorf("failed to acquire lock for key '%s'", key)
	}
	return &postgresLock{tx: tx}, nil
}

func hashKey(key string) int64 {
	hash := sha256.Sum256([]byte(key))            // Compute SHA-256 hash
	unsigned := binary.BigEndian.Uint64(hash[:8]) // Take the first 8 bytes
	return int64(unsigned)
}
