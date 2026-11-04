package locker

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
)

const (
	defaultLeaseTTL = 30 * time.Second
	maxLockKeyLen   = 256
)

var holderInstance atomic.Uint64

type PostgresLocker struct {
	db                *sqlx.DB
	holder            string
	leaseTTL          time.Duration
	heartbeatInterval time.Duration
	maxOpenConns      int
	maxIdleConns      int
	connMaxLifetime   time.Duration
	connMaxIdleTime   time.Duration
}

type postgresLock struct {
	db                *sqlx.DB
	key               string
	holder            string
	token             int64
	leaseSec          float64
	heartbeatInterval time.Duration
	stopHeartbeat     context.CancelFunc
	heartbeatDone     sync.WaitGroup
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

func WithLeaseTTL(d time.Duration) LockerOption {
	return func(l *PostgresLocker) {
		l.leaseTTL = d
	}
}

func WithHeartbeatInterval(d time.Duration) LockerOption {
	return func(l *PostgresLocker) {
		l.heartbeatInterval = d
	}
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
	hostname, err := os.Hostname()
	if err != nil {
		return nil, errors.Wrap(err, "failed to resolve hostname for lock holder id")
	}
	locker.holder = fmt.Sprintf("%s:%d:%d", hostname, os.Getpid(), holderInstance.Add(1))
	if locker.leaseTTL <= 0 {
		locker.leaseTTL = defaultLeaseTTL
	}
	if locker.heartbeatInterval <= 0 {
		locker.heartbeatInterval = max(locker.leaseTTL/3, time.Second)
	}
	if locker.heartbeatInterval >= locker.leaseTTL {
		return nil, errors.New("heartbeat interval must be less than lease TTL")
	}
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
	if len(key) == 0 || len(key) > maxLockKeyLen {
		return nil, errors.Errorf("invalid lock key length for key '%s'", key)
	}
	leaseSec := p.leaseTTL.Seconds()
	var token int64
	q := `
	INSERT INTO lock_leases (key, holder, token, expires_at)
	VALUES ($1, $2, 1, now() + ($3 * interval '1 second'))
	ON CONFLICT (key) DO UPDATE
	SET holder = EXCLUDED.holder,
		expires_at = EXCLUDED.expires_at,
		token = lock_leases.token + 1
	WHERE lock_leases.expires_at <= now()
	RETURNING token;
	`
	err := p.db.QueryRowContext(ctx, q, key, p.holder, leaseSec).Scan(&token)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errors.Errorf("failed to acquire lock for key '%s'", key)
	}
	if err != nil {
		return nil, errors.Wrapf(err, "failed to acquire lock for key '%s'", key)
	}
	hbCtx, cancel := context.WithCancel(context.Background())
	lock := &postgresLock{
		db:                p.db,
		key:               key,
		holder:            p.holder,
		token:             token,
		leaseSec:          leaseSec,
		heartbeatInterval: p.heartbeatInterval,
		stopHeartbeat:     cancel,
	}
	lock.heartbeatDone.Add(1)
	go lock.runHeartbeat(hbCtx)
	return lock, nil
}

func (l *postgresLock) ReleaseLock(ctx context.Context) error {
	if l.stopHeartbeat != nil {
		l.stopHeartbeat()
	}
	l.heartbeatDone.Wait()
	q := `DELETE FROM lock_leases WHERE key = $1 AND holder = $2 AND token = $3;`
	res, err := l.db.ExecContext(ctx, q, l.key, l.holder, l.token)
	if err != nil {
		return errors.Wrapf(err, "failed to release lock for key '%s'", l.key)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return errors.Wrapf(err, "failed to release lock for key '%s'", l.key)
	}
	if n == 0 {
		return errors.Errorf("failed to release lock for key '%s'", l.key)
	}
	return nil
}

func (l *postgresLock) runHeartbeat(ctx context.Context) {
	defer l.heartbeatDone.Done()
	ticker := time.NewTicker(l.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			q := `
			UPDATE lock_leases
			SET expires_at = now() + ($4 * interval '1 second')
			WHERE key = $1 AND holder = $2 AND token = $3;
			`
			res, err := l.db.ExecContext(ctx, q, l.key, l.holder, l.token, l.leaseSec)
			if err != nil {
				return
			}
			n, err := res.RowsAffected()
			if err != nil || n == 0 {
				return
			}
		}
	}
}
