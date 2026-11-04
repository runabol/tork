package locker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testPostgresDSN = "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"

func TestNewPostgresLocker(t *testing.T) {
	locker, err := NewPostgresLocker(testPostgresDSN)
	assert.NoError(t, err, "locker initialization should succeed")
	assert.NotNil(t, locker, "locker should not be nil")
}

func TestPostgresLocker_AcquireLock(t *testing.T) {
	locker, err := NewPostgresLocker(testPostgresDSN)
	assert.NoError(t, err, "locker initialization should succeed")
	assert.NotNil(t, locker, "locker should not be nil")

	ctx := context.Background()
	key := "test_key"

	lock, err := locker.AcquireLock(ctx, key)
	assert.NoError(t, err, "lock acquisition should succeed")
	assert.NotNil(t, lock)

	lock2, err := locker.AcquireLock(ctx, key)
	assert.Error(t, err, "lock acquisition should not succeed")
	assert.Nil(t, lock2)

	assert.NoError(t, lock.ReleaseLock(ctx))

	lock3, err := locker.AcquireLock(ctx, key)
	assert.NoError(t, err, "lock acquisition should succeed")
	assert.NoError(t, lock3.ReleaseLock(ctx))
}

func TestPostgresLocker_AcquireLock_DifferentHolders(t *testing.T) {
	locker1, err := NewPostgresLocker(testPostgresDSN, WithLeaseTTL(30*time.Second), WithHeartbeatInterval(5*time.Second))
	assert.NoError(t, err)

	locker2, err := NewPostgresLocker(testPostgresDSN, WithLeaseTTL(30*time.Second), WithHeartbeatInterval(5*time.Second))
	assert.NoError(t, err)

	ctx := context.Background()
	key := "test_key_holders"

	lock, err := locker1.AcquireLock(ctx, key)
	assert.NoError(t, err)

	lock2, err := locker2.AcquireLock(ctx, key)
	assert.Error(t, err)
	assert.Nil(t, lock2)

	assert.NoError(t, lock.ReleaseLock(ctx))
}

// concurrentAcquireSameKey has two locker instances (two coordinators) race to
// acquire the same key at the same time. Returns how many acquired successfully.
func concurrentAcquireSameKey(t *testing.T, maxOpenConns int, hold time.Duration) int32 {
	t.Helper()

	locker1, err := NewPostgresLocker(
		testPostgresDSN,
		WithMaxOpenConns(maxOpenConns),
		WithMaxIdleConns(maxOpenConns),
		WithLeaseTTL(30*time.Second),
		WithHeartbeatInterval(5*time.Second),
	)
	require.NoError(t, err)

	locker2, err := NewPostgresLocker(
		testPostgresDSN,
		WithMaxOpenConns(maxOpenConns),
		WithMaxIdleConns(maxOpenConns),
		WithLeaseTTL(30*time.Second),
		WithHeartbeatInterval(5*time.Second),
	)
	require.NoError(t, err)

	ctx := context.Background()
	key := fmt.Sprintf("concurrent_%s", t.Name())

	var successes atomic.Int32
	start := make(chan struct{})
	var wg sync.WaitGroup

	tryAcquire := func(l Locker) {
		defer wg.Done()
		<-start
		lock, err := l.AcquireLock(ctx, key)
		if err != nil {
			return
		}
		successes.Add(1)
		time.Sleep(hold)
		assert.NoError(t, lock.ReleaseLock(ctx))
	}

	wg.Add(2)
	go tryAcquire(locker1)
	go tryAcquire(locker2)
	close(start)
	wg.Wait()

	return successes.Load()
}

func TestPostgresLocker_ConcurrentAcquireSingleWinnerWithoutPoolContention(t *testing.T) {
	n := concurrentAcquireSameKey(t, 25, 100*time.Millisecond)
	assert.Equal(t, int32(1), n, "only one holder should acquire the lease")
}

// Regression: lease acquire must not succeed twice for the same key when the pool
// is limited and the second attempt is delayed behind the first holder.
func TestPostgresLocker_ConcurrentAcquireSingleWinnerWhenPoolContended(t *testing.T) {
	n := concurrentAcquireSameKey(t, 1, 100*time.Millisecond)
	assert.Equal(t, int32(1), n, "must not grant a second lease after delayed pool checkout")
}
