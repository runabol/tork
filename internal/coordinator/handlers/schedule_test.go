package handlers

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/runabol/tork/internal/uuid"
	"github.com/runabol/tork/locker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const postgresDSN = "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"

// runScheduledTaskOnTwoCoordinators simulates two coordinators that registered the
// same scheduled job and trigger a fire at the same instant (via RunNow).
func runScheduledTaskOnTwoCoordinators(t *testing.T, maxOpenConns int, scheduleID string, onFire func()) {
	t.Helper()

	sc1 := newDistributedScheduler(t, newPostgresLocker(t, maxOpenConns))
	sc2 := newDistributedScheduler(t, newPostgresLocker(t, maxOpenConns))

	register := func(sc gocron.Scheduler) gocron.Job {
		j, err := sc.NewJob(
			gocron.DurationJob(time.Hour),
			gocron.NewTask(onFire),
			gocron.WithName(scheduleID),
		)
		require.NoError(t, err)
		return j
	}

	j1 := register(sc1)
	j2 := register(sc2)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		require.NoError(t, j1.RunNow())
	}()
	go func() {
		defer wg.Done()
		require.NoError(t, j2.RunNow())
	}()
	wg.Wait()
}

func TestScheduledJobDistributedLockSingleRunWithoutPoolContention(t *testing.T) {
	scheduleID := uuid.NewUUID()

	var runs atomic.Int32
	runScheduledTaskOnTwoCoordinators(t, 25, scheduleID, func() {
		runs.Add(1)
	})

	assert.Eventually(t, func() bool {
		return runs.Load() >= 1
	}, 5*time.Second, 50*time.Millisecond, "expected at least one coordinator to run the job")

	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, int32(1), runs.Load(), "only one coordinator should execute when lock contention fails fast")
}

// TestScheduledJobNoDuplicateWhenLockerPoolContended is a regression test for duplicate
// cron fires under postgres locker pool pressure.
//
// With the lease-based locker, AcquireLock uses a short pooled query and does not hold a
// connection for the job run. A second coordinator may wait briefly for MaxOpenConns(1),
// but it should fail to take the lease while the first still holds it—not run the job
// after the first finishes.
func TestScheduledJobNoDuplicateWhenLockerPoolContended(t *testing.T) {
	scheduleID := uuid.NewUUID()

	var runs atomic.Int32
	runScheduledTaskOnTwoCoordinators(t, 1, scheduleID, func() {
		runs.Add(1)
	})

	assert.Eventually(t, func() bool {
		return runs.Load() >= 1
	}, 5*time.Second, 50*time.Millisecond, "expected one coordinator to run the job")

	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, int32(1), runs.Load(), "must not run twice after delayed pool checkout")
}

func newPostgresLocker(t *testing.T, maxOpenConns int) locker.Locker {
	t.Helper()
	l, err := locker.NewPostgresLocker(
		postgresDSN,
		locker.WithMaxOpenConns(maxOpenConns),
		locker.WithMaxIdleConns(maxOpenConns),
	)
	if err != nil {
		t.Skipf("postgres locker unavailable: %v", err)
	}
	return l
}

func newDistributedScheduler(t *testing.T, l locker.Locker) gocron.Scheduler {
	t.Helper()
	sc, err := gocron.NewScheduler(gocron.WithDistributedLocker(glocker{locker: l}))
	require.NoError(t, err)
	sc.Start()
	t.Cleanup(func() {
		require.NoError(t, sc.Shutdown())
	})
	return sc
}
