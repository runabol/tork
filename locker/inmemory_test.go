package locker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInmemoryLocker_AcquireLock(t *testing.T) {
	locker := NewInMemoryLocker()

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
