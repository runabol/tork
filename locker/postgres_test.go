package locker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewPostgresLocker(t *testing.T) {
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	locker, err := NewPostgresLocker(dsn)
	assert.NoError(t, err, "locker initialization should succeed")
	assert.NotNil(t, locker, "locker should not be nil")
}

func TestPostgresLocker_AcquireLock(t *testing.T) {
	dsn := "host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable"
	locker, err := NewPostgresLocker(dsn)
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

func Test_hashKey(t *testing.T) {
	i := hashKey("2c7eb7e1951343468ce360c906003a22")
	assert.Equal(t, int64(-414568140838410356), i)
}
