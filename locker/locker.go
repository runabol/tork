package locker

import (
	"context"
)

const (
	LOCKER_INMEMORY = "inmemory"
	LOCKER_POSTGRES = "postgres"
)

type Lock interface {
	ReleaseLock(ctx context.Context) error
}

type Locker interface {
	AcquireLock(ctx context.Context, key string) (Lock, error)
}
