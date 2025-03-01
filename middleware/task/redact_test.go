package task

import (
	"context"
	"testing"

	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore/postgres"
	"github.com/runabol/tork/internal/redact"
	"github.com/runabol/tork/internal/uuid"
	"github.com/stretchr/testify/assert"
)

func TestRedactOnRead(t *testing.T) {
	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	ctx := context.Background()
	j1 := tork.Job{
		ID: uuid.NewUUID(),
	}
	err = ds.CreateJob(ctx, &j1)
	assert.NoError(t, err)
	hm := ApplyMiddleware(NoOpHandlerFunc, []MiddlewareFunc{Redact(redact.NewRedacter(ds))})
	t1 := &tork.Task{
		JobID: j1.ID,
		Env: map[string]string{
			"secret": "1234",
		},
	}
	assert.NoError(t, hm(context.Background(), Read, t1))
	assert.Equal(t, "[REDACTED]", t1.Env["secret"])
	assert.NoError(t, ds.Close())
}

func TestNoRedact(t *testing.T) {
	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	ctx := context.Background()
	j1 := tork.Job{
		ID: uuid.NewUUID(),
	}
	err = ds.CreateJob(ctx, &j1)
	assert.NoError(t, err)
	hm := ApplyMiddleware(NoOpHandlerFunc, []MiddlewareFunc{Redact(redact.NewRedacter(ds))})
	t1 := &tork.Task{
		JobID: j1.ID,
		Env: map[string]string{
			"secret": "1234",
		},
	}
	assert.NoError(t, hm(context.Background(), StateChange, t1))
	assert.Equal(t, "1234", t1.Env["secret"])
	assert.NoError(t, ds.Close())
}
