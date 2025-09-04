package log

import (
	"context"
	"testing"
	"time"

	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore/postgres"
	"github.com/runabol/tork/internal/uuid"
	"github.com/stretchr/testify/assert"
)

func TestRedactOnRead(t *testing.T) {
	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)

	j := &tork.Job{
		ID: uuid.NewUUID(),
		Secrets: map[string]string{
			"secret": "1234",
		},
	}

	err = ds.CreateJob(context.Background(), j)
	assert.NoError(t, err)

	now := time.Now().UTC()

	tk := &tork.Task{
		ID:        uuid.NewUUID(),
		JobID:     j.ID,
		CreatedAt: &now,
	}

	err = ds.CreateTask(context.Background(), tk)
	assert.NoError(t, err)

	hm := ApplyMiddleware(NoOpHandlerFunc, []MiddlewareFunc{Redact(ds)})
	p := &tork.TaskLogPart{
		Contents: "line 1 -- 1234",
		TaskID:   tk.ID,
	}
	assert.NoError(t, hm(context.Background(), Read, []*tork.TaskLogPart{p}))
	assert.Equal(t, "line 1 -- [REDACTED]", p.Contents)
	assert.NoError(t, ds.Close())
}
