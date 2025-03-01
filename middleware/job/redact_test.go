package job

import (
	"context"
	"testing"

	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore/postgres"
	"github.com/runabol/tork/internal/redact"
	"github.com/stretchr/testify/assert"
)

func TestRedactOnRead(t *testing.T) {
	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	hm := ApplyMiddleware(NoOpHandlerFunc, []MiddlewareFunc{Redact(redact.NewRedacter(ds))})
	j := &tork.Job{
		Inputs: map[string]string{
			"secret": "1234",
		},
	}
	assert.NoError(t, hm(context.Background(), Read, j))
	assert.Equal(t, "[REDACTED]", j.Inputs["secret"])
	assert.NoError(t, ds.Close())
}

func TestNoRedact(t *testing.T) {
	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	hm := ApplyMiddleware(NoOpHandlerFunc, []MiddlewareFunc{Redact(redact.NewRedacter(ds))})
	j := &tork.Job{
		Inputs: map[string]string{
			"secret": "1234",
		},
	}
	assert.NoError(t, hm(context.Background(), StateChange, j))
	assert.Equal(t, "1234", j.Inputs["secret"])
	assert.NoError(t, ds.Close())
}
