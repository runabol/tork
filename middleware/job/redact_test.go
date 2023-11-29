package job

import (
	"context"
	"testing"

	"github.com/runabol/tork"
	"github.com/runabol/tork/internal/redact"
	"github.com/stretchr/testify/assert"
)

func TestRedactOnRead(t *testing.T) {
	hm := ApplyMiddleware(NoOpHandlerFunc, []MiddlewareFunc{Redact(redact.NewRedacter())})
	j := &tork.Job{
		Inputs: map[string]string{
			"secret": "1234",
		},
	}
	assert.NoError(t, hm(context.Background(), Read, j))
	assert.Equal(t, "[REDACTED]", j.Inputs["secret"])
}

func TestNoRedact(t *testing.T) {
	hm := ApplyMiddleware(NoOpHandlerFunc, []MiddlewareFunc{Redact(redact.NewRedacter())})
	j := &tork.Job{
		Inputs: map[string]string{
			"secret": "1234",
		},
	}
	assert.NoError(t, hm(context.Background(), StateChange, j))
	assert.Equal(t, "1234", j.Inputs["secret"])
}
