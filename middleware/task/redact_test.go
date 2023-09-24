package task

import (
	"context"
	"testing"

	"github.com/runabol/tork"
	"github.com/stretchr/testify/assert"
)

func TestRedactOnRead(t *testing.T) {
	hm := ApplyMiddleware(NoOpHandlerFunc, []MiddlewareFunc{Redact})
	t1 := &tork.Task{
		Env: map[string]string{
			"secret": "1234",
		},
	}
	assert.NoError(t, hm(context.Background(), Read, t1))
	assert.Equal(t, "[REDACTED]", t1.Env["secret"])
}

func TestNoRedact(t *testing.T) {
	hm := ApplyMiddleware(NoOpHandlerFunc, []MiddlewareFunc{Redact})
	t1 := &tork.Task{
		Env: map[string]string{
			"secret": "1234",
		},
	}
	assert.NoError(t, hm(context.Background(), StateChange, t1))
	assert.Equal(t, "1234", t1.Env["secret"])
}
