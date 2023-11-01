package task

import (
	"context"
	"os"
	"testing"

	"github.com/runabol/tork"
	"github.com/stretchr/testify/assert"
)

func TestHostEnv1(t *testing.T) {
	mw, err := NewHostEnv("TORK_HOST_VAR1")
	os.Setenv("TORK_HOST_VAR1", "value1")
	defer func() {
		os.Unsetenv("TORK_HOST_VAR1")
	}()
	assert.NoError(t, err)
	hm := ApplyMiddleware(NoOpHandlerFunc, []MiddlewareFunc{mw.Execute})
	t1 := &tork.Task{
		State: tork.TaskStateScheduled,
	}
	assert.NoError(t, hm(context.Background(), StateChange, t1))
	assert.Equal(t, "value1", t1.Env["TORK_HOST_VAR1"])
}

func TestHostEnv2(t *testing.T) {
	mw, err := NewHostEnv("TORK_HOST_VAR2")
	os.Setenv("TORK_HOST_VAR2", "value2")
	defer func() {
		os.Unsetenv("TORK_HOST_VAR2")
	}()
	assert.NoError(t, err)
	hm := ApplyMiddleware(NoOpHandlerFunc, []MiddlewareFunc{mw.Execute})
	t1 := &tork.Task{
		State: tork.TaskStateScheduled,
		Env: map[string]string{
			"OTHER_VAR": "othervalue",
		},
	}
	assert.NoError(t, hm(context.Background(), StateChange, t1))
	assert.Equal(t, "value2", t1.Env["TORK_HOST_VAR2"])
	assert.Equal(t, "othervalue", t1.Env["OTHER_VAR"])
}

func TestHostEnv3(t *testing.T) {
	mw, err := NewHostEnv("TORK_HOST_VAR3:VAR3")
	os.Setenv("TORK_HOST_VAR3", "value3")
	defer func() {
		os.Unsetenv("TORK_HOST_VAR3")
	}()
	assert.NoError(t, err)
	hm := ApplyMiddleware(NoOpHandlerFunc, []MiddlewareFunc{mw.Execute})
	t1 := &tork.Task{
		State: tork.TaskStateScheduled,
		Env: map[string]string{
			"OTHER_VAR": "othervalue",
		},
	}
	assert.NoError(t, hm(context.Background(), StateChange, t1))
	assert.Equal(t, "value3", t1.Env["VAR3"])
	assert.Equal(t, "othervalue", t1.Env["OTHER_VAR"])
}

func TestHostEnv4(t *testing.T) {
	_, err := NewHostEnv("TORK_HOST_VAR4:VAR4_:XYZ")
	assert.Error(t, err)
}

func TestHostEnv5(t *testing.T) {
	mw, err := NewHostEnv("TORK_HOST_VAR5:VAR5")
	os.Setenv("TORK_HOST_VAR5", "value5")
	defer func() {
		os.Unsetenv("TORK_HOST_VAR5")
	}()
	assert.NoError(t, err)
	hm := ApplyMiddleware(NoOpHandlerFunc, []MiddlewareFunc{mw.Execute})
	t1 := &tork.Task{
		State: tork.TaskStateScheduled,
		Env: map[string]string{
			"OTHER_VAR": "othervalue",
		},
		Pre: []*tork.Task{{
			Name: "some pre task",
		}},
		Post: []*tork.Task{{
			Name: "some post task",
		}},
	}
	assert.NoError(t, hm(context.Background(), StateChange, t1))
	assert.Equal(t, "value5", t1.Env["VAR5"])
	assert.Equal(t, "othervalue", t1.Env["OTHER_VAR"])
	assert.Equal(t, "value5", t1.Pre[0].Env["VAR5"])
	assert.Equal(t, "value5", t1.Post[0].Env["VAR5"])
}
