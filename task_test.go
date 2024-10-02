package tork_test

import (
	"testing"

	"github.com/runabol/tork"
	"github.com/stretchr/testify/assert"
)

func TestCloneTask(t *testing.T) {
	t1 := &tork.Task{
		Env: map[string]string{
			"VAR1": "VAL1",
		},
		Limits: &tork.TaskLimits{
			CPUs: "1",
		},
		Parallel: &tork.ParallelTask{
			Tasks: []*tork.Task{
				{
					Env: map[string]string{
						"PVAR1": "PVAL1",
					},
				},
			},
		},
	}
	t2 := t1.Clone()
	assert.Equal(t, t1.Env, t2.Env)
	assert.Equal(t, t1.Limits.CPUs, t2.Limits.CPUs)
	assert.Equal(t, t1.Parallel.Tasks[0].Env, t2.Parallel.Tasks[0].Env)

	t2.Env["VAR2"] = "VAL2"
	t2.Limits.CPUs = "2"
	t2.Parallel.Tasks[0].Env["PVAR2"] = "PVAL2"
	assert.NotEqual(t, t1.Env, t2.Env)
	assert.NotEqual(t, t1.Limits.CPUs, t2.Limits.CPUs)
	assert.NotEqual(t, t1.Parallel.Tasks[0].Env, t2.Parallel.Tasks[0].Env)
}

func TestIsActive(t *testing.T) {
	t1 := &tork.Task{
		State: tork.TaskStateCancelled,
	}
	assert.False(t, t1.State.IsActive())
	t2 := &tork.Task{
		State: tork.TaskStateCreated,
	}
	assert.True(t, t2.State.IsActive())
	t3 := &tork.Task{
		State: tork.TaskStatePending,
	}
	assert.True(t, t3.State.IsActive())
	t4 := &tork.Task{
		State: tork.TaskStateRunning,
	}
	assert.True(t, t4.State.IsActive())
	t5 := &tork.Task{
		State: tork.TaskStateCompleted,
	}
	assert.False(t, t5.State.IsActive())
}
