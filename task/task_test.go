package task_test

import (
	"testing"

	"github.com/runabol/tork/task"
	"github.com/stretchr/testify/assert"
)

func TestClone(t *testing.T) {
	t1 := &task.Task{
		Env: map[string]string{
			"VAR1": "VAL1",
		},
		Limits: &task.Limits{
			CPUs: "1",
		},
		Parallel: &task.Parallel{
			Tasks: []*task.Task{
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
