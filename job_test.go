package tork_test

import (
	"testing"

	"github.com/runabol/tork"
	"github.com/stretchr/testify/assert"
)

func TestClone(t *testing.T) {
	j1 := &tork.Job{
		Context: tork.JobContext{
			Inputs: map[string]string{
				"INPUT1": "VAL1",
			},
			Job: map[string]string{
				"id":   "some-id",
				"name": "my job",
			},
		},
		Tasks: []*tork.Task{
			{
				Env: map[string]string{
					"VAR1": "VAL1",
				},
			},
		},
		Execution: []*tork.Task{
			{
				Env: map[string]string{
					"EVAR1": "EVAL1",
				},
			},
		},
	}

	j2 := j1.Clone()

	assert.Equal(t, j1.Context.Inputs, j2.Context.Inputs)
	assert.Equal(t, j1.Context.Job, j2.Context.Job)
	assert.Equal(t, j1.Tasks[0].Env, j2.Tasks[0].Env)
	assert.Equal(t, j1.Execution[0].Env, j2.Execution[0].Env)

	j2.Context.Inputs["INPUT2"] = "VAL2"
	j2.Tasks[0].Env["VAR2"] = "VAL2"
	j2.Execution[0].Env["EVAR2"] = "VAL2"
	assert.NotEqual(t, j1.Context.Inputs, j2.Context.Inputs)
	assert.NotEqual(t, j1.Tasks[0].Env, j2.Tasks[0].Env)
	assert.NotEqual(t, j1.Execution[0].Env, j2.Execution[0].Env)
}
