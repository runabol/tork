package redact

import (
	"testing"

	"github.com/runabol/tork"
	"github.com/stretchr/testify/assert"
)

func TestRedactTask(t *testing.T) {
	ta := tork.Task{
		Env: map[string]string{
			"secret_1":          "secret",
			"SecrET_2":          "secret",
			"PASSword":          "password",
			"AWS_ACCESS_KEY_ID": "some-key",
			"harmless":          "hello world",
		},
		Pre: []*tork.Task{
			{
				Env: map[string]string{
					"secret_1": "secret",
					"harmless": "hello world",
				},
			},
		},
		Post: []*tork.Task{
			{
				Env: map[string]string{
					"secret_1": "secret",
					"harmless": "hello world",
				},
			},
		},
		Parallel: &tork.ParallelTask{
			Tasks: []*tork.Task{
				{
					Env: map[string]string{
						"secret_1": "secret",
						"harmless": "hello world",
					},
				},
			},
		},
		Registry: &tork.Registry{
			Username: "me",
			Password: "secret",
		},
	}

	Task(&ta)

	assert.Equal(t, "[REDACTED]", ta.Env["secret_1"])
	assert.Equal(t, "[REDACTED]", ta.Env["SecrET_2"])
	assert.Equal(t, "[REDACTED]", ta.Env["PASSword"])
	assert.Equal(t, "hello world", ta.Env["harmless"])
	assert.Equal(t, "[REDACTED]", ta.Env["AWS_ACCESS_KEY_ID"])
	assert.Equal(t, "[REDACTED]", ta.Pre[0].Env["secret_1"])
	assert.Equal(t, "[REDACTED]", ta.Pre[0].Env["secret_1"])
	assert.Equal(t, "hello world", ta.Pre[0].Env["harmless"])
	assert.Equal(t, "[REDACTED]", ta.Post[0].Env["secret_1"])
	assert.Equal(t, "hello world", ta.Post[0].Env["harmless"])
	assert.Equal(t, "[REDACTED]", ta.Parallel.Tasks[0].Env["secret_1"])
	assert.Equal(t, "hello world", ta.Parallel.Tasks[0].Env["harmless"])
	assert.Equal(t, "[REDACTED]", ta.Registry.Password)
}

func TestRedactJob(t *testing.T) {
	o := &tork.Job{
		Tasks: []*tork.Task{
			{
				Env: map[string]string{
					"secret_1":          "secret",
					"SecrET_2":          "secret",
					"PASSword":          "password",
					"AWS_ACCESS_KEY_ID": "some-key",
					"harmless":          "hello world",
				},
			},
		},
		Execution: []*tork.Task{
			{
				Env: map[string]string{
					"secret_1":          "secret",
					"SecrET_2":          "secret",
					"PASSword":          "password",
					"AWS_ACCESS_KEY_ID": "some-key",
					"harmless":          "hello world",
				},
			},
		},
		Inputs: map[string]string{
			"secret": "password",
			"plain":  "helloworld",
		},
		Context: tork.JobContext{
			Inputs: map[string]string{
				"secret": "password",
				"plain":  "helloworld",
			},
			Tasks: map[string]string{
				"secret": "password",
				"task2":  "helloworld",
			},
		},
	}
	j := o.Clone()
	Job(j)
	assert.Equal(t, "[REDACTED]", j.Tasks[0].Env["secret_1"])
	assert.Equal(t, "[REDACTED]", j.Tasks[0].Env["SecrET_2"])
	assert.Equal(t, "[REDACTED]", j.Tasks[0].Env["PASSword"])
	assert.Equal(t, "hello world", j.Tasks[0].Env["harmless"])
	assert.Equal(t, "[REDACTED]", j.Tasks[0].Env["AWS_ACCESS_KEY_ID"])
	assert.Equal(t, "[REDACTED]", j.Inputs["secret"])
	assert.Equal(t, "helloworld", j.Inputs["plain"])
	assert.Equal(t, map[string]string{
		"secret": "[REDACTED]",
		"plain":  "helloworld",
	}, j.Context.Inputs)
	assert.Equal(t, map[string]string{
		"secret": "[REDACTED]",
		"task2":  "helloworld",
	}, j.Context.Tasks)
	assert.Equal(t, "[REDACTED]", j.Execution[0].Env["secret_1"])
}
