package coordinator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tork/job"
	"github.com/tork/task"
)

func TestRedactTask(t *testing.T) {
	ta := task.Task{
		Env: map[string]string{
			"secret_1":          "secret",
			"SecrET_2":          "secret",
			"PASSword":          "password",
			"AWS_ACCESS_KEY_ID": "some-key",
			"harmless":          "hello world",
		},
		Pre: []task.Task{
			{
				Env: map[string]string{
					"secret_1": "secret",
					"harmless": "hello world",
				},
			},
		},
		Post: []task.Task{
			{
				Env: map[string]string{
					"secret_1": "secret",
					"harmless": "hello world",
				},
			},
		},
	}
	tr := redactTask(ta)

	assert.Equal(t, "[REDACTED]", tr.Env["secret_1"])
	assert.Equal(t, "[REDACTED]", tr.Env["SecrET_2"])
	assert.Equal(t, "[REDACTED]", tr.Env["PASSword"])
	assert.Equal(t, "hello world", tr.Env["harmless"])
	assert.Equal(t, "[REDACTED]", tr.Env["AWS_ACCESS_KEY_ID"])
	assert.Equal(t, "[REDACTED]", tr.Pre[0].Env["secret_1"])
	assert.Equal(t, "[REDACTED]", tr.Pre[0].Env["secret_1"])
	assert.Equal(t, "hello world", tr.Pre[0].Env["harmless"])
	assert.Equal(t, "[REDACTED]", tr.Post[0].Env["secret_1"])
	assert.Equal(t, "hello world", tr.Post[0].Env["harmless"])
}

func TestRedactJob(t *testing.T) {
	ta := task.Task{
		Env: map[string]string{
			"secret_1":          "secret",
			"SecrET_2":          "secret",
			"PASSword":          "password",
			"AWS_ACCESS_KEY_ID": "some-key",
			"harmless":          "hello world",
		},
	}
	j := redactJob(job.Job{
		Tasks: []task.Task{ta},
		Inputs: map[string]string{
			"secret": "password",
			"plain":  "helloworld",
		},
		Context: job.Context{
			Inputs: map[string]string{
				"secret": "password",
				"plain":  "helloworld",
			},
			Tasks: map[string]map[string]string{
				"task1": {
					"secret": "password",
					"plain":  "helloworld",
				},
			},
		},
	})
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
	assert.Equal(t, map[string]map[string]string{
		"task1": {
			"secret": "[REDACTED]",
			"plain":  "helloworld",
		}}, j.Context.Tasks)
}
