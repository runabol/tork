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
	}
	tr := redactTask(ta)
	// original should remain untouched
	assert.Equal(t, "secret", ta.Env["secret_1"])
	assert.Equal(t, "secret", ta.Env["SecrET_2"])
	assert.Equal(t, "password", ta.Env["PASSword"])
	assert.Equal(t, "hello world", ta.Env["harmless"])
	assert.Equal(t, "some-key", ta.Env["AWS_ACCESS_KEY_ID"])
	// redacted version
	assert.Equal(t, "[REDACTED]", tr.Env["secret_1"])
	assert.Equal(t, "[REDACTED]", tr.Env["SecrET_2"])
	assert.Equal(t, "[REDACTED]", tr.Env["PASSword"])
	assert.Equal(t, "hello world", tr.Env["harmless"])
	assert.Equal(t, "[REDACTED]", tr.Env["AWS_ACCESS_KEY_ID"])
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
	})
	assert.Equal(t, "[REDACTED]", j.Tasks[0].Env["secret_1"])
	assert.Equal(t, "[REDACTED]", j.Tasks[0].Env["SecrET_2"])
	assert.Equal(t, "[REDACTED]", j.Tasks[0].Env["PASSword"])
	assert.Equal(t, "hello world", j.Tasks[0].Env["harmless"])
	assert.Equal(t, "[REDACTED]", j.Tasks[0].Env["AWS_ACCESS_KEY_ID"])
}
