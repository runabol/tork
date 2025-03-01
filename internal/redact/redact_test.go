package redact

import (
	"context"
	"testing"

	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore/postgres"
	"github.com/runabol/tork/internal/uuid"
	"github.com/stretchr/testify/assert"
)

func TestRedactTask(t *testing.T) {
	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	ctx := context.Background()
	j1 := tork.Job{
		ID: uuid.NewUUID(),
		Secrets: map[string]string{
			"hush": "shhhhh",
		},
	}
	err = ds.CreateJob(ctx, &j1)
	assert.NoError(t, err)

	ta := tork.Task{
		JobID: j1.ID,
		Env: map[string]string{
			"secret_1":          "secret",
			"SecrET_2":          "secret",
			"PASSword":          "password",
			"AWS_ACCESS_KEY_ID": "some-key",
			"harmless":          "hello world",
			"thing":             "shhhhh",
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
		SubJob: &tork.SubJobTask{
			Secrets: map[string]string{
				"hush": "shhhhh",
			},
		},
		Registry: &tork.Registry{
			Username: "me",
			Password: "secret",
		},
	}

	redacter := NewRedacter(ds)

	redacter.RedactTask(&ta)

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
	assert.Equal(t, "[REDACTED]", ta.Env["thing"])
	assert.Equal(t, "[REDACTED]", ta.SubJob.Secrets["hush"])
	assert.NoError(t, ds.Close())
}

func TestRedactJob(t *testing.T) {
	o := &tork.Job{
		Secrets: map[string]string{
			"something": "shhhhh",
		},
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
		Webhooks: []*tork.Webhook{
			{
				URL: "http://example.com/1",
			},
			{
				URL: "http://example.com/2",
				Headers: map[string]string{
					"my-header":      "my-value",
					"my-secret":      "secret",
					"another-header": "shhhhh",
				},
			},
		},
	}
	j := o.Clone()

	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	redacter := NewRedacter(ds)
	redacter.RedactJob(j)

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
	assert.Equal(t, "http://example.com/1", j.Webhooks[0].URL)
	assert.Equal(t, "http://example.com/2", j.Webhooks[1].URL)
	assert.Equal(t, map[string]string{"my-header": "my-value", "my-secret": "[REDACTED]", "another-header": "[REDACTED]"}, j.Webhooks[1].Headers)
}

func TestRedactJobContains(t *testing.T) {
	o := &tork.Job{
		Tasks: []*tork.Task{
			{
				Env: map[string]string{
					"secret_1": "secret",
					"PASSword": "password",
					"harmless": "hello world",
				},
			},
		},
	}
	j := o.Clone()

	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	redacter := NewRedacter(ds)
	redacter.RedactJob(j)

	assert.Equal(t, "[REDACTED]", j.Tasks[0].Env["secret_1"])
	assert.Equal(t, "[REDACTED]", j.Tasks[0].Env["PASSword"])
	assert.Equal(t, "hello world", j.Tasks[0].Env["harmless"])
	assert.NoError(t, ds.Close())
}

func TestRedactJobWildcard(t *testing.T) {
	o := &tork.Job{
		Tasks: []*tork.Task{
			{
				Env: map[string]string{
					"secret_1":  "secret",
					"_secret_2": "secret",
					"PASSword":  "password",
					"harmless":  "hello world",
				},
			},
		},
	}
	j := o.Clone()

	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	redacter := NewRedacter(ds, Wildcard("secret*"))
	redacter.RedactJob(j)

	assert.Equal(t, "[REDACTED]", j.Tasks[0].Env["secret_1"])
	assert.Equal(t, "secret", j.Tasks[0].Env["_secret_2"])
	assert.Equal(t, "password", j.Tasks[0].Env["PASSword"])
	assert.Equal(t, "hello world", j.Tasks[0].Env["harmless"])
	assert.NoError(t, ds.Close())
}
