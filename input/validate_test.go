package input

import (
	"strings"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/runabol/tork"
	"github.com/runabol/tork/broker"
	"github.com/runabol/tork/datastore/postgres"

	"github.com/stretchr/testify/assert"
)

func TestValidateMinJob(t *testing.T) {
	j := Job{
		Name: "test job",
		Tasks: []Task{
			{
				Name:  "test task",
				Image: "some:image",
			},
		},
	}
	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	err = j.Validate(ds)
	assert.NoError(t, err)
	assert.NoError(t, ds.Close())
}

func TestValidateJobNoTasks(t *testing.T) {
	j := Job{
		Name:  "test job",
		Tasks: []Task{},
	}
	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	err = j.Validate(ds)
	assert.Error(t, err)
	assert.NoError(t, ds.Close())
}

func TestValidateQueue(t *testing.T) {
	j := Job{
		Name: "test job",
		Tasks: []Task{
			{
				Name:  "test task",
				Image: "some:image",
				Queue: "urgent",
			},
		},
	}
	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	err = j.Validate(ds)
	assert.NoError(t, err)

	j = Job{
		Name: "test job",
		Tasks: []Task{
			{
				Name:  "test task",
				Image: "some:image",
				Queue: "x-788222",
			},
		},
	}
	err = j.Validate(ds)
	assert.Error(t, err)

	j = Job{
		Name: "test job",
		Tasks: []Task{
			{
				Name:  "test task",
				Image: "some:image",
				Queue: broker.QUEUE_JOBS,
			},
		},
	}
	err = j.Validate(ds)
	assert.Error(t, err)
	assert.NoError(t, ds.Close())
}

func TestValidateJobNoName(t *testing.T) {
	j := Job{
		Name: "test job",
		Tasks: []Task{
			{
				Image: "some:image",
			},
		},
	}
	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	err = j.Validate(ds)
	assert.Error(t, err)
	assert.NoError(t, ds.Close())
}

func TestValidateVar(t *testing.T) {
	j := Job{
		Name: "test job",
		Tasks: []Task{
			{
				Name: "test task",
				Var:  "somevar",
			},
		},
	}
	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	err = j.Validate(ds)
	assert.NoError(t, err)

	j = Job{
		Name: "test job",
		Tasks: []Task{
			{
				Name: "test task",
				Var:  strings.Repeat("a", 64),
			},
		},
	}
	err = j.Validate(ds)
	assert.NoError(t, err)

	j = Job{
		Name: "test job",
		Tasks: []Task{
			{
				Name: "test task",
				Var:  strings.Repeat("a", 65),
			},
		},
	}
	err = j.Validate(ds)
	assert.Error(t, err)
	assert.NoError(t, ds.Close())
}

func TestValidateJobDefaults(t *testing.T) {
	j := Job{
		Name: "test job",
		Tasks: []Task{
			{
				Name:  "some task",
				Image: "some:image",
			},
		},
		Defaults: &Defaults{
			Timeout: "1234",
		},
	}
	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	err = j.Validate(ds)
	assert.Error(t, err)
	errs := err.(validator.ValidationErrors)
	assert.Equal(t, "Timeout", errs[0].Field())
	assert.NoError(t, ds.Close())
}

func TestValidateJobTaskNoName(t *testing.T) {
	j := Job{
		Name: "test job",
		Tasks: []Task{
			{
				Image: "some:image",
			},
		},
	}
	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	err = j.Validate(ds)
	assert.Error(t, err)
	assert.NoError(t, ds.Close())
}

func TestValidateJobTaskNoImage(t *testing.T) {
	j := Job{
		Name: "test job",
		Tasks: []Task{
			{
				Name: "some task",
			},
		},
	}
	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	err = j.Validate(ds)
	assert.NoError(t, err)
	assert.NoError(t, ds.Close())
}

func TestValidateJobTaskRetry(t *testing.T) {
	j := Job{
		Name: "test job",
		Tasks: []Task{
			{
				Name:  "test task",
				Image: "some:image",
				Retry: &Retry{
					Limit: 5,
				},
			},
		},
	}
	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	err = j.Validate(ds)
	assert.NoError(t, err)

	j = Job{
		Name: "test job",
		Tasks: []Task{
			{
				Name:  "test task",
				Image: "some:image",
				Retry: &Retry{
					Limit: 50,
				},
			},
		},
	}
	err = j.Validate(ds)
	assert.Error(t, err)
	assert.NoError(t, ds.Close())
}

func TestValidateJobTaskTimeout(t *testing.T) {
	j := Job{
		Name: "test job",
		Tasks: []Task{
			{
				Name:    "test task",
				Image:   "some:image",
				Timeout: "6h",
			},
		},
	}
	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	err = j.Validate(ds)
	assert.NoError(t, err)
	assert.NoError(t, ds.Close())
}

func TestValidateSubJob(t *testing.T) {
	j := Job{
		Name: "test job",
		Tasks: []Task{
			{
				Name: "test task",
				SubJob: &SubJob{
					Name: "test sub job",
					Webhooks: []Webhook{{
						URL: "http://example.com",
					}},
					Tasks: []Task{{
						Name:  "test task",
						Image: "some task",
					}},
				},
			},
		},
	}
	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	err = j.Validate(ds)
	assert.NoError(t, err)
	assert.NoError(t, ds.Close())
}

func TestValidateSubJobBadWebhook(t *testing.T) {
	j := Job{
		Name: "test job",
		Tasks: []Task{
			{
				Name: "test task",
				SubJob: &SubJob{
					Name: "test sub job",
					Webhooks: []Webhook{{
						URL: "",
					}},
					Tasks: []Task{{
						Name:  "test task",
						Image: "some task",
					}},
				},
			},
		},
	}
	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	err = j.Validate(ds)
	assert.Error(t, err)
	assert.NoError(t, ds.Close())
}

func TestValidateParallelOrEachTaskType(t *testing.T) {
	j := Job{
		Name: "test job",
		Tasks: []Task{
			{
				Name: "test task",
				Each: &Each{
					List: "5+5",
					Task: Task{
						Name:  "test task",
						Image: "some task",
					},
				},
			},
		},
	}
	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	err = j.Validate(ds)
	assert.NoError(t, err)

	j = Job{
		Name: "test job",
		Tasks: []Task{
			{
				Name: "test task",
				Parallel: &Parallel{
					Tasks: []Task{
						{
							Name:  "test task",
							Image: "some task",
						},
					},
				},
			},
		},
	}
	err = j.Validate(ds)
	assert.NoError(t, err)

	j = Job{
		Name: "test job",
		Tasks: []Task{
			{
				Name:    "test task",
				Image:   "some:image",
				Timeout: "6h",
				Each: &Each{
					List: "some expression",
					Task: Task{
						Name:  "test task",
						Image: "some task",
					},
				},
				Parallel: &Parallel{
					Tasks: []Task{
						{
							Name:  "test task",
							Image: "some task",
						},
					},
				},
			},
		},
	}
	err = j.Validate(ds)
	assert.Error(t, err)
	assert.NoError(t, ds.Close())
}

func TestValidateParallelOrSubJobTaskType(t *testing.T) {
	j := Job{
		Name: "test job",
		Tasks: []Task{
			{
				Name: "test task",
				Parallel: &Parallel{
					Tasks: []Task{
						{
							Name:  "test task",
							Image: "some task",
						},
					},
				},
			},
		},
	}
	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	err = j.Validate(ds)
	assert.NoError(t, err)

	j = Job{
		Name: "test job",
		Tasks: []Task{
			{
				Name:  "test task",
				Image: "some:image",
				Parallel: &Parallel{
					Tasks: []Task{
						{
							Name:  "test task",
							Image: "some task",
						},
					},
				},
				SubJob: &SubJob{
					Name: "test sub job",
					Tasks: []Task{{
						Name:  "test task",
						Image: "some task",
					}},
				},
			},
		},
	}
	err = j.Validate(ds)
	assert.Error(t, err)
	assert.NoError(t, ds.Close())
}

func TestValidateExpr(t *testing.T) {
	j := Job{
		Name: "test job",
		Tasks: []Task{
			{
				Name: "test task",
				Each: &Each{
					List: "1+1",
					Task: Task{
						Name:  "test task",
						Image: "some:image",
					},
				},
			},
		},
	}
	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	err = j.Validate(ds)
	assert.NoError(t, err)

	j = Job{
		Name: "test job",
		Tasks: []Task{
			{
				Name: "test task",
				Each: &Each{
					List: "{{1+1}}",
					Task: Task{
						Name:  "test task",
						Image: "some:image",
					},
				},
			},
		},
	}
	err = j.Validate(ds)
	assert.NoError(t, err)

	j = Job{
		Name: "test job",
		Tasks: []Task{
			{
				Name: "test task",
				Each: &Each{
					List: "{1+1",
					Task: Task{
						Name:  "test task",
						Image: "some:image",
					},
				},
			},
		},
	}
	err = j.Validate(ds)
	assert.Error(t, err)
	assert.NoError(t, ds.Close())
}

func TestValidateMounts(t *testing.T) {
	j := Job{
		Name: "test job",
		Tasks: []Task{
			{
				Name:  "test task",
				Image: "some:image",
				Run:   "some script",
				Mounts: []Mount{
					{
						Type:   "", // missing
						Target: "", // missing
					},
				},
			},
		},
	}
	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	err = j.Validate(ds)
	assert.Error(t, err)

	j = Job{
		Name: "test job",
		Tasks: []Task{
			{
				Name:  "test task",
				Image: "some:image",
				Run:   "some script",
				Mounts: []Mount{
					{
						Type:   tork.MountTypeVolume,
						Target: "/some/target",
					},
				},
			},
		},
	}
	err = j.Validate(ds)
	assert.NoError(t, err)

	j = Job{
		Name: "test job",
		Tasks: []Task{
			{
				Name:  "test task",
				Image: "some:image",
				Run:   "some script",
				Mounts: []Mount{
					{
						Type:   "custom",
						Target: "/some/target",
					},
				},
			},
		},
	}
	err = j.Validate(ds)
	assert.NoError(t, err)

	j = Job{
		Name: "test job",
		Tasks: []Task{
			{
				Name:  "test task",
				Image: "some:image",
				Run:   "some script",
				Mounts: []Mount{
					{
						Type:   tork.MountTypeBind,
						Source: "", // missing
						Target: "/some/target",
					},
				},
			},
		},
	}
	err = j.Validate(ds)
	assert.Error(t, err)

	j = Job{
		Name: "test job",
		Tasks: []Task{
			{
				Name:  "test task",
				Image: "some:image",
				Run:   "some script",
				Mounts: []Mount{
					{
						Type:   tork.MountTypeBind,
						Source: "/some/source",
						Target: "/some/target",
					},
				},
			},
		},
	}
	err = j.Validate(ds)
	assert.NoError(t, err)

	j = Job{
		Name: "test job",
		Tasks: []Task{
			{
				Name:  "test task",
				Image: "some:image",
				Run:   "some script",
				Mounts: []Mount{
					{
						Type:   tork.MountTypeBind,
						Source: "/some#/source", // invalid
						Target: "/some/target",
					},
				},
			},
		},
	}
	err = j.Validate(ds)
	assert.Error(t, err)

	j = Job{
		Name: "test job",
		Tasks: []Task{
			{
				Name:  "test task",
				Image: "some:image",
				Run:   "some script",
				Mounts: []Mount{
					{
						Type:   tork.MountTypeBind,
						Source: "/some/source",
						Target: "/some:/target", // invalid
					},
				},
			},
		},
	}
	err = j.Validate(ds)
	assert.Error(t, err)

	j = Job{
		Name: "test job",
		Tasks: []Task{
			{
				Name:  "test task",
				Image: "some:image",
				Run:   "some script",
				Mounts: []Mount{
					{
						Type:   tork.MountTypeBind,
						Source: "/some/source",
						Target: "/tork",
					},
				},
			},
		},
	}
	err = j.Validate(ds)
	assert.Error(t, err)

	j = Job{
		Name: "test job",
		Tasks: []Task{
			{
				Name:  "test task",
				Image: "some:image",
				Run:   "some script",
				Mounts: []Mount{
					{
						Type:   tork.MountTypeBind,
						Source: "bucket=some-bucket path=/mnt/some-path",
						Target: "/some/path",
					},
				},
			},
		},
	}
	err = j.Validate(ds)
	assert.NoError(t, err)
	assert.NoError(t, ds.Close())
}

func TestValidateWebhook(t *testing.T) {
	j := Job{
		Name: "test job",
		Webhooks: []Webhook{{
			URL: "http://example.com",
		}},
		Tasks: []Task{
			{
				Name:  "test task",
				Image: "some:image",
			},
		},
	}
	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	err = j.Validate(ds)
	assert.NoError(t, err)

	j = Job{
		Name: "test job",
		Webhooks: []Webhook{{
			URL: "",
		}},
		Tasks: []Task{
			{
				Name:  "test task",
				Image: "some:image",
			},
		},
	}
	err = j.Validate(ds)
	assert.Error(t, err)
	assert.NoError(t, ds.Close())
}

func TestValidateCron(t *testing.T) {
	validate := validator.New()
	err := validate.RegisterValidation("cron", validateCron)
	assert.NoError(t, err)

	tests := []struct {
		name      string
		cron      string
		shouldErr bool
	}{
		{"Valid cron expression", "0 0 * * *", false},
		{"Valid cron expression", "0/10 0 * * *", false},
		{"Invalid cron expression", "invalid-cron", true},
		{"Empty cron expression", "", true},
		{"Valid cron expression with seconds", "0 0 0 * * *", true},
		{"Invalid cron expression with extra field", "0 0 0 0 * * *", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validate.Var(tt.cron, "cron")
			if tt.shouldErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
