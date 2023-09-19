package input

import (
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/runabol/tork"
	"github.com/runabol/tork/mq"
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
	err := j.Validate()
	assert.NoError(t, err)
}

func TestValidateJobNoTasks(t *testing.T) {
	j := Job{
		Name:  "test job",
		Tasks: []Task{},
	}
	err := j.Validate()
	assert.Error(t, err)
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
	err := j.Validate()
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
	err = j.Validate()
	assert.Error(t, err)

	j = Job{
		Name: "test job",
		Tasks: []Task{
			{
				Name:  "test task",
				Image: "some:image",
				Queue: mq.QUEUE_JOBS,
			},
		},
	}
	err = j.Validate()
	assert.Error(t, err)
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
	err := j.Validate()
	assert.Error(t, err)
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
	err := j.Validate()
	assert.Error(t, err)
	errs := err.(validator.ValidationErrors)
	assert.Equal(t, "Timeout", errs[0].Field())
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
	err := j.Validate()
	assert.Error(t, err)
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
	err := j.Validate()
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
	err = j.Validate()
	assert.Error(t, err)
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
	err := j.Validate()
	assert.NoError(t, err)
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
	err := j.Validate()
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
	err = j.Validate()
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
	err = j.Validate()
	assert.Error(t, err)
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
	err := j.Validate()
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
	err = j.Validate()
	assert.Error(t, err)
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
	err := j.Validate()
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
	err = j.Validate()
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
	err = j.Validate()
	assert.Error(t, err)
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
	err := j.Validate()
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
	err = j.Validate()
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
						Type:   "bad type",
						Target: "/some/target",
					},
				},
			},
		},
	}
	err = j.Validate()
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
						Source: "", // missing
						Target: "/some/target",
					},
				},
			},
		},
	}
	err = j.Validate()
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
	err = j.Validate()
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
						Source: "../some/source", // invalid
						Target: "/some/target",
					},
				},
			},
		},
	}
	err = j.Validate()
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
						Source: "/some#/source", // invalid
						Target: "/some/target",
					},
				},
			},
		},
	}
	err = j.Validate()
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
	err = j.Validate()
	assert.Error(t, err)
}
