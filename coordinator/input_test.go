package coordinator

import (
	"testing"

	"github.com/runabol/tork/mq"
	"github.com/stretchr/testify/assert"
)

func TestValidateMinJobInput(t *testing.T) {
	j := jobInput{
		Name: "test job",
		Tasks: []taskInput{
			{
				Name:  "test task",
				Image: "some:image",
			},
		},
	}
	err := j.validate()
	assert.NoError(t, err)
}

func TestValidateJobNoTasks(t *testing.T) {
	j := jobInput{
		Name:  "test job",
		Tasks: []taskInput{},
	}
	err := j.validate()
	assert.Error(t, err)
}

func TestValidateQueue(t *testing.T) {
	j := jobInput{
		Name: "test job",
		Tasks: []taskInput{
			{
				Name:  "test task",
				Image: "some:image",
				Queue: "urgent",
			},
		},
	}
	err := j.validate()
	assert.NoError(t, err)

	j = jobInput{
		Name: "test job",
		Tasks: []taskInput{
			{
				Name:  "test task",
				Image: "some:image",
				Queue: "x-788222",
			},
		},
	}
	err = j.validate()
	assert.Error(t, err)

	j = jobInput{
		Name: "test job",
		Tasks: []taskInput{
			{
				Name:  "test task",
				Image: "some:image",
				Queue: mq.QUEUE_JOBS,
			},
		},
	}
	err = j.validate()
	assert.Error(t, err)
}

func TestValidateJobInputNoName(t *testing.T) {
	j := jobInput{
		Name: "test job",
		Tasks: []taskInput{
			{
				Image: "some:image",
			},
		},
	}
	err := j.validate()
	assert.Error(t, err)
}

func TestValidateJobInputTaskNoName(t *testing.T) {
	j := jobInput{
		Name: "test job",
		Tasks: []taskInput{
			{
				Image: "some:image",
			},
		},
	}
	err := j.validate()
	assert.Error(t, err)
}

func TestValidateJobInputTaskRetry(t *testing.T) {
	j := jobInput{
		Name: "test job",
		Tasks: []taskInput{
			{
				Name:  "test task",
				Image: "some:image",
				Retry: &retryInput{
					Limit: 5,
				},
			},
		},
	}
	err := j.validate()
	assert.NoError(t, err)

	j = jobInput{
		Name: "test job",
		Tasks: []taskInput{
			{
				Name:  "test task",
				Image: "some:image",
				Retry: &retryInput{
					Limit: 50,
				},
			},
		},
	}
	err = j.validate()
	assert.Error(t, err)
}

func TestValidateJobInputTaskTimeout(t *testing.T) {
	j := jobInput{
		Name: "test job",
		Tasks: []taskInput{
			{
				Name:    "test task",
				Image:   "some:image",
				Timeout: "6h",
			},
		},
	}
	err := j.validate()
	assert.NoError(t, err)
}

func TestValidateParallelOrEachTaskType(t *testing.T) {
	j := jobInput{
		Name: "test job",
		Tasks: []taskInput{
			{
				Name: "test task",
				Each: &eachInput{
					List: "5+5",
					Task: taskInput{
						Name:  "test task",
						Image: "some task",
					},
				},
			},
		},
	}
	err := j.validate()
	assert.NoError(t, err)

	j = jobInput{
		Name: "test job",
		Tasks: []taskInput{
			{
				Name: "test task",
				Parallel: []taskInput{
					{
						Name:  "test task",
						Image: "some task",
					},
				},
			},
		},
	}
	err = j.validate()
	assert.NoError(t, err)

	j = jobInput{
		Name: "test job",
		Tasks: []taskInput{
			{
				Name:    "test task",
				Image:   "some:image",
				Timeout: "6h",
				Each: &eachInput{
					List: "some expression",
					Task: taskInput{
						Name:  "test task",
						Image: "some task",
					},
				},
				Parallel: []taskInput{
					{
						Name:  "test task",
						Image: "some task",
					},
				},
			},
		},
	}
	err = j.validate()
	assert.Error(t, err)
}

func TestValidateParallelOrSubJobTaskType(t *testing.T) {
	j := jobInput{
		Name: "test job",
		Tasks: []taskInput{
			{
				Name: "test task",
				Parallel: []taskInput{
					{
						Name:  "test task",
						Image: "some task",
					},
				},
			},
		},
	}
	err := j.validate()
	assert.NoError(t, err)

	j = jobInput{
		Name: "test job",
		Tasks: []taskInput{
			{
				Name:  "test task",
				Image: "some:image",
				Parallel: []taskInput{
					{
						Name:  "test task",
						Image: "some task",
					},
				},
				SubJob: &subJobInput{
					Name: "test sub job",
					Tasks: []taskInput{{
						Name:  "test task",
						Image: "some task",
					}},
				},
			},
		},
	}
	err = j.validate()
	assert.Error(t, err)
}

func TestValidateExpr(t *testing.T) {
	j := jobInput{
		Name: "test job",
		Tasks: []taskInput{
			{
				Name: "test task",
				Each: &eachInput{
					List: "1+1",
					Task: taskInput{
						Name:  "test task",
						Image: "some:image",
					},
				},
			},
		},
	}
	err := j.validate()
	assert.NoError(t, err)

	j = jobInput{
		Name: "test job",
		Tasks: []taskInput{
			{
				Name: "test task",
				Each: &eachInput{
					List: "{{1+1}}",
					Task: taskInput{
						Name:  "test task",
						Image: "some:image",
					},
				},
			},
		},
	}
	err = j.validate()
	assert.NoError(t, err)

	j = jobInput{
		Name: "test job",
		Tasks: []taskInput{
			{
				Name: "test task",
				Each: &eachInput{
					List: "{1+1",
					Task: taskInput{
						Name:  "test task",
						Image: "some:image",
					},
				},
			},
		},
	}
	err = j.validate()
	assert.Error(t, err)
}
