package eval_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tork/eval"
	"github.com/tork/job"
	"github.com/tork/task"
)

func TestEvalNoop(t *testing.T) {
	t1 := &task.Task{}
	err := eval.Evaluate(t1, job.Context{})
	assert.NoError(t, err)
	assert.Empty(t, t1.Env)
}

func TestEvalLiteral(t *testing.T) {
	t1 := &task.Task{
		Env: map[string]string{
			"HELLO": "WORLD",
		},
	}
	err := eval.Evaluate(t1, job.Context{})
	assert.NoError(t, err)
	assert.Equal(t, "WORLD", t1.Env["HELLO"])
}

func TestEvalVar(t *testing.T) {
	t1 := &task.Task{
		Env: map[string]string{
			"HELLO": `{{inputs.SOMEVAR}}`,
		},
	}
	err := eval.Evaluate(t1, job.Context{
		Inputs: map[string]string{
			"SOMEVAR": "SOME DATA",
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, "SOME DATA", t1.Env["HELLO"])
}

func TestEvalName(t *testing.T) {
	t1 := &task.Task{
		Name: "{{ inputs.SOMENAME }}y",
	}
	err := eval.Evaluate(t1, job.Context{
		Inputs: map[string]string{
			"SOMENAME": "John Smith",
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, "John Smithy", t1.Name)
}

func TestEvalImage(t *testing.T) {
	t1 := &task.Task{
		Image: "ubuntu:{{ inputs.TAG }}",
	}
	err := eval.Evaluate(t1, job.Context{
		Inputs: map[string]string{
			"TAG": "maverick",
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, "ubuntu:maverick", t1.Image)
}

func TestEvalQueue(t *testing.T) {
	t1 := &task.Task{
		Queue: "{{ inputs.QUEUE }}",
	}
	err := eval.Evaluate(t1, job.Context{
		Inputs: map[string]string{
			"QUEUE": "default",
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, "default", t1.Queue)
}

func TestEvalFunc(t *testing.T) {
	t1 := &task.Task{
		Env: map[string]string{
			"RAND_NUM": "{{ randomInt() }}",
		},
	}
	err := eval.Evaluate(t1, job.Context{})
	assert.NoError(t, err)
	assert.NotEmpty(t, t1.Env["RAND_NUM"])
	intVar, err := strconv.Atoi(t1.Env["RAND_NUM"])
	assert.NoError(t, err)
	assert.Greater(t, intVar, 0)
}

func TestDontEvalRun(t *testing.T) {
	t1 := &task.Task{
		Run: "Hello {{ inputs.NAME }}",
	}
	err := eval.Evaluate(t1, job.Context{
		Inputs: map[string]string{
			"NAME": "world",
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, "Hello {{ inputs.NAME }}", t1.Run)
}

func TestEvalPre(t *testing.T) {
	t1 := &task.Task{
		Pre: []task.Task{
			{
				Env: map[string]string{
					"HELLO": "{{ inputs.SOMEVAR }}",
				},
			},
		},
	}
	err := eval.Evaluate(t1, job.Context{
		Inputs: map[string]string{
			"SOMEVAR": "SOME DATA",
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, "SOME DATA", t1.Pre[0].Env["HELLO"])
}

func TestEvalPost(t *testing.T) {
	t1 := &task.Task{
		Post: []task.Task{
			{
				Env: map[string]string{
					"HELLO": "{{ inputs.SOMEVAR }}",
				},
			},
		},
	}
	err := eval.Evaluate(t1, job.Context{
		Inputs: map[string]string{
			"SOMEVAR": "SOME DATA",
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, "SOME DATA", t1.Post[0].Env["HELLO"])
}

func BenchmarkEval(b *testing.B) {
	for i := 0; i < b.N; i++ {
		t1 := &task.Task{
			Env: map[string]string{
				"HELLO": "{{ inputs.SOMEVAR }}",
			},
		}
		err := eval.Evaluate(t1, job.Context{
			Inputs: map[string]string{
				"SOMEVAR": "SOME DATA",
			},
		})
		assert.NoError(b, err)
		assert.Equal(b, "SOME DATA", t1.Env["HELLO"])
	}
}
