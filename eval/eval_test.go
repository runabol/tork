package eval_test

import (
	"strconv"
	"testing"

	"github.com/runabol/tork"
	"github.com/runabol/tork/eval"
	"github.com/stretchr/testify/assert"
)

func TestEvalNoop(t *testing.T) {
	t1 := &tork.Task{}
	err := eval.EvaluateTask(t1, map[string]any{})
	assert.NoError(t, err)
	assert.Empty(t, t1.Env)
}

func TestEvalLiteral(t *testing.T) {
	t1 := &tork.Task{
		Env: map[string]string{
			"HELLO": "WORLD",
		},
	}
	err := eval.EvaluateTask(t1, map[string]any{})
	assert.NoError(t, err)
	assert.Equal(t, "WORLD", t1.Env["HELLO"])
}

func TestEvalVar(t *testing.T) {
	t1 := &tork.Task{
		Env: map[string]string{
			"HELLO": `{{inputs.SOMEVAR}}`,
		},
	}
	err := eval.EvaluateTask(t1, map[string]any{
		"inputs": map[string]string{
			"SOMEVAR": "SOME DATA",
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, "SOME DATA", t1.Env["HELLO"])
}

func TestEvalName(t *testing.T) {
	t1 := &tork.Task{
		Name: "{{ inputs.SOMENAME }}y",
	}
	err := eval.EvaluateTask(t1, map[string]any{
		"inputs": map[string]string{
			"SOMENAME": "John Smith",
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, "John Smithy", t1.Name)
}

func TestEvalImage(t *testing.T) {
	t1 := &tork.Task{
		Image: "ubuntu:{{ inputs.TAG }}",
	}
	err := eval.EvaluateTask(t1, map[string]any{
		"inputs": map[string]string{
			"TAG": "maverick",
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, "ubuntu:maverick", t1.Image)
}

func TestEvalQueue(t *testing.T) {
	t1 := &tork.Task{
		Queue: "{{ inputs.QUEUE }}",
	}
	err := eval.EvaluateTask(t1, map[string]any{
		"inputs": map[string]string{
			"QUEUE": "default",
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, "default", t1.Queue)
}

func TestEvalFunc(t *testing.T) {
	t1 := &tork.Task{
		Env: map[string]string{
			"RAND_NUM": "{{ randomInt() }}",
		},
	}
	err := eval.EvaluateTask(t1, map[string]any{})
	assert.NoError(t, err)
	assert.NotEmpty(t, t1.Env["RAND_NUM"])
	intVar, err := strconv.Atoi(t1.Env["RAND_NUM"])
	assert.NoError(t, err)
	assert.Greater(t, intVar, 0)
}

func TestEvalIf(t *testing.T) {
	t1 := &tork.Task{
		If: `{{ 1 == 1 }}`,
	}
	err := eval.EvaluateTask(t1, map[string]any{})
	assert.NoError(t, err)
	assert.Equal(t, "true", t1.If)

	t1 = &tork.Task{
		If: `{{ 1 == 2 }}`,
	}
	err = eval.EvaluateTask(t1, map[string]any{})
	assert.NoError(t, err)
	assert.Equal(t, "false", t1.If)

	t1 = &tork.Task{
		If: `{{ !(1 == 2) }}`,
	}
	err = eval.EvaluateTask(t1, map[string]any{})
	assert.NoError(t, err)
	assert.Equal(t, "true", t1.If)
}

func TestDontEvalRun(t *testing.T) {
	t1 := &tork.Task{
		Run: "Hello {{ inputs.NAME }}",
	}
	err := eval.EvaluateTask(t1, map[string]any{
		"inputs": map[string]string{
			"NAME": "world",
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, "Hello {{ inputs.NAME }}", t1.Run)
}

func TestEvalPre(t *testing.T) {
	t1 := &tork.Task{
		Pre: []*tork.Task{
			{
				Env: map[string]string{
					"HELLO": "{{ inputs.SOMEVAR }}",
				},
			},
		},
	}
	err := eval.EvaluateTask(t1, map[string]any{
		"inputs": map[string]string{
			"SOMEVAR": "SOME DATA",
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, "SOME DATA", t1.Pre[0].Env["HELLO"])
}

func TestEvalPost(t *testing.T) {
	t1 := &tork.Task{
		Post: []*tork.Task{
			{
				Env: map[string]string{
					"HELLO": "{{ inputs.SOMEVAR }}",
				},
			},
		},
	}
	err := eval.EvaluateTask(t1, map[string]any{
		"inputs": map[string]string{
			"SOMEVAR": "SOME DATA",
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, "SOME DATA", t1.Post[0].Env["HELLO"])
}

func TestEvalParallel(t *testing.T) {
	t1 := &tork.Task{
		Parallel: &tork.ParallelTask{
			Tasks: []*tork.Task{
				{
					Env: map[string]string{
						"HELLO": "{{ inputs.SOMEVAR }}",
					},
				},
			},
		},
	}
	err := eval.EvaluateTask(t1, map[string]any{
		"inputs": map[string]string{
			"SOMEVAR": "SOME DATA",
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, "SOME DATA", t1.Parallel.Tasks[0].Env["HELLO"])
}

func TestEvalExpr(t *testing.T) {
	v, err := eval.EvaluateExpr("1+1", map[string]any{})
	assert.NoError(t, err)
	assert.Equal(t, 2, v)

	v, err = eval.EvaluateExpr("{{1+1}}", map[string]any{})
	assert.NoError(t, err)
	assert.Equal(t, 2, v)

	v, err = eval.EvaluateExpr("{{ [1,2,3] }}", map[string]any{})
	assert.NoError(t, err)
	assert.Equal(t, []any{1, 2, 3}, v)

	v, err = eval.EvaluateExpr("{{ fromJSON( inputs.json ) }}", map[string]any{"inputs": map[string]string{
		"json": "[1,2,3]",
	}})
	assert.NoError(t, err)
	assert.Equal(t, []any{float64(1), float64(2), float64(3)}, v)

	v, err = eval.EvaluateExpr("{{ fromJSON( inputs.json ) }}", map[string]any{"inputs": map[string]string{
		"json": `{"hello":"world"}`,
	}})
	assert.NoError(t, err)
	assert.Equal(t, map[string]any(map[string]any{"hello": "world"}), v)
}

func TestValidExpr(t *testing.T) {
	assert.True(t, eval.ValidExpr("{{1+1}}"))
	assert.False(t, eval.ValidExpr("{1+1}}"))
	assert.True(t, eval.ValidExpr("1+1"))
	assert.False(t, eval.ValidExpr(""))
}

func BenchmarkEval(b *testing.B) {
	for i := 0; i < b.N; i++ {
		t1 := &tork.Task{
			Env: map[string]string{
				"HELLO": "{{ inputs.SOMEVAR }}",
			},
		}
		err := eval.EvaluateTask(t1, map[string]any{
			"inputs": map[string]string{
				"SOMEVAR": "SOME DATA",
			},
		})
		assert.NoError(b, err)
		assert.Equal(b, "SOME DATA", t1.Env["HELLO"])
	}
}
