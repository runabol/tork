package eval_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tork/eval"
	"github.com/tork/task"
)

func TestEvalNoop(t *testing.T) {
	t1 := &task.Task{}
	err := eval.Evaluate(t1, make(map[string]any))
	assert.NoError(t, err)
	assert.Empty(t, t1.Env)
}

func TestEvalLiteral(t *testing.T) {
	t1 := &task.Task{
		Env: map[string]string{
			"HELLO": "WORLD",
		},
	}
	err := eval.Evaluate(t1, make(map[string]any))
	assert.NoError(t, err)
	assert.Equal(t, "WORLD", t1.Env["HELLO"])
}

func TestEvalVar(t *testing.T) {
	t1 := &task.Task{
		Env: map[string]string{
			"HELLO": "{{ .SOMEVAR }}",
		},
	}
	err := eval.Evaluate(t1, map[string]any{
		"SOMEVAR": "SOME DATA",
	})
	assert.NoError(t, err)
	assert.Equal(t, "SOME DATA", t1.Env["HELLO"])
}

func TestEvalName(t *testing.T) {
	t1 := &task.Task{
		Name: "{{ .SOMENAME }}",
	}
	err := eval.Evaluate(t1, map[string]any{
		"SOMENAME": "John Smith",
	})
	assert.NoError(t, err)
	assert.Equal(t, "John Smith", t1.Name)
}

func TestEvalImage(t *testing.T) {
	t1 := &task.Task{
		Image: "ubuntu:{{ .TAG }}",
	}
	err := eval.Evaluate(t1, map[string]any{
		"TAG": "maverick",
	})
	assert.NoError(t, err)
	assert.Equal(t, "ubuntu:maverick", t1.Image)
}

func TestEvalQueue(t *testing.T) {
	t1 := &task.Task{
		Queue: "{{ .QUEUE }}",
	}
	err := eval.Evaluate(t1, map[string]any{
		"QUEUE": "default",
	})
	assert.NoError(t, err)
	assert.Equal(t, "default", t1.Queue)
}

func TestDontEvalRun(t *testing.T) {
	t1 := &task.Task{
		Run: "Hello {{ .NAME }}",
	}
	err := eval.Evaluate(t1, map[string]any{
		"NAME": "world",
	})
	assert.NoError(t, err)
	assert.Equal(t, "Hello {{ .NAME }}", t1.Run)
}

func TestEvalVarInMap(t *testing.T) {
	t1 := &task.Task{
		Env: map[string]string{
			"HELLO": "{{ .SOME.VAR }}",
		},
	}
	err := eval.Evaluate(t1, map[string]any{
		"SOME": map[string]string{"VAR": "SOME DATA"},
	})
	assert.NoError(t, err)
	assert.Equal(t, "SOME DATA", t1.Env["HELLO"])
}

func TestEvalPre(t *testing.T) {
	t1 := &task.Task{
		Pre: []task.Task{
			{
				Env: map[string]string{
					"HELLO": "{{ .SOMEVAR }}",
				},
			},
		},
	}
	err := eval.Evaluate(t1, map[string]any{
		"SOMEVAR": "SOME DATA",
	})
	assert.NoError(t, err)
	assert.Equal(t, "SOME DATA", t1.Pre[0].Env["HELLO"])
}

func TestEvalPost(t *testing.T) {
	t1 := &task.Task{
		Post: []task.Task{
			{
				Env: map[string]string{
					"HELLO": "{{ .SOMEVAR }}",
				},
			},
		},
	}
	err := eval.Evaluate(t1, map[string]any{
		"SOMEVAR": "SOME DATA",
	})
	assert.NoError(t, err)
	assert.Equal(t, "SOME DATA", t1.Post[0].Env["HELLO"])
}

func BenchmarkEval(b *testing.B) {
	for i := 0; i < b.N; i++ {
		t1 := &task.Task{
			Env: map[string]string{
				"HELLO": "{{ .SOMEVAR }}",
			},
		}
		err := eval.Evaluate(t1, map[string]any{
			"SOMEVAR": "SOME DATA",
		})
		assert.NoError(b, err)
		assert.Equal(b, "SOME DATA", t1.Env["HELLO"])
	}
}
