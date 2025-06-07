package shell

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/runabol/tork"
	"github.com/runabol/tork/broker"
	"github.com/runabol/tork/internal/uuid"
	"github.com/stretchr/testify/assert"
)

func TestShellRuntimeRunResult(t *testing.T) {
	rt := NewShellRuntime(Config{
		UID: DEFAULT_UID,
		GID: DEFAULT_GID,
		Rexec: func(args ...string) *exec.Cmd {
			cmd := exec.Command(args[5], args[6:]...)
			return cmd
		},
	})

	tk := &tork.Task{
		ID:  uuid.NewUUID(),
		Run: "echo -n hello world > $REEXEC_TORK_OUTPUT",
	}

	err := rt.Run(context.Background(), tk)

	assert.NoError(t, err)
	assert.Equal(t, "hello world", tk.Result)
}

func TestShellRuntimeRunPath(t *testing.T) {
	rt := NewShellRuntime(Config{
		UID: DEFAULT_UID,
		GID: DEFAULT_GID,
		Rexec: func(args ...string) *exec.Cmd {
			cmd := exec.Command(args[5], args[6:]...)
			return cmd
		},
	})

	tk := &tork.Task{
		ID:  uuid.NewUUID(),
		Run: "echo -n $PATH > $REEXEC_TORK_OUTPUT",
	}
	err := rt.Run(context.Background(), tk)
	assert.NoError(t, err)
	assert.NotEmpty(t, tk.Result)
}

func TestShellRuntimeRunFile(t *testing.T) {
	rt := NewShellRuntime(Config{
		UID: DEFAULT_UID,
		GID: DEFAULT_GID,
		Rexec: func(args ...string) *exec.Cmd {
			cmd := exec.Command(args[5], args[6:]...)
			return cmd
		},
	})

	tk := &tork.Task{
		ID:  uuid.NewUUID(),
		Run: "cat hello.txt > $REEXEC_TORK_OUTPUT",
		Files: map[string]string{
			"hello.txt": "hello world",
		},
	}

	err := rt.Run(context.Background(), tk)

	assert.NoError(t, err)
	assert.Equal(t, "hello world", tk.Result)
}

func TestShellRuntimeRunNotSupported(t *testing.T) {
	rt := NewShellRuntime(Config{})

	tk := &tork.Task{
		ID:       uuid.NewUUID(),
		Run:      "echo hello world",
		Networks: []string{"some-network"},
	}

	err := rt.Run(context.Background(), tk)

	assert.Error(t, err)
}

func TestShellRuntimeRunError(t *testing.T) {
	rt := NewShellRuntime(Config{
		UID: DEFAULT_UID,
		GID: DEFAULT_GID,
		Rexec: func(args ...string) *exec.Cmd {
			cmd := exec.Command(args[5], args[6:]...)
			return cmd
		},
	})

	tk := &tork.Task{
		ID:  uuid.NewUUID(),
		Run: "no_such_command",
	}

	err := rt.Run(context.Background(), tk)

	assert.Error(t, err)
}

func TestShellRuntimeRunTimeout(t *testing.T) {
	rt := NewShellRuntime(Config{
		UID: DEFAULT_UID,
		GID: DEFAULT_GID,
		Rexec: func(args ...string) *exec.Cmd {
			cmd := exec.Command(args[5], args[6:]...)
			return cmd
		},
	})

	tk := &tork.Task{
		ID:  uuid.NewUUID(),
		Run: "sleep 30",
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()

	err := rt.Run(ctx, tk)

	assert.Error(t, err)
}

func TestRunTaskCMDLogger(t *testing.T) {
	b := broker.NewInMemoryBroker()
	processed := make(chan any)
	err := b.SubscribeForTaskLogPart(func(p *tork.TaskLogPart) {
		close(processed)
	})
	assert.NoError(t, err)
	rt := NewShellRuntime(Config{
		UID: DEFAULT_UID,
		GID: DEFAULT_GID,
		Rexec: func(args ...string) *exec.Cmd {
			cmd := exec.Command(args[5], args[6:]...)
			return cmd
		},
		Broker: b,
	})
	err = rt.Run(context.Background(), &tork.Task{
		ID:  uuid.NewUUID(),
		Run: "echo hello",
	})
	assert.NoError(t, err)
	<-processed
}

func TestBuildEnv(t *testing.T) {
	// Set up environment variables with the REEXEC_ prefix
	assert.NoError(t, os.Setenv("REEXEC_VAR1", "value1"))
	assert.NoError(t, os.Setenv("REEXEC_VAR2", "value2"))
	assert.NoError(t, os.Setenv("NON_REEXEC_VAR", "should_not_be_included"))
	assert.NoError(t, os.Setenv("REEXEC_URL", "{\"POSTGRES_DB\":\"somedb\",\"POSTGRES_URL\":\"postgres://user:password@localhost:5432/todos?sslmode=disable\"}"))

	env, err := buildEnv()

	// Clean up environment variables
	assert.NoError(t, os.Unsetenv("REEXEC_VAR1"))
	assert.NoError(t, os.Unsetenv("REEXEC_VAR2"))
	assert.NoError(t, os.Unsetenv("REEXEC_URL"))
	assert.NoError(t, os.Unsetenv("NON_REEXEC_VAR"))

	assert.NoError(t, err)
	assert.Contains(t, env, "VAR1=value1")
	assert.Contains(t, env, `URL={"POSTGRES_DB":"somedb","POSTGRES_URL":"postgres://user:password@localhost:5432/todos?sslmode=disable"}`)
	assert.Contains(t, env, "VAR2=value2")
	assert.NotContains(t, env, "NON_REEXEC_VAR=should_not_be_included")
}

func TestRunTaskWithPrePost(t *testing.T) {
	rt := NewShellRuntime(Config{
		UID: DEFAULT_UID,
		GID: DEFAULT_GID,
		Rexec: func(args ...string) *exec.Cmd {
			cmd := exec.Command(args[5], args[6:]...)
			return cmd
		},
	})

	t1 := &tork.Task{
		ID:  uuid.NewUUID(),
		Run: "cat /tmp/pre.txt > $REEXEC_TORK_OUTPUT",
		Pre: []*tork.Task{{
			Run: "echo hello pre > /tmp/pre.txt",
		}},
		Post: []*tork.Task{{
			Run: "echo bye bye",
		}},
	}

	err := rt.Run(context.Background(), t1)
	assert.NoError(t, err)
	assert.Equal(t, "hello pre\n", t1.Result)
}

func TestRunTaskWithSidecar(t *testing.T) {
	rt := NewShellRuntime(Config{
		UID: DEFAULT_UID,
		GID: DEFAULT_GID,
		Rexec: func(args ...string) *exec.Cmd {
			cmd := exec.Command(args[5], args[6:]...)
			return cmd
		},
	})

	t1 := &tork.Task{
		ID:  uuid.NewUUID(),
		Run: "sleep 1.5; cat /tmp/sidecar > $REEXEC_TORK_OUTPUT",
		Sidecars: []*tork.Task{{
			Run: "echo hello sidecar > /tmp/sidecar",
		}},
	}

	err := rt.Run(context.Background(), t1)
	assert.NoError(t, err)
	assert.Equal(t, "hello sidecar\n", t1.Result)
}
