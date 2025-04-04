package shell

import (
	"context"
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
	assert.NotEmpty(t, "hello world", tk.Result)
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
