package podman

import (
	"context"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/runabol/tork"

	"github.com/runabol/tork/broker"
	"github.com/runabol/tork/internal/uuid"
	"github.com/runabol/tork/runtime"
	"github.com/runabol/tork/runtime/docker"

	"github.com/stretchr/testify/assert"
)

func TestPodmanRunTaskCMD(t *testing.T) {
	rt := NewPodmanRuntime()

	err := rt.Run(context.Background(), &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "busybox:stable",
		CMD:   []string{"ls"},
	})
	assert.NoError(t, err)
}

func TestPodmanRunTaskRun(t *testing.T) {
	rt := NewPodmanRuntime()

	tk := &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "busybox:stable",
		Run:   "echo hello world > $TORK_OUTPUT",
	}
	err := rt.Run(context.Background(), tk)
	assert.NoError(t, err)
	assert.Equal(t, "hello world\n", tk.Result)
}

func TestPodmanCoustomEntrypoint(t *testing.T) {
	rt := NewPodmanRuntime()

	tk := &tork.Task{
		ID:         uuid.NewUUID(),
		Image:      "ubuntu:mantic",
		Run:        "echo hello world > $TORK_OUTPUT",
		Entrypoint: []string{"/bin/bash", "-c"},
	}
	err := rt.Run(context.Background(), tk)
	assert.NoError(t, err)
	assert.Equal(t, "hello world\n", tk.Result)
}

func TestPodmanRunPrePost(t *testing.T) {
	rt := NewPodmanRuntime()

	tk := &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "busybox:stable",
		Run:   "cat /somedir/thing > $TORK_OUTPUT",
		Pre: []*tork.Task{{
			ID:    uuid.NewUUID(),
			Image: "busybox:stable",
			Run:   "echo hello > /somedir/thing",
		}},
		Post: []*tork.Task{{
			ID:    uuid.NewUUID(),
			Image: "busybox:stable",
			Run:   "echo post",
		}},
		Mounts: []tork.Mount{{
			Type:   tork.MountTypeVolume,
			Target: "/somedir",
		}},
	}
	err := rt.Run(context.Background(), tk)
	assert.NoError(t, err)
	assert.Equal(t, "hello\n", tk.Result)
}

func TestPodmanProgress(t *testing.T) {
	rt := NewPodmanRuntime()

	tk := &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "alpine:latest",
		Run: `
		  echo 5 > $TORK_PROGRESS
		  sleep 2
		`,
	}

	ctx := context.Background()

	go func() {
		err := rt.Run(ctx, tk)
		assert.NoError(t, err)
	}()

	time.Sleep(time.Second * 2)
	containerID, ok := rt.tasks.Get(tk.ID)
	assert.True(t, ok)
	assert.NotEmpty(t, containerID)

	workDir := path.Join(os.TempDir(), "tork", tk.ID)
	progressFile := path.Join(workDir, "progress")

	p, err := rt.readProgress(progressFile)
	assert.NoError(t, err)
	assert.Equal(t, float64(5), p)
}

func TestPodmanRunTaskCMDLogger(t *testing.T) {
	b := broker.NewInMemoryBroker()
	processed := make(chan any)
	err := b.SubscribeForTaskLogPart(func(p *tork.TaskLogPart) {
		processed <- 1
	})
	assert.NoError(t, err)
	rt := NewPodmanRuntime(WithBroker(b))
	assert.NoError(t, err)
	assert.NotNil(t, rt)

	err = rt.Run(context.Background(), &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "alpine:latest",
		CMD:   []string{"ls"},
	})
	assert.NoError(t, err)
	<-processed
}

func TestPodmanRunTaskConcurrently(t *testing.T) {
	rt := NewPodmanRuntime()
	wg := sync.WaitGroup{}
	c := 10
	wg.Add(10)
	for i := 0; i < c; i++ {
		go func() {
			defer wg.Done()
			tk := &tork.Task{
				ID:    uuid.NewUUID(),
				Image: "alpine:latest",
				Run:   "echo -n hello > $TORK_OUTPUT",
			}
			err := rt.Run(context.Background(), tk)
			assert.NoError(t, err)
			assert.Equal(t, "hello", tk.Result)
		}()
	}
	wg.Wait()
}

func TestPodmanRunTaskWithTimeout(t *testing.T) {
	rt := NewPodmanRuntime()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := rt.Run(ctx, &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "alpine:latest",
		CMD:   []string{"sleep", "10"},
	})
	assert.Error(t, err)
}

func TestPodmanRunTaskWithError(t *testing.T) {
	rt := NewPodmanRuntime()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := rt.Run(ctx, &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "alpine:latest",
		Run:   "not_a_thing",
	})
	assert.Error(t, err)
}

func TestPodmanRunAndStopTask(t *testing.T) {
	rt := NewPodmanRuntime()
	t1 := &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "alpine:latest",
		CMD:   []string{"sleep", "10"},
	}
	go func() {
		err := rt.Run(context.Background(), t1)
		assert.Error(t, err)
	}()
	// give the task a chance to get started
	time.Sleep(time.Second)
	err := rt.Stop(context.Background(), t1)
	assert.NoError(t, err)
}

func TesPodmantHealthCheck(t *testing.T) {
	rt := NewPodmanRuntime()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	assert.NoError(t, rt.HealthCheck(ctx))
}

func TestPodmanHealthCheckFailed(t *testing.T) {
	rt := NewPodmanRuntime()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.Error(t, rt.HealthCheck(ctx))
}

func TestRunTaskWithNetwork(t *testing.T) {
	rt := NewPodmanRuntime()
	err := rt.Run(context.Background(), &tork.Task{
		ID:       uuid.NewUUID(),
		Image:    "alpine:latest",
		CMD:      []string{"ls"},
		Networks: []string{"default"},
	})
	assert.NoError(t, err)
	rt = NewPodmanRuntime()
	assert.NoError(t, err)
	assert.NotNil(t, rt)
	err = rt.Run(context.Background(), &tork.Task{
		ID:       uuid.NewUUID(),
		Image:    "alpine:latest",
		CMD:      []string{"ls"},
		Networks: []string{"no-such-network"},
	})
	assert.Error(t, err)
}

func TestPodmanRunTaskWithVolume(t *testing.T) {
	rt := NewPodmanRuntime()
	ctx := context.Background()
	t1 := &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "ubuntu:mantic",
		Run:   "echo hello world > /xyz/thing",
		Mounts: []tork.Mount{
			{
				Type:   tork.MountTypeVolume,
				Target: "/xyz",
			},
		},
	}
	err := rt.Run(ctx, t1)
	assert.NoError(t, err)
}

func TestPodmanRunTaskWithVolumeAndCustomWorkdir(t *testing.T) {
	rt := NewPodmanRuntime()

	ctx := context.Background()

	t1 := &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "ubuntu:mantic",
		Run: `echo hello world > /xyz/thing
              ls > $TORK_OUTPUT`,
		Mounts: []tork.Mount{
			{
				Type:   tork.MountTypeVolume,
				Target: "/xyz",
			},
		},
		Workdir: "/xyz",
	}
	err := rt.Run(ctx, t1)
	assert.NoError(t, err)
	assert.Equal(t, "thing\n", t1.Result)
}

func TestPodmanRunTaskWithBind(t *testing.T) {
	mm := runtime.NewMultiMounter()
	vm := NewVolumeMounter()
	mm.RegisterMounter("bind", docker.NewBindMounter(docker.BindConfig{Allowed: true}))
	mm.RegisterMounter("volume", vm)
	rt := NewPodmanRuntime(WithMounter(mm))
	ctx := context.Background()
	dir := path.Join(os.TempDir(), uuid.NewUUID())
	t1 := &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "ubuntu:mantic",
		Run:   "echo hello world > /xyz/thing",
		Mounts: []tork.Mount{{
			Type:   tork.MountTypeBind,
			Target: "/xyz",
			Source: dir,
		}},
	}
	err := rt.Run(ctx, t1)
	assert.NoError(t, err)
}

func TestPodmanRunTaskWithVolumeAndWorkdir(t *testing.T) {
	rt := NewPodmanRuntime()
	ctx := context.Background()
	t1 := &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "ubuntu:mantic",
		Run:   "echo hello world > ./thing",
		Mounts: []tork.Mount{
			{
				Type:   tork.MountTypeVolume,
				Target: "/xyz",
			},
		},
		Workdir: "/xyz",
	}
	err := rt.Run(ctx, t1)
	assert.NoError(t, err)
}

func TestPodmanRunTaskInitWorkdir(t *testing.T) {
	rt := NewPodmanRuntime()
	t1 := &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "ubuntu:mantic",
		Run:   "cat hello.txt > $TORK_OUTPUT",
		Files: map[string]string{
			"hello.txt": "hello world",
			"large.txt": strings.Repeat("a", 100_000),
		},
	}
	ctx := context.Background()
	err := rt.Run(ctx, t1)
	assert.NoError(t, err)
	assert.Equal(t, "hello world", t1.Result)
}

func TestPodmanRunTaskInitWorkdirLs(t *testing.T) {
	rt := NewPodmanRuntime()
	t1 := &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "ubuntu:mantic",
		Run:   "ls > $TORK_OUTPUT",
		Files: map[string]string{
			"hello.txt": "hello world",
			"large.txt": strings.Repeat("a", 100_000),
		},
	}
	ctx := context.Background()
	err := rt.Run(ctx, t1)
	assert.NoError(t, err)
	assert.Equal(t, "hello.txt\nlarge.txt\n", t1.Result)
}

func TestRunTaskWithCustomMounter(t *testing.T) {
	mounter := runtime.NewMultiMounter()
	vmounter := NewVolumeMounter()
	mounter.RegisterMounter(tork.MountTypeVolume, vmounter)
	rt := NewPodmanRuntime(WithMounter(mounter))
	t1 := &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "ubuntu:mantic",
		Run:   "echo hello world > /xyz/thing",
		Mounts: []tork.Mount{
			{
				Type:   tork.MountTypeVolume,
				Target: "/xyz",
			},
		},
	}
	ctx := context.Background()
	err := rt.Run(ctx, t1)
	assert.NoError(t, err)
}

func Test_imagePull(t *testing.T) {
	ctx := context.Background()

	rt := NewPodmanRuntime()

	err := rt.imagePull(ctx, &tork.Task{Image: "localhost:5001/no/suchthing"}, os.Stdout)
	assert.Error(t, err)

	wg := sync.WaitGroup{}
	wg.Add(3)

	for i := 0; i < 3; i++ {
		go func() {
			defer wg.Done()
			err := rt.imagePull(ctx, &tork.Task{Image: "alpine:latest"}, os.Stdout)
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
}

func TestRunTaskWithPrivilegedModeOn(t *testing.T) {
	rt := NewPodmanRuntime(WithPrivileged(true))
	assert.NotNil(t, rt)

	ctx := context.Background()
	t1 := &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "alpine:3.18.3",
		Run:   "RESULT=$(sysctl -w net.ipv4.ip_forward=1 > /dev/null 2>&1 && echo 'Can modify kernel params' || echo 'Cannot modify kernel params'); echo $RESULT > $TORK_OUTPUT",
	}
	err := rt.Run(ctx, t1)
	assert.NoError(t, err)
	assert.Equal(t, "Can modify kernel params\n", t1.Result)
}

func TestRunTaskWithPrivilegedModeOff(t *testing.T) {
	rt := NewPodmanRuntime(WithPrivileged(false))
	assert.NotNil(t, rt)

	ctx := context.Background()
	t1 := &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "alpine:3.18.3",
		Run:   "RESULT=$(sysctl -w net.ipv4.ip_forward=1 > /dev/null 2>&1 && echo 'Can modify kernel params' || echo 'Cannot modify kernel params'); echo $RESULT > $TORK_OUTPUT",
	}
	err := rt.Run(ctx, t1)
	assert.NoError(t, err)
	assert.Equal(t, "Cannot modify kernel params\n", t1.Result)
}
