package docker

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"

	"github.com/runabol/tork"

	"github.com/runabol/tork/internal/uuid"
	"github.com/runabol/tork/runtime"

	"github.com/stretchr/testify/assert"
)

func TestParseCPUs(t *testing.T) {
	parsed, err := parseCPUs(&tork.TaskLimits{CPUs: ".25"})
	assert.NoError(t, err)
	assert.Equal(t, int64(250000000), parsed)

	parsed, err = parseCPUs(&tork.TaskLimits{CPUs: "1"})
	assert.NoError(t, err)
	assert.Equal(t, int64(1000000000), parsed)

	parsed, err = parseCPUs(&tork.TaskLimits{CPUs: "0.5"})
	assert.NoError(t, err)
	assert.Equal(t, int64(500000000), parsed)
}

func TestParseMemory(t *testing.T) {
	parsed, err := parseMemory(&tork.TaskLimits{Memory: "1MB"})
	assert.NoError(t, err)
	assert.Equal(t, int64(1048576), parsed)

	parsed, err = parseMemory(&tork.TaskLimits{Memory: "10MB"})
	assert.NoError(t, err)
	assert.Equal(t, int64(10485760), parsed)

	parsed, err = parseMemory(&tork.TaskLimits{Memory: "500KB"})
	assert.NoError(t, err)
	assert.Equal(t, int64(512000), parsed)

	parsed, err = parseMemory(&tork.TaskLimits{Memory: "1B"})
	assert.NoError(t, err)
	assert.Equal(t, int64(1), parsed)
}

func TestNewDockerRuntime(t *testing.T) {
	rt, err := NewDockerRuntime()
	assert.NoError(t, err)
	assert.NotNil(t, rt)
}

func TestRunTaskCMD(t *testing.T) {
	rt, err := NewDockerRuntime()
	assert.NoError(t, err)
	assert.NotNil(t, rt)

	err = rt.Run(context.Background(), &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "ubuntu:mantic",
		CMD:   []string{"ls"},
	})
	assert.NoError(t, err)
}

func TestRunTaskWithTimeout(t *testing.T) {
	rt, err := NewDockerRuntime()
	assert.NoError(t, err)
	assert.NotNil(t, rt)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = rt.Run(ctx, &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "ubuntu:mantic",
		CMD:   []string{"sleep", "10"},
	})
	assert.Error(t, err)
}

func TestRunAndStopTask(t *testing.T) {
	rt, err := NewDockerRuntime()
	assert.NoError(t, err)
	assert.NotNil(t, rt)
	t1 := &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "ubuntu:mantic",
		CMD:   []string{"sleep", "10"},
	}
	go func() {
		err := rt.Run(context.Background(), t1)
		assert.Error(t, err)
	}()
	// give the task a chance to get started
	time.Sleep(time.Second)
	err = rt.Stop(context.Background(), t1)
	assert.NoError(t, err)
}

func TestHealthCheck(t *testing.T) {
	rt, err := NewDockerRuntime()
	assert.NoError(t, err)
	assert.NotNil(t, rt)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	assert.NoError(t, rt.HealthCheck(ctx))
}

func TestHealthCheckFailed(t *testing.T) {
	rt, err := NewDockerRuntime()
	assert.NoError(t, err)
	assert.NotNil(t, rt)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.Error(t, rt.HealthCheck(ctx))
}

func TestRunTaskWithNetwork(t *testing.T) {
	rt, err := NewDockerRuntime()
	assert.NoError(t, err)
	assert.NotNil(t, rt)
	err = rt.Run(context.Background(), &tork.Task{
		ID:       uuid.NewUUID(),
		Image:    "ubuntu:mantic",
		CMD:      []string{"ls"},
		Networks: []string{"default"},
	})
	assert.NoError(t, err)
	rt, err = NewDockerRuntime()
	assert.NoError(t, err)
	assert.NotNil(t, rt)
	err = rt.Run(context.Background(), &tork.Task{
		ID:       uuid.NewUUID(),
		Image:    "ubuntu:mantic",
		CMD:      []string{"ls"},
		Networks: []string{"no-such-network"},
	})
	assert.Error(t, err)
}

func TestRunTaskWithVolume(t *testing.T) {
	rt, err := NewDockerRuntime()
	assert.NoError(t, err)

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
	err = rt.Run(ctx, t1)
	assert.NoError(t, err)
}

func TestRunTaskWithCustomMounter(t *testing.T) {
	mounter := runtime.NewMultiMounter()
	vmounter, err := NewVolumeMounter()
	assert.NoError(t, err)
	mounter.RegisterMounter(tork.MountTypeVolume, vmounter)
	rt, err := NewDockerRuntime(WithMounter(mounter))
	assert.NoError(t, err)
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
	err = rt.Run(ctx, t1)
	assert.NoError(t, err)
}

func TestRunTaskInitWorkdir(t *testing.T) {
	rt, err := NewDockerRuntime()
	assert.NoError(t, err)
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
	err = rt.Run(ctx, t1)
	assert.NoError(t, err)
	assert.Equal(t, "hello world", t1.Result)
}

func Test_imagePull(t *testing.T) {
	ctx := context.Background()

	rt, err := NewDockerRuntime()
	assert.NoError(t, err)
	assert.NotNil(t, rt)

	images, err := rt.client.ImageList(ctx, types.ImageListOptions{
		Filters: filters.NewArgs(filters.Arg("reference", "alpine:*")),
	})
	assert.NoError(t, err)

	for _, img := range images {
		_, err = rt.client.ImageRemove(ctx, img.ID, types.ImageRemoveOptions{Force: true})
		assert.NoError(t, err)
	}

	err = rt.imagePull(ctx, &tork.Task{Image: "localhost:5000/no/suchthing"})
	assert.Error(t, err)

	wg := sync.WaitGroup{}
	wg.Add(3)

	for i := 0; i < 3; i++ {
		go func() {
			defer wg.Done()
			err := rt.imagePull(ctx, &tork.Task{Image: "alpine:3.18.3"})
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
}

func Test_imagePullPrivateRegistry(t *testing.T) {
	ctx := context.Background()

	rt, err := NewDockerRuntime()
	assert.NoError(t, err)
	assert.NotNil(t, rt)

	r1, err := rt.client.ImagePull(ctx, "alpine:3.18.3", types.ImagePullOptions{})
	assert.NoError(t, err)
	assert.NoError(t, r1.Close())

	images, err := rt.client.ImageList(ctx, types.ImageListOptions{
		Filters: filters.NewArgs(filters.Arg("reference", "alpine:3.18.3")),
	})
	assert.NoError(t, err)
	assert.Len(t, images, 1)

	err = rt.client.ImageTag(ctx, "alpine:3.18.3", "localhost:5000/tork/alpine:3.18.3")
	assert.NoError(t, err)

	r2, err := rt.client.ImagePush(ctx, "localhost:5000/tork/alpine:3.18.3", types.ImagePushOptions{RegistryAuth: "noauth"})
	assert.NoError(t, err)
	assert.NoError(t, r2.Close())

	err = rt.imagePull(ctx, &tork.Task{
		Image: "localhost:5000/tork/alpine:3.18.3",
		Registry: &tork.Registry{
			Username: "username",
			Password: "password",
		},
	})

	assert.NoError(t, err)
}
