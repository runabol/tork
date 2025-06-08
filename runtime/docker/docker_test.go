package docker

import (
	"bytes"
	"context"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/rs/zerolog/log"

	"github.com/runabol/tork"

	"github.com/runabol/tork/broker"
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

func TestDockerLogsReader(t *testing.T) {
	bs := []byte{}
	for i := 0; i < 100; i++ {
		bs = append(bs, []byte{1, 0, 0, 0, 0, 0, 0, 5, 104, 101, 108, 108, 111}...)
	}
	s := ""
	for i := 0; i < 100; i++ {
		s = s + "hello"
	}
	pr := dockerLogsReader{reader: bytes.NewReader(bs)}
	b, err := io.ReadAll(pr)
	assert.NoError(t, err)
	assert.Equal(t, s, string(b))
}

func TestDockerLogsReaderNewLine(t *testing.T) {
	s := []byte{}
	for i := 0; i < 1000; i++ {
		s = append(s, 0)
	}
	s = append(s, []byte{1, 0, 0, 0, 0, 0, 0, 12, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100, 10}...)
	pr := dockerLogsReader{reader: bytes.NewReader(s)}
	b, err := io.ReadAll(pr)
	assert.NoError(t, err)
	assert.Equal(t, "hello world\n", string(b))
}

type eofReader struct {
	hdr     []byte
	data    []byte
	hdrRead bool
}

func (r *eofReader) Read(p []byte) (int, error) {
	if !r.hdrRead {
		r.hdrRead = true
		copy(p, r.hdr)
		return len(r.hdr), nil
	}
	copy(p, r.data)
	return len(r.data), io.EOF
}

func TestDockerLogsReaderEOF(t *testing.T) {
	pr := dockerLogsReader{reader: &eofReader{
		hdr:  []byte{1, 0, 0, 0, 0, 0, 0, 11},
		data: []byte{104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100},
	}}
	b, err := io.ReadAll(pr)
	assert.NoError(t, err)
	assert.Equal(t, "hello world", string(b))
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
		Image: "busybox:stable",
		CMD:   []string{"ls"},
	})
	assert.NoError(t, err)
}

func TestProgress(t *testing.T) {
	rt, err := NewDockerRuntime()
	assert.NoError(t, err)
	assert.NotNil(t, rt)

	tk := &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "busybox:stable",
		Run:   "sleep 1",
	}

	ctx := context.Background()

	go func() {
		err := rt.Run(ctx, tk)
		assert.NoError(t, err)
	}()

	time.Sleep(time.Millisecond * 500)
	containerID, ok := rt.tasks.Get(tk.ID)
	assert.True(t, ok)
	assert.NotEmpty(t, containerID)

	p, err := rt.readProgress(ctx, containerID)
	assert.NoError(t, err)
	assert.Equal(t, float64(0), p)
}

func TestRunTaskCMDLogger(t *testing.T) {
	b := broker.NewInMemoryBroker()
	processed := make(chan any)
	err := b.SubscribeForTaskLogPart(func(p *tork.TaskLogPart) {
		processed <- 1
	})
	assert.NoError(t, err)
	rt, err := NewDockerRuntime(WithBroker(b))
	assert.NoError(t, err)
	assert.NotNil(t, rt)

	err = rt.Run(context.Background(), &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "busybox:stable",
		CMD:   []string{"ls"},
	})
	assert.NoError(t, err)
	<-processed
}

func TestRunTaskConcurrently(t *testing.T) {
	rt, err := NewDockerRuntime()
	assert.NoError(t, err)
	assert.NotNil(t, rt)
	wg := sync.WaitGroup{}
	c := 10
	wg.Add(10)
	for i := 0; i < c; i++ {
		go func() {
			defer wg.Done()
			tk := &tork.Task{
				ID:    uuid.NewUUID(),
				Image: "busybox:stable",
				Run:   "echo -n hello > $TORK_OUTPUT",
			}
			err := rt.Run(context.Background(), tk)
			assert.NoError(t, err)
			assert.Equal(t, "hello", tk.Result)
		}()
	}
	wg.Wait()
}

func TestRunTaskWithTimeout(t *testing.T) {
	rt, err := NewDockerRuntime()
	assert.NoError(t, err)
	assert.NotNil(t, rt)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = rt.Run(ctx, &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "busybox:stable",
		CMD:   []string{"sleep", "10"},
	})
	assert.Error(t, err)
}

func TestRunTaskWithError(t *testing.T) {
	rt, err := NewDockerRuntime()
	assert.NoError(t, err)
	assert.NotNil(t, rt)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = rt.Run(ctx, &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "busybox:stable",
		Run:   "not_a_thing",
	})
	assert.Error(t, err)
}

func TestRunAndCancelTask(t *testing.T) {
	rt, err := NewDockerRuntime()
	assert.NoError(t, err)
	assert.NotNil(t, rt)
	t1 := &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "busybox:stable",
		CMD:   []string{"sleep", "60"},
	}
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan error)
	go func() {
		err := rt.Run(ctx, t1)
		assert.Error(t, err)
		ch <- err
	}()
	// give the task a chance to get started
	time.Sleep(time.Second * 3)
	cancel()
	assert.Error(t, <-ch)
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

	ctx := context.Background()

	nw := uuid.NewUUID()
	networkCreateResp, err := rt.client.NetworkCreate(ctx, nw, types.NetworkCreate{
		CheckDuplicate: true,
		Driver:         "bridge",
	})
	assert.NoError(t, err)
	log.Debug().Msgf("Created network %s with ID %s", nw, networkCreateResp.ID)
	defer func() {
		if err := rt.client.NetworkRemove(context.Background(), nw); err != nil {
			log.Error().Err(err).Msgf("error removing network %s", nw)
		}
	}()

	err = rt.Run(context.Background(), &tork.Task{
		ID:       uuid.NewUUID(),
		Image:    "busybox:stable",
		CMD:      []string{"ls"},
		Networks: []string{nw},
	})
	assert.NoError(t, err)
	rt, err = NewDockerRuntime()
	assert.NoError(t, err)
	assert.NotNil(t, rt)
	err = rt.Run(context.Background(), &tork.Task{
		ID:       uuid.NewUUID(),
		Image:    "busybox:stable",
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
		Image: "busybox:stable",
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

func TestRunTaskWithVolumeAndCustomWorkdir(t *testing.T) {
	rt, err := NewDockerRuntime()
	assert.NoError(t, err)

	ctx := context.Background()

	t1 := &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "busybox:stable",
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
	err = rt.Run(ctx, t1)
	assert.NoError(t, err)
	assert.Equal(t, "thing\n", t1.Result)
}

func TestRunTaskWithBind(t *testing.T) {
	mm := runtime.NewMultiMounter()
	vm, err := NewVolumeMounter()
	assert.NoError(t, err)
	mm.RegisterMounter("bind", NewBindMounter(BindConfig{Allowed: true}))
	mm.RegisterMounter("volume", vm)
	rt, err := NewDockerRuntime(WithMounter(mm))
	assert.NoError(t, err)
	ctx := context.Background()
	dir := path.Join(os.TempDir(), uuid.NewUUID())
	t1 := &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "busybox:stable",
		Run:   "echo hello world > /xyz/thing",
		Mounts: []tork.Mount{{
			Type:   tork.MountTypeBind,
			Target: "/xyz",
			Source: dir,
		}},
	}
	err = rt.Run(ctx, t1)
	assert.NoError(t, err)
}

func TestRunTaskWithTempfs(t *testing.T) {
	rt, err := NewDockerRuntime(
		WithMounter(NewTmpfsMounter()),
	)
	assert.NoError(t, err)

	ctx := context.Background()

	t1 := &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "busybox:stable",
		Run:   "echo hello world > /xyz/thing",
		Mounts: []tork.Mount{
			{
				Type:   tork.MountTypeTmpfs,
				Target: "/xyz",
			},
		},
	}
	err = rt.Run(ctx, t1)
	assert.NoError(t, err)
}

func TestRunTaskWithVolumeAndWorkdir(t *testing.T) {
	rt, err := NewDockerRuntime()
	assert.NoError(t, err)

	ctx := context.Background()

	t1 := &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "busybox:stable",
		Run:   "echo hello world > ./thing",
		Mounts: []tork.Mount{
			{
				Type:   tork.MountTypeVolume,
				Target: "/xyz",
			},
		},
		Workdir: "/xyz",
	}
	err = rt.Run(ctx, t1)
	assert.NoError(t, err)
}

func TestRunTaskWithTempfsAndWorkdir(t *testing.T) {
	rt, err := NewDockerRuntime(
		WithMounter(NewTmpfsMounter()),
	)
	assert.NoError(t, err)

	ctx := context.Background()

	t1 := &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "busybox:stable",
		Run:   "echo hello world > ./thing",
		Mounts: []tork.Mount{
			{
				Type:   tork.MountTypeTmpfs,
				Target: "/xyz",
			},
		},
		Workdir: "/xyz",
	}
	err = rt.Run(ctx, t1)
	assert.NoError(t, err)
}

func TestRunTaskInitWorkdir(t *testing.T) {
	rt, err := NewDockerRuntime()
	assert.NoError(t, err)
	t1 := &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "busybox:stable",
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

func TestRunTaskInitWorkdirLs(t *testing.T) {
	rt, err := NewDockerRuntime()
	assert.NoError(t, err)
	t1 := &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "busybox:stable",
		Run:   "ls > $TORK_OUTPUT",
		Files: map[string]string{
			"hello.txt": "hello world",
			"large.txt": strings.Repeat("a", 100_000),
		},
	}
	ctx := context.Background()
	err = rt.Run(ctx, t1)
	assert.NoError(t, err)
	assert.Equal(t, "hello.txt\nlarge.txt\n", t1.Result)
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
		Image: "busybox:stable",
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

func Test_imagePull(t *testing.T) {
	ctx := context.Background()

	rt, err := NewDockerRuntime()
	assert.NoError(t, err)
	assert.NotNil(t, rt)

	images, err := rt.client.ImageList(ctx, image.ListOptions{
		Filters: filters.NewArgs(filters.Arg("reference", "busybox:*")),
	})
	assert.NoError(t, err)

	for _, img := range images {
		_, err = rt.client.ImageRemove(ctx, img.ID, image.RemoveOptions{Force: true})
		assert.NoError(t, err)
	}

	err = rt.imagePull(ctx, &tork.Task{Image: "localhost:5001/no/suchthing"}, os.Stdout)
	assert.Error(t, err)

	wg := sync.WaitGroup{}
	wg.Add(3)

	for i := 0; i < 3; i++ {
		go func() {
			defer wg.Done()
			err := rt.imagePull(ctx, &tork.Task{Image: "busybox:stable"}, os.Stdout)
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

	r1, err := rt.client.ImagePull(ctx, "busybox:stable", image.PullOptions{})
	assert.NoError(t, err)
	assert.NoError(t, r1.Close())

	images, err := rt.client.ImageList(ctx, image.ListOptions{
		Filters: filters.NewArgs(filters.Arg("reference", "busybox:stable")),
	})
	assert.NoError(t, err)
	assert.Len(t, images, 1)

	err = rt.client.ImageTag(ctx, "busybox:stable", "localhost:5001/tork/busybox:stable")
	assert.NoError(t, err)

	r2, err := rt.client.ImagePush(ctx, "localhost:5001/tork/busybox:stable", image.PushOptions{RegistryAuth: "noauth"})
	assert.NoError(t, err)
	assert.NoError(t, r2.Close())

	err = rt.imagePull(ctx, &tork.Task{
		Image: "localhost:5001/tork/busybox:stable",
		Registry: &tork.Registry{
			Username: "username",
			Password: "password",
		},
	}, os.Stdout)

	assert.NoError(t, err)
}

func TestRunTaskWithPrivilegedModeOn(t *testing.T) {
	rt, err := NewDockerRuntime(WithPrivileged(true))
	assert.NoError(t, err)
	assert.NotNil(t, rt)

	ctx := context.Background()
	t1 := &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "busybox:stable",
		Run:   "RESULT=$(sysctl -w net.ipv4.ip_forward=1 > /dev/null 2>&1 && echo 'Can modify kernel params' || echo 'Cannot modify kernel params'); echo $RESULT > $TORK_OUTPUT",
	}
	err = rt.Run(ctx, t1)
	assert.NoError(t, err)
	assert.Equal(t, "Can modify kernel params\n", t1.Result)
}

func Test_doPullRequest(t *testing.T) {
	ctx := context.Background()
	rt, err := NewDockerRuntime(WithImageTTL(time.Second))
	assert.NoError(t, err)
	assert.NotNil(t, rt)
	err = rt.doPullRequest(&pullRequest{
		ctx:    ctx,
		image:  "alpine:3.18.3",
		logger: os.Stdout,
	})
	assert.NoError(t, err)
	// check that the image is cached
	images, err := rt.client.ImageList(ctx, image.ListOptions{
		Filters: filters.NewArgs(filters.Arg("reference", "alpine:3.18.3")),
	})
	assert.NoError(t, err)
	assert.Len(t, images, 1)

	// get the image again (should be cached)
	err = rt.doPullRequest(&pullRequest{
		ctx:    ctx,
		image:  "alpine:3.18.3",
		logger: os.Stdout,
	})
	assert.NoError(t, err)

	// check that the image is still cached
	images, err = rt.client.ImageList(ctx, image.ListOptions{
		Filters: filters.NewArgs(filters.Arg("reference", "alpine:3.18.3")),
	})
	assert.NoError(t, err)
	assert.Len(t, images, 1)

	err = rt.Run(context.Background(), &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "alpine:3.18.3",
		Run:   "echo hello world > $TORK_OUTPUT",
	})
	assert.NoError(t, err)

	time.Sleep(time.Millisecond * 1100)
	err = rt.doPullRequest(&pullRequest{
		ctx:    ctx,
		image:  "busybox:stable",
		logger: os.Stdout,
	})
	assert.NoError(t, err)
	// check that the image is not longer cached
	images, err = rt.client.ImageList(ctx, image.ListOptions{
		Filters: filters.NewArgs(filters.Arg("reference", "alpine:3.18.3")),
	})
	assert.NoError(t, err)
	assert.Len(t, images, 0)
}

func TestRunTaskWithPrePost(t *testing.T) {
	rt, err := NewDockerRuntime()
	assert.NoError(t, err)
	assert.NotNil(t, rt)

	t1 := &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "busybox:stable",
		Run:   "cat /mnt/pre.txt > $TORK_OUTPUT",
		Pre: []*tork.Task{{
			Image: "busybox:stable",
			Run:   "echo hello pre > /mnt/pre.txt",
		}},
		Post: []*tork.Task{{
			Image: "busybox:stable",
			Run:   "echo bye bye",
		}},
		Mounts: []tork.Mount{
			{
				Type:   tork.MountTypeVolume,
				Target: "/mnt",
			},
		},
	}

	err = rt.Run(context.Background(), t1)
	assert.NoError(t, err)
	assert.Equal(t, "hello pre\n", t1.Result)
}

func TestRunTaskWithSidecar(t *testing.T) {
	rt, err := NewDockerRuntime()
	assert.NoError(t, err)
	assert.NotNil(t, rt)

	t1 := &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "alpine:3.18.3",
		Run:   "echo hello > $TORK_OUTPUT",
		Sidecars: []*tork.Task{{
			Image: "busybox:stable",
			Run:   "echo hello sidecar",
		}},
	}

	err = rt.Run(context.Background(), t1)
	assert.NoError(t, err)
	assert.Equal(t, "hello\n", t1.Result)
}

func TestRunTaskWithSidecarAndMount(t *testing.T) {
	rt, err := NewDockerRuntime()
	assert.NoError(t, err)
	assert.NotNil(t, rt)

	t1 := &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "busybox:stable",
		Run:   "sleep 1; cat /mnt/sidecar > $TORK_OUTPUT",
		Sidecars: []*tork.Task{{
			Image: "busybox:stable",
			Run:   "echo hello sidecar > /mnt/sidecar",
		}},
		Mounts: []tork.Mount{
			{
				Type:   tork.MountTypeVolume,
				Target: "/mnt",
			},
		},
	}

	err = rt.Run(context.Background(), t1)
	assert.NoError(t, err)
	assert.Equal(t, "hello sidecar\n", t1.Result)
}

func TestRunTaskWithHTTPServerSidecar(t *testing.T) {
	rt, err := NewDockerRuntime()
	assert.NoError(t, err)
	assert.NotNil(t, rt)

	t1 := &tork.Task{
		ID:    uuid.NewUUID(),
		Name:  "Some task",
		Image: "curlimages/curl:latest",
		Run:   "curl --retry 3 --retry-max-time 5 http://myserver:9000/ > $TORK_OUTPUT",
		Sidecars: []*tork.Task{{
			Name:  "myserver",
			Image: "python:3.11-alpine",
			Run:   "python server.py",
			Files: map[string]string{
				"server.py": `
from http.server import BaseHTTPRequestHandler, HTTPServer
class Handler(BaseHTTPRequestHandler):
	def do_GET(self):
	  self.send_response(200); self.send_header('Content-type', 'text/plain'); self.end_headers()
	  self.wfile.write(f'Hello from sidecar'.encode())
HTTPServer(('0.0.0.0', 9000), Handler).serve_forever()`,
			},
		}},
	}

	err = rt.Run(context.Background(), t1)
	assert.NoError(t, err)
	assert.Equal(t, "Hello from sidecar", t1.Result)
}
