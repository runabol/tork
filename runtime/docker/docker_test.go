package docker

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/errdefs"

	"github.com/runabol/tork"

	mobyarchive "github.com/moby/moby/pkg/archive"

	"github.com/runabol/tork/internal/uuid"
	"github.com/runabol/tork/mq"
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
		Image: "ubuntu:mantic",
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
		Image: "ubuntu:mantic",
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

	// wait for the task to complete
	time.Sleep(time.Second * 1)

	p, err = rt.readProgress(ctx, containerID)
	var notFoundError errdefs.ErrNotFound
	assert.ErrorAs(t, err, &notFoundError)
	assert.Equal(t, float64(0), p)
}

func TestRunTaskCMDLogger(t *testing.T) {
	b := mq.NewInMemoryBroker()
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
		Image: "ubuntu:mantic",
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
				Image: "ubuntu:mantic",
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
		Image: "ubuntu:mantic",
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
		Image: "ubuntu:mantic",
		Run:   "not_a_thing",
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

func TestRunTaskWithVolumeAndCustomWorkdir(t *testing.T) {
	rt, err := NewDockerRuntime()
	assert.NoError(t, err)

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
		Image: "ubuntu:mantic",
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
		Image: "ubuntu:mantic",
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
		Image: "ubuntu:mantic",
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

func TestRunTaskInitWorkdirLs(t *testing.T) {
	rt, err := NewDockerRuntime()
	assert.NoError(t, err)
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

func Test_imagePull(t *testing.T) {
	ctx := context.Background()

	rt, err := NewDockerRuntime()
	assert.NoError(t, err)
	assert.NotNil(t, rt)

	images, err := rt.client.ImageList(ctx, image.ListOptions{
		Filters: filters.NewArgs(filters.Arg("reference", "debian:*")),
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
			err := rt.imagePull(ctx, &tork.Task{Image: "debian:jessie-slim"}, os.Stdout)
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

	r1, err := rt.client.ImagePull(ctx, "alpine:3.18.3", image.PullOptions{})
	assert.NoError(t, err)
	assert.NoError(t, r1.Close())

	images, err := rt.client.ImageList(ctx, image.ListOptions{
		Filters: filters.NewArgs(filters.Arg("reference", "alpine:3.18.3")),
	})
	assert.NoError(t, err)
	assert.Len(t, images, 1)

	err = rt.client.ImageTag(ctx, "alpine:3.18.3", "localhost:5001/tork/alpine:3.18.3")
	assert.NoError(t, err)

	r2, err := rt.client.ImagePush(ctx, "localhost:5001/tork/alpine:3.18.3", image.PushOptions{RegistryAuth: "noauth"})
	assert.NoError(t, err)
	assert.NoError(t, r2.Close())

	err = rt.imagePull(ctx, &tork.Task{
		Image: "localhost:5001/tork/alpine:3.18.3",
		Registry: &tork.Registry{
			Username: "username",
			Password: "password",
		},
	}, os.Stdout)

	assert.NoError(t, err)
}

func TestRunTaskSandbox(t *testing.T) {
	t.Run("ubuntu", func(t *testing.T) {
		rt, err := NewDockerRuntime(WithSandbox(true))
		assert.NoError(t, err)
		assert.NotNil(t, rt)

		tk := &tork.Task{
			ID:    uuid.NewUUID(),
			Image: "ubuntu:mantic",
			Run:   "id > $TORK_OUTPUT",
		}

		err = rt.Run(context.Background(), tk)
		assert.NoError(t, err)
		assert.Equal(t, "uid=1000(ubuntu) gid=1000(ubuntu) groups=1000(ubuntu)\n", tk.Result)
	})

	t.Run("centos", func(t *testing.T) {
		rt, err := NewDockerRuntime(WithSandbox(true))
		assert.NoError(t, err)
		assert.NotNil(t, rt)

		tk := &tork.Task{
			ID:    uuid.NewUUID(),
			Image: "centos:latest",
			Run:   "id > $TORK_OUTPUT",
		}

		err = rt.Run(context.Background(), tk)
		assert.NoError(t, err)
		assert.Equal(t, "uid=1000 gid=1000 groups=1000\n", tk.Result)
	})

	t.Run("alpine", func(t *testing.T) {
		rt, err := NewDockerRuntime(WithSandbox(true))
		assert.NoError(t, err)
		assert.NotNil(t, rt)

		tk := &tork.Task{
			ID:    uuid.NewUUID(),
			Image: "alpine:latest",
			Run:   "id > $TORK_OUTPUT",
		}

		err = rt.Run(context.Background(), tk)
		assert.NoError(t, err)
		assert.Equal(t, "uid=1000 gid=1000 groups=1000\n", tk.Result)
	})

	t.Run("busybox", func(t *testing.T) {
		rt, err := NewDockerRuntime(WithSandbox(true))
		assert.NoError(t, err)
		assert.NotNil(t, rt)

		tk := &tork.Task{
			ID:    uuid.NewUUID(),
			Image: "busybox:latest",
			Run:   "id > $TORK_OUTPUT",
		}

		err = rt.Run(context.Background(), tk)
		assert.NoError(t, err)
		assert.Equal(t, "uid=1000 gid=1000 groups=1000\n", tk.Result)
	})

	t.Run("busybox-user-root", func(t *testing.T) {
		rt, err := NewDockerRuntime(WithSandbox(true))
		assert.NoError(t, err)
		assert.NotNil(t, rt)

		dockerfile := `
		FROM busybox:latest

		USER root:root
		`

		dockerfileTar, err := mobyarchive.Generate("Dockerfile", dockerfile)
		assert.NoError(t, err)

		buildOptions := types.ImageBuildOptions{
			Context:    dockerfileTar,
			Dockerfile: "Dockerfile",
			Tags:       []string{"busybox_user_root"},
		}

		response, err := rt.client.ImageBuild(context.Background(), dockerfileTar, buildOptions)
		assert.NoError(t, err)
		defer response.Body.Close()

		_, err = io.Copy(os.Stdout, response.Body)
		assert.NoError(t, err)

		tk := &tork.Task{
			ID:    uuid.NewUUID(),
			Image: "busybox_user_root:latest",
			Run:   "id > $TORK_OUTPUT",
		}

		err = rt.Run(context.Background(), tk)
		assert.NoError(t, err)
		assert.Equal(t, "uid=1000 gid=1000 groups=1000\n", tk.Result)
	})

	t.Run("pre_post_and_files", func(t *testing.T) {
		rt, err := NewDockerRuntime(WithSandbox(true))
		assert.NoError(t, err)
		assert.NotNil(t, rt)

		tk := &tork.Task{
			ID:    uuid.NewUUID(),
			Image: "ubuntu:mantic",
			Run:   "id > $TORK_OUTPUT",
			Files: map[string]string{
				"somefile.txt": "hello world",
			},
			Mounts: []tork.Mount{{
				Type:   tork.MountTypeVolume,
				Target: "/workdir",
			}},
			Pre: []*tork.Task{{
				ID:    uuid.NewUUID(),
				Image: "ubuntu:mantic",
				Run:   "id > $TORK_OUTPUT",
			}},
			Post: []*tork.Task{{
				ID:    uuid.NewUUID(),
				Image: "ubuntu:mantic",
				Run:   "id > $TORK_OUTPUT",
			}},
		}

		err = rt.Run(context.Background(), tk)
		assert.NoError(t, err)
		assert.Equal(t, "uid=1000(ubuntu) gid=1000(ubuntu) groups=1000(ubuntu)\n", tk.Result)
	})

	t.Run("custom busybox image", func(t *testing.T) {
		rt, err := NewDockerRuntime(WithSandbox(true), WithBusyboxImage("busybox:latest"))
		assert.NoError(t, err)
		assert.NotNil(t, rt)

		tk := &tork.Task{
			ID:    uuid.NewUUID(),
			Image: "ubuntu:mantic",
			Run:   "id > $TORK_OUTPUT",
			Files: map[string]string{
				"somefile.txt": "hello world",
			},
			Mounts: []tork.Mount{{
				Type:   tork.MountTypeVolume,
				Target: "/workdir",
			}},
			Pre: []*tork.Task{{
				ID:    uuid.NewUUID(),
				Image: "ubuntu:mantic",
				Run:   "id > $TORK_OUTPUT",
			}},
			Post: []*tork.Task{{
				ID:    uuid.NewUUID(),
				Image: "ubuntu:mantic",
				Run:   "id > $TORK_OUTPUT",
			}},
		}

		err = rt.Run(context.Background(), tk)
		assert.NoError(t, err)
		assert.Equal(t, "uid=1000(ubuntu) gid=1000(ubuntu) groups=1000(ubuntu)\n", tk.Result)
	})

	t.Run("no sandbox", func(t *testing.T) {
		rt, err := NewDockerRuntime(WithSandbox(false))
		assert.NoError(t, err)
		assert.NotNil(t, rt)

		tk := &tork.Task{
			ID:    uuid.NewUUID(),
			Image: "ubuntu:mantic",
			Run:   "id > $TORK_OUTPUT",
		}

		err = rt.Run(context.Background(), tk)
		assert.NoError(t, err)
		assert.Equal(t, "uid=0(root) gid=0(root) groups=0(root)\n", tk.Result)
	})

}

func TestRunServiceTask(t *testing.T) {
	rt, err := NewDockerRuntime()
	assert.NoError(t, err)
	assert.NotNil(t, rt)

	tk := &tork.Task{
		ID:    uuid.NewUUID(),
		Image: "node:lts-alpine3.20",
		Run:   "node server.js",
		Ports: []*tork.Port{{
			Port:     "8080",
			HostPort: "55000",
		}},
		Files: map[string]string{
			"server.js": `
             const http = require('http');
             const hostname = '0.0.0.0';
             const port = 8080;
             const server = http.createServer((req, res) => {
               res.statusCode = 200;
               res.setHeader('Content-Type', 'text/plain');
               res.end('Hello World\n');
             });
            server.listen(port, hostname, () => {
              console.log('server running');
            });
			`,
		},
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
		defer cancel()
		_ = rt.Run(ctx, tk)
	}()

	var resp *http.Response
	for i := 0; i < 30; i++ {
		resp, err = http.Get("http://localhost:55000")
		if err == nil && resp.StatusCode == 200 {
			break
		}
		time.Sleep(time.Second)
	}

	assert.Equal(t, 200, resp.StatusCode)
	err = rt.Stop(context.Background(), tk)
	assert.NoError(t, err)
}
