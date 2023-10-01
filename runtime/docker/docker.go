package docker

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"os"
	"strings"
	"time"
	"unicode"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/go-units"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
	"github.com/runabol/tork/internal/syncx"
	"github.com/runabol/tork/internal/uuid"
	"github.com/runabol/tork/runtime"
)

type DockerRuntime struct {
	client  *client.Client
	tasks   *syncx.Map[string, string]
	images  *syncx.Map[string, bool]
	pullq   chan *pullRequest
	mounter runtime.Mounter
}

type printableReader struct {
	reader io.Reader
}

type pullRequest struct {
	image    string
	registry registry
	done     chan error
}

type registry struct {
	username string
	password string
}

type Option = func(rt *DockerRuntime)

func WithMounter(mounter runtime.Mounter) Option {
	return func(rt *DockerRuntime) {
		rt.mounter = mounter
	}
}

func NewDockerRuntime(opts ...Option) (*DockerRuntime, error) {
	dc, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return nil, err
	}
	rt := &DockerRuntime{
		client: dc,
		tasks:  new(syncx.Map[string, string]),
		images: new(syncx.Map[string, bool]),
		pullq:  make(chan *pullRequest, 1),
	}
	for _, o := range opts {
		o(rt)
	}
	// setup a default mounter
	if rt.mounter == nil {
		vmounter, err := NewVolumeMounter()
		if err != nil {
			return nil, err
		}
		rt.mounter = vmounter
	}
	go rt.puller(context.Background())
	return rt, nil
}

func (d *DockerRuntime) Run(ctx context.Context, t *tork.Task) error {
	// prepare mounts
	for i, mnt := range t.Mounts {
		err := d.mounter.Mount(ctx, &mnt)
		if err != nil {
			return err
		}
		defer func(m tork.Mount) {
			uctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			if err := d.mounter.Unmount(uctx, &m); err != nil {
				log.Error().
					Err(err).
					Msgf("error deleting mount: %s", m)
			}
		}(mnt)
		t.Mounts[i] = mnt
	}
	// excute pre-tasks
	for _, pre := range t.Pre {
		pre.ID = uuid.NewUUID()
		pre.Mounts = t.Mounts
		pre.Networks = t.Networks
		pre.Limits = t.Limits
		if err := d.doRun(ctx, pre); err != nil {
			return err
		}
	}
	// run the actual task
	if err := d.doRun(ctx, t); err != nil {
		return err
	}
	// execute post tasks
	for _, post := range t.Post {
		post.ID = uuid.NewUUID()
		post.Mounts = t.Mounts
		post.Networks = t.Networks
		post.Limits = t.Limits
		if err := d.doRun(ctx, post); err != nil {
			return err
		}
	}
	return nil
}

func (d *DockerRuntime) doRun(ctx context.Context, t *tork.Task) error {
	if t.ID == "" {
		return errors.New("task id is required")
	}
	if err := d.imagePull(ctx, t); err != nil {
		return errors.Wrapf(err, "error pulling image: %s", t.Image)
	}

	env := []string{}
	for name, value := range t.Env {
		env = append(env, fmt.Sprintf("%s=%s", name, value))
	}
	env = append(env, "TORK_OUTPUT=/tork/stdout")

	var mounts []mount.Mount

	for _, m := range t.Mounts {
		var mt mount.Type
		switch m.Type {
		case tork.MountTypeVolume:
			mt = mount.TypeVolume
			if m.Target == "" {
				return errors.Errorf("volume target is required")
			}
		case tork.MountTypeBind:
			mt = mount.TypeBind
			if m.Target == "" {
				return errors.Errorf("bind target is required")
			}
			if m.Source == "" {
				return errors.Errorf("bind source is required")
			}
		default:
			return errors.Errorf("unknown mount type: %s", m.Type)
		}
		mount := mount.Mount{
			Type:   mt,
			Source: m.Source,
			Target: m.Target,
		}
		log.Debug().Msgf("Mounting %s -> %s", mount.Source, mount.Target)
		mounts = append(mounts, mount)
	}
	// create the workdir mount
	workdir := &tork.Mount{Type: tork.MountTypeVolume, Target: "/tork"}
	if err := d.mounter.Mount(ctx, workdir); err != nil {
		return err
	}
	defer func() {
		uctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		if err := d.mounter.Unmount(uctx, workdir); err != nil {
			log.Error().Err(err).Msgf("error unmounting workdir")
		}
	}()
	mounts = append(mounts, mount.Mount{
		Type:   mount.TypeVolume,
		Source: workdir.Source,
		Target: workdir.Target,
	})

	// parse task limits
	cpus, err := parseCPUs(t.Limits)
	if err != nil {
		return errors.Wrapf(err, "invalid CPUs value")
	}
	mem, err := parseMemory(t.Limits)
	if err != nil {
		return errors.Wrapf(err, "invalid memory value")
	}

	hc := container.HostConfig{
		PublishAllPorts: true,
		Mounts:          mounts,
		Resources: container.Resources{
			NanoCPUs: cpus,
			Memory:   mem,
		},
	}

	cmd := t.CMD
	if len(cmd) == 0 {
		cmd = []string{"./entrypoint"}
	}
	entrypoint := t.Entrypoint
	if len(entrypoint) == 0 && t.Run != "" {
		entrypoint = []string{"sh", "-c"}
	}
	cc := container.Config{
		Image:      t.Image,
		Env:        env,
		Cmd:        cmd,
		Entrypoint: entrypoint,
		WorkingDir: workdir.Target,
	}

	nc := network.NetworkingConfig{
		EndpointsConfig: make(map[string]*network.EndpointSettings),
	}

	for _, nw := range t.Networks {
		nc.EndpointsConfig[nw] = &network.EndpointSettings{NetworkID: nw}
	}

	resp, err := d.client.ContainerCreate(
		ctx, &cc, &hc, &nc, nil, "")
	if err != nil {
		log.Error().Msgf(
			"Error creating container using image %s: %v\n",
			t.Image, err,
		)
		return err
	}

	log.Debug().Msgf("created container %s", resp.ID)

	// remove the container
	defer func() {
		stopContext, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		if err := d.Stop(stopContext, t); err != nil {
			log.Error().
				Err(err).
				Str("container-id", resp.ID).
				Msg("error removing container upon completion")
		}
	}()

	// initialize the work directory
	if err := d.initWorkdir(ctx, resp.ID, t); err != nil {
		return errors.Wrapf(err, "error initializing container")
	}

	// create a mapping between task id and container id
	d.tasks.Set(t.ID, resp.ID)

	// start the container
	log.Debug().Msgf("Starting container %s", resp.ID)
	err = d.client.ContainerStart(
		ctx, resp.ID, types.ContainerStartOptions{})
	if err != nil {
		return errors.Wrapf(err, "error starting container %s: %v\n", resp.ID, err)
	}
	// read the container's stdout
	out, err := d.client.ContainerLogs(
		ctx,
		resp.ID,
		types.ContainerLogsOptions{
			ShowStdout: true,
			ShowStderr: true,
			Follow:     true,
		},
	)
	if err != nil {
		return errors.Wrapf(err, "error getting logs for container %s: %v\n", resp.ID, err)
	}
	defer func() {
		if err := out.Close(); err != nil {
			log.Error().Err(err).Msgf("error closing stdout on container %s", resp.ID)
		}
	}()
	_, err = io.Copy(os.Stdout, out)
	if err != nil {
		return errors.Wrapf(err, "error reading the std out")
	}
	statusCh, errCh := d.client.ContainerWait(ctx, resp.ID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		if err != nil {
			return err
		}
	case status := <-statusCh:
		if status.StatusCode != 0 { // error
			out, err := d.client.ContainerLogs(
				ctx,
				resp.ID,
				types.ContainerLogsOptions{
					ShowStdout: true,
					ShowStderr: true,
					Tail:       "10",
				},
			)
			if err != nil {
				log.Error().Err(err).Msg("error tailing the log")
				return errors.Errorf("exit code %d", status.StatusCode)
			}
			bufout := new(strings.Builder)
			_, err = io.Copy(bufout, printableReader{reader: out})
			if err != nil {
				log.Error().Err(err).Msg("error copying the output")
			}
			return errors.Errorf("exit code %d: %s", status.StatusCode, bufout.String())
		} else {
			stdout, err := d.readOutput(ctx, resp.ID)
			if err != nil {
				return err
			}
			t.Result = stdout
		}
		log.Debug().
			Int64("status-code", status.StatusCode).
			Str("task-id", t.ID).
			Msg("task completed")
	}
	return nil
}

func (d *DockerRuntime) readOutput(ctx context.Context, containerID string) (string, error) {
	r, _, err := d.client.CopyFromContainer(ctx, containerID, "/tork/stdout")
	if err != nil {
		return "", err
	}
	defer func() {
		err := r.Close()
		if err != nil {
			log.Error().Err(err).Msgf("error closing /tork/stdout reader")
		}
	}()
	tr := tar.NewReader(r)
	var buf bytes.Buffer
	for {
		_, err := tr.Next()
		if err == io.EOF {
			break // End of archive
		}
		if err != nil {
			return "", err
		}

		if _, err := io.Copy(&buf, tr); err != nil {
			return "", err
		}
	}
	return buf.String(), nil
}

func (d *DockerRuntime) initWorkdir(ctx context.Context, containerID string, t *tork.Task) error {
	log.Debug().Msgf("initialize the workdir for container %s", containerID)
	// create the archive
	filename, err := createArchive(t)
	if err != nil {
		return err
	}
	// clean up the archive
	defer func() {
		err := os.Remove(filename)
		if err != nil {
			log.Error().Err(err).Msgf("error removing archive: %s", filename)
		}
	}()
	// open the archive for reading by the container
	ar, err := os.Open(filename)
	if err != nil {
		return err
	}
	// close the archive
	defer func() {
		err := ar.Close()
		if err != nil {
			log.Error().Err(err).Msg("error closing archive file")
		}
	}()
	r := bufio.NewReader(ar)
	if err := d.client.CopyToContainer(ctx, containerID, "/tork", r, types.CopyToContainerOptions{}); err != nil {
		return err
	}
	return nil
}

func createArchive(t *tork.Task) (string, error) {
	// create an archive file
	ar, err := os.CreateTemp("", "archive-*.tar")
	if err != nil {
		return "", errors.Wrapf(err, "error creating tar file")
	}
	defer func() {
		if err := ar.Close(); err != nil {
			log.Error().Err(err).Msg("error closing archive.tar file")
		}
	}()
	// write the run script as an entrypoint
	buf := bufio.NewWriter(ar)
	tw := tar.NewWriter(buf)
	if t.Run != "" {
		hdr := &tar.Header{
			Name: "entrypoint",
			Mode: 0111, // execute only
			Size: int64(len(t.Run)),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return "", err
		}
		if _, err := tw.Write([]byte(t.Run)); err != nil {
			return "", err
		}
	}
	// write an stdout placeholder file
	hdr := &tar.Header{
		Name: "stdout",
		Mode: 0222, // write-only
		Size: int64(0),
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return "", err
	}
	if _, err := tw.Write([]byte{}); err != nil {
		return "", err
	}
	// write all other files specified on the task
	for filename, contents := range t.Files {
		hdr := &tar.Header{
			Name: filename,
			Mode: 0444, // read-only
			Size: int64(len(contents)),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return "", err
		}
		if _, err := tw.Write([]byte(contents)); err != nil {
			return "", err
		}
	}
	if err := buf.Flush(); err != nil {
		return "", err
	}
	// close the tar writer
	if err := tw.Close(); err != nil {
		return "", err
	}
	return ar.Name(), nil
}

func (d *DockerRuntime) Stop(ctx context.Context, t *tork.Task) error {
	containerID, ok := d.tasks.Get(t.ID)
	if !ok {
		return nil
	}
	d.tasks.Delete(t.ID)
	log.Printf("Attempting to stop and remove container %v", containerID)
	return d.client.ContainerRemove(ctx, containerID, types.ContainerRemoveOptions{
		RemoveVolumes: true,
		RemoveLinks:   false,
		Force:         true,
	})
}

func (d *DockerRuntime) HealthCheck(ctx context.Context) error {
	_, err := d.client.ContainerList(ctx, types.ContainerListOptions{})
	return err
}

// take from https://github.com/docker/cli/blob/9bd5ec504afd13e82d5e50b60715e7190c1b2aa0/opts/opts.go#L393-L403
func parseCPUs(limits *tork.TaskLimits) (int64, error) {
	if limits == nil || limits.CPUs == "" {
		return 0, nil
	}
	cpu, ok := new(big.Rat).SetString(limits.CPUs)
	if !ok {
		return 0, fmt.Errorf("failed to parse %v as a rational number", limits.CPUs)
	}
	nano := cpu.Mul(cpu, big.NewRat(1e9, 1))
	if !nano.IsInt() {
		return 0, fmt.Errorf("value is too precise")
	}
	return nano.Num().Int64(), nil
}

func parseMemory(limits *tork.TaskLimits) (int64, error) {
	if limits == nil || limits.Memory == "" {
		return 0, nil
	}
	return units.RAMInBytes(limits.Memory)
}

func (r printableReader) Read(p []byte) (int, error) {
	buf := make([]byte, len(p))
	n, err := r.reader.Read(buf)
	if err != nil {
		return n, err
	}
	j := 0
	for i := 0; i < n; i++ {
		if unicode.IsPrint(rune(buf[i])) {
			p[j] = buf[i]
			j++
		}
	}
	if j == 0 {
		return 0, io.EOF
	}
	return j, nil
}

func (d *DockerRuntime) imagePull(ctx context.Context, t *tork.Task) error {
	_, ok := d.images.Get(t.Image)
	if ok {
		return nil
	}
	// let's check if we have the image
	// locally already
	images, err := d.client.ImageList(
		ctx,
		types.ImageListOptions{All: true},
	)
	if err != nil {
		return err
	}
	for _, img := range images {
		for _, tag := range img.RepoTags {
			if tag == t.Image {
				d.images.Set(tag, true)
				return nil
			}
		}
	}
	pr := &pullRequest{
		image: t.Image,
		done:  make(chan error),
	}
	if t.Registry != nil {
		pr.registry = registry{
			username: t.Registry.Username,
			password: t.Registry.Password,
		}
	}
	d.pullq <- pr
	return <-pr.done
}

// puller is a goroutine that serializes all requests
// to pull images from the docker repo
func (d *DockerRuntime) puller(ctx context.Context) {
	for pr := range d.pullq {
		authConfig := types.AuthConfig{
			Username: pr.registry.username,
			Password: pr.registry.password,
		}
		encodedJSON, err := json.Marshal(authConfig)
		if err != nil {
			pr.done <- err
			continue
		}
		authStr := base64.URLEncoding.EncodeToString(encodedJSON)
		reader, err := d.client.ImagePull(
			ctx, pr.image, types.ImagePullOptions{RegistryAuth: authStr})
		if err != nil {
			pr.done <- err
			continue
		}
		_, err = io.Copy(os.Stdout, reader)
		if err != nil {
			pr.done <- err
			continue
		}
		pr.done <- nil
	}
}
