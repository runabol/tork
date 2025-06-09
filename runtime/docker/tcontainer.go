package docker

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	cliopts "github.com/docker/cli/opts"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	"github.com/docker/go-connections/nat"
	"github.com/gosimple/slug"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
	"github.com/runabol/tork/broker"
	"github.com/runabol/tork/internal/fns"
	"github.com/runabol/tork/internal/uuid"
	"github.com/runabol/tork/runtime"
)

type tcontainer struct {
	id      string
	client  *client.Client
	mounter runtime.Mounter
	broker  broker.Broker
	task    *tork.Task
	logger  io.Writer
	torkdir *tork.Mount
}

func createTaskContainer(ctx context.Context, rt *DockerRuntime, t *tork.Task, logger io.Writer) (*tcontainer, error) {
	if t.ID == "" {
		return nil, errors.New("task id is required")
	}
	if err := rt.imagePull(ctx, t, logger); err != nil {
		return nil, errors.Wrapf(err, "error pulling image: %s", t.Image)
	}
	env := []string{}
	for name, value := range t.Env {
		env = append(env, fmt.Sprintf("%s=%s", name, value))
	}
	env = append(env, "TORK_OUTPUT=/tork/stdout")
	env = append(env, "TORK_PROGRESS=/tork/progress")

	var mounts []mount.Mount

	for _, m := range t.Mounts {
		var mt mount.Type
		switch m.Type {
		case tork.MountTypeVolume:
			mt = mount.TypeVolume
			if m.Target == "" {
				return nil, errors.Errorf("volume target is required")
			}
		case tork.MountTypeBind:
			mt = mount.TypeBind
			if m.Target == "" {
				return nil, errors.Errorf("bind target is required")
			}
			if m.Source == "" {
				return nil, errors.Errorf("bind source is required")
			}
		case tork.MountTypeTmpfs:
			mt = mount.TypeTmpfs
		default:
			return nil, errors.Errorf("unknown mount type: %s", m.Type)
		}
		mount := mount.Mount{
			Type:   mt,
			Source: m.Source,
			Target: m.Target,
		}
		log.Debug().Msgf("Mounting %s -> %s", mount.Source, mount.Target)
		mounts = append(mounts, mount)
	}

	torkdir := &tork.Mount{
		ID:     uuid.NewUUID(),
		Type:   tork.MountTypeVolume,
		Target: "/tork",
	}
	if err := rt.mounter.Mount(ctx, torkdir); err != nil {
		return nil, err
	}
	mounts = append(mounts, mount.Mount{
		Type:   mount.TypeVolume,
		Source: torkdir.Source,
		Target: torkdir.Target,
	})

	// parse task limits
	cpus, err := parseCPUs(t.Limits)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid CPUs value")
	}
	mem, err := parseMemory(t.Limits)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid memory value")
	}

	resources := container.Resources{
		NanoCPUs: cpus,
		Memory:   mem,
	}

	if t.GPUs != "" {
		gpuOpts := cliopts.GpuOpts{}
		if err := gpuOpts.Set(t.GPUs); err != nil {
			return nil, errors.Wrapf(err, "error setting GPUs")
		}
		resources.DeviceRequests = gpuOpts.Value()
	}

	cmd := t.CMD
	if len(cmd) == 0 {
		cmd = []string{"/tork/entrypoint"}
	}
	entrypoint := t.Entrypoint
	if len(entrypoint) == 0 && t.Run != "" {
		entrypoint = []string{"sh", "-c"}
	}
	containerConf := container.Config{
		Image:      t.Image,
		Env:        env,
		Cmd:        cmd,
		Entrypoint: entrypoint,
	}

	hc := container.HostConfig{
		PublishAllPorts: false,
		Mounts:          mounts,
		Resources:       resources,
		Privileged:      rt.privileged,
	}

	if t.Probe != nil {
		containerConf.ExposedPorts = nat.PortSet{
			nat.Port(fmt.Sprintf("%d/tcp", t.Probe.Port)): struct{}{}, // Expose the probe port
		}

		hc.PortBindings = nat.PortMap{
			nat.Port(fmt.Sprintf("%d/tcp", t.Probe.Port)): []nat.PortBinding{
				{
					HostIP:   "localhost",
					HostPort: "0", // Let Docker assign a random port
				},
			},
		}
	}

	// we want to override the default
	// image WORKDIR only if the task
	// introduces work files _or_ if the
	// user specifies a WORKDIR
	if t.Workdir != "" {
		containerConf.WorkingDir = t.Workdir
	} else if len(t.Files) > 0 {
		t.Workdir = defaultWorkdir
		containerConf.WorkingDir = t.Workdir
	}

	nc := network.NetworkingConfig{
		EndpointsConfig: make(map[string]*network.EndpointSettings),
	}

	for _, nw := range t.Networks {
		endpoint := &network.EndpointSettings{
			NetworkID: nw,
			Aliases:   []string{slug.Make(t.Name)},
		}
		nc.EndpointsConfig[nw] = endpoint
	}

	// we want to create the container using a background context
	// in case the task is being cancelled while the container is
	// being created. This could lead to a situation where the
	// container is created in a "zombie" state leading to a situation
	// where the attached volumes can't be removed and cleaned up.
	createCtx, createCancel := context.WithTimeout(context.Background(), time.Second*30)
	defer createCancel()
	resp, err := rt.client.ContainerCreate(
		createCtx, &containerConf, &hc, &nc, nil, "")
	if err != nil {
		log.Error().Msgf(
			"Error creating container using image %s: %v\n",
			t.Image, err,
		)
		return nil, err
	}

	tc := &tcontainer{
		id:      resp.ID,
		client:  rt.client,
		mounter: rt.mounter,
		broker:  rt.broker,
		task:    t,
		torkdir: torkdir,
		logger:  logger,
	}

	// initialize the tork and, optionally, the work directory
	if err := tc.initTorkdir(ctx); err != nil {
		return nil, errors.Wrapf(err, "error initializing torkdir")
	}

	if err := tc.initWorkDir(ctx); err != nil {
		return nil, errors.Wrapf(err, "error initializing workdir")
	}

	log.Debug().Msgf("Created container %s", tc.id)

	return tc, nil
}

func (tc *tcontainer) Start(ctx context.Context) error {
	// start the container
	log.Debug().Msgf("Starting container %s", tc.id)
	err := tc.client.ContainerStart(
		ctx, tc.id, container.StartOptions{})
	if err != nil {
		return errors.Wrapf(err, "error starting container %s: %v\n", tc.id, err)
	}

	// wait for the container to be ready
	if err := tc.probeContainer(ctx); err != nil {
		return errors.Wrapf(err, "error waiting for container %s to be ready", tc.id)
	}

	return nil
}

func (tc *tcontainer) Remove(ctx context.Context) error {
	log.Debug().Msgf("Attempting to stop and remove container %v", tc.id)
	if err := tc.client.ContainerRemove(ctx, tc.id, container.RemoveOptions{
		RemoveVolumes: true,
		RemoveLinks:   false,
		Force:         true,
	}); err != nil {
		return err
	}
	if err := tc.mounter.Unmount(ctx, tc.torkdir); err != nil {
		return errors.Wrapf(err, "error unmounting torkdir %s", tc.torkdir.ID)
	}
	return nil
}

func (tc *tcontainer) Wait(ctx context.Context) (string, error) {
	// report task progress
	pctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go tc.reportProgress(pctx)

	// read the container's stdout
	out, err := tc.client.ContainerLogs(
		ctx,
		tc.id,
		container.LogsOptions{
			ShowStdout: true,
			ShowStderr: true,
			Follow:     true,
		},
	)
	if err != nil {
		return "", errors.Wrapf(err, "error getting logs for container %s: %v\n", tc.id, err)
	}
	// close the stdout reader
	defer func() {
		if err := out.Close(); err != nil {
			log.Error().Err(err).Msgf("error closing stdout on container %s", tc.id)
		}
	}()
	// copy the container's stdout to the logger
	go func() {
		_, err = io.Copy(tc.logger, dockerLogsReader{reader: out})
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
			log.Error().Err(err).Msgf("error reading the std out")
		}
	}()
	// wait for the task to finish execution
	statusCh, errCh := tc.client.ContainerWait(ctx, tc.id, container.WaitConditionNotRunning)
	select {
	case err := <-errCh: // error waiting for the container to finish
		return "", err
	case status := <-statusCh:
		if status.StatusCode != 0 { // error
			out, err := tc.client.ContainerLogs(
				ctx,
				tc.id,
				container.LogsOptions{
					ShowStdout: true,
					ShowStderr: true,
					Tail:       "10",
				},
			)
			if err != nil {
				log.Error().Err(err).Msg("error tailing the log")
				return "", errors.Errorf("exit code %d", status.StatusCode)
			}
			buf, err := io.ReadAll(dockerLogsReader{reader: out})
			if err != nil {
				log.Error().Err(err).Msg("error copying the output")
			}
			return "", errors.Errorf("exit code %d: %s", status.StatusCode, string(buf))
		} else {
			stdout, err := tc.readOutput(ctx)
			if err != nil {
				return "", err
			}
			log.Debug().
				Int64("status-code", status.StatusCode).
				Str("task-id", tc.task.ID).
				Msg("task completed")
			return stdout, nil
		}
	}

}

func (tc *tcontainer) probeContainer(ctx context.Context) error {
	if tc.task.Probe == nil {
		return nil
	}

	if tc.task.Probe.Path == "" {
		tc.task.Probe.Path = "/" // Default path if not specified
	}

	if tc.task.Probe.Timeout == "" {
		tc.task.Probe.Timeout = "1m" // Default timeout if not specified
	}

	// If there's a probe, get the assigned host port
	insp, err := tc.client.ContainerInspect(ctx, tc.id)
	if err != nil {
		return errors.Wrapf(err, "error inspecting container %s", tc.id)
	}
	portStr := insp.NetworkSettings.Ports[nat.Port(fmt.Sprintf("%d/tcp", tc.task.Probe.Port))][0].HostPort
	portNum, err := strconv.Atoi(portStr)
	if err != nil {
		return errors.Wrapf(err, "error parsing assigned port %s", portStr)
	}

	probeURL := fmt.Sprintf("http://localhost:%d%s", portNum, tc.task.Probe.Path)
	client := &http.Client{
		Timeout: time.Second * 3,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout: time.Second * 3,
			}).DialContext,
		},
	}

	timeout, err := time.ParseDuration(tc.task.Probe.Timeout)
	if err != nil {
		return errors.Wrapf(err, "error parsing probe timeout %s", tc.task.Probe.Timeout)
	}

	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for {
		select {
		case <-probeCtx.Done():
			return errors.New("probe timed out after 1 minute")
		case <-time.After(time.Second):
			req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, probeURL, nil)
			if err != nil {
				fns.Fprintf(tc.logger, "Failed to create probe request for container %s. Retrying...\n", tc.id)
				continue
			}
			res, err := client.Do(req)
			if err != nil {
				fns.Fprintf(tc.logger, "Probe failed for container %s: %v. Retrying...\n", tc.id, err)
				continue
			}
			fns.CloseIgnore(res.Body)
			if res.StatusCode == http.StatusOK {
				return nil
			} else {
				fns.Fprintf(tc.logger, "Probe for container %s returned status code %d. Retrying...\n", tc.id, res.StatusCode)
			}
		}
	}
}

func (tc *tcontainer) reportProgress(ctx context.Context) {
	for {
		progress, err := tc.readProgress(ctx)
		if err != nil {
			var notFoundError errdefs.ErrNotFound
			if errors.As(err, &notFoundError) {
				return // progress file not found
			}
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Warn().Err(err).Msgf("error reading progress value")
		} else {
			if progress != tc.task.Progress {
				tc.task.Progress = progress
				if err := tc.broker.PublishTaskProgress(ctx, tc.task); err != nil {
					log.Warn().Err(err).Msgf("error publishing task progress")
				}
			}
		}
		select {
		case <-time.After(time.Second * 10):
		case <-ctx.Done():
			return
		}
	}
}

func (tc *tcontainer) readOutput(ctx context.Context) (string, error) {
	r, _, err := tc.client.CopyFromContainer(ctx, tc.id, "/tork/stdout")
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

func (tc *tcontainer) readProgress(ctx context.Context) (float64, error) {
	r, _, err := tc.client.CopyFromContainer(ctx, tc.id, "/tork/progress")
	if err != nil {
		return 0, err
	}
	defer func() {
		err := r.Close()
		if err != nil {
			log.Error().Err(err).Msgf("error closing /tork/progress reader")
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
			return 0, err
		}

		if _, err := io.Copy(&buf, tr); err != nil {
			return 0, err
		}
	}
	s := strings.TrimSpace(buf.String())
	if s == "" {
		return 0, nil
	}
	return strconv.ParseFloat(s, 32)
}

func (tc *tcontainer) initTorkdir(ctx context.Context) error {
	ar, err := NewTempArchive()
	if err != nil {
		return err
	}

	defer func() {
		if err := ar.Remove(); err != nil {
			log.Error().Err(err).Msgf("error removing temp archive: %s", ar.Name())
		}
	}()

	if err := ar.WriteFile("stdout", 0222, []byte{}); err != nil {
		return err
	}
	if err := ar.WriteFile("progress", 0222, []byte{}); err != nil {
		return err
	}

	if tc.task.Run != "" {
		if err := ar.WriteFile("entrypoint", 0555, []byte(tc.task.Run)); err != nil {
			return err
		}
	}

	if err := tc.client.CopyToContainer(ctx, tc.id, "/tork", ar, types.CopyToContainerOptions{}); err != nil {
		return err
	}

	return nil
}

func (tc *tcontainer) initWorkDir(ctx context.Context) (err error) {
	if len(tc.task.Files) == 0 {
		return
	}

	ar, err := NewTempArchive()
	if err != nil {
		return err
	}

	defer func() {
		if err := ar.Remove(); err != nil {
			log.Error().Err(err).Msgf("error removing temp archive: %s", ar.Name())
		}
	}()

	for filename, contents := range tc.task.Files {
		if err := ar.WriteFile(filename, 0444, []byte(contents)); err != nil {
			return err
		}
	}

	if err := tc.client.CopyToContainer(ctx, tc.id, tc.task.Workdir, ar, types.CopyToContainerOptions{}); err != nil {
		return err
	}

	return nil
}
