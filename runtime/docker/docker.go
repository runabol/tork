package docker

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"strings"
	"sync"
	"time"

	cliopts "github.com/docker/cli/opts"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/api/types/network"
	regtypes "github.com/docker/docker/api/types/registry"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	"github.com/docker/go-units"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
	"github.com/runabol/tork/broker"
	"github.com/runabol/tork/internal/fns"
	"github.com/runabol/tork/internal/logging"
	"github.com/runabol/tork/internal/syncx"
	"github.com/runabol/tork/internal/uuid"
	"github.com/runabol/tork/runtime"
)

// defaultWorkdir is the directory where `Task.File`s are
// written to by default, should `Task.Workdir` not be set
const (
	defaultWorkdir  = "/tork/workdir"
	DefaultImageTTL = time.Hour * 24
)

type DockerRuntime struct {
	client     *client.Client
	tasks      *syncx.Map[string, string]
	images     map[string]time.Time
	pullq      chan *pullRequest
	mounter    runtime.Mounter
	broker     broker.Broker
	config     string
	privileged bool
	imageTTL   time.Duration
}

type dockerLogsReader struct {
	reader io.Reader
}

type pullRequest struct {
	ctx      context.Context
	image    string
	logger   io.Writer
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

func WithBroker(broker broker.Broker) Option {
	return func(rt *DockerRuntime) {
		rt.broker = broker
	}
}

func WithPrivileged(privileged bool) Option {
	return func(rt *DockerRuntime) {
		rt.privileged = privileged
	}
}

func WithConfig(config string) Option {
	return func(rt *DockerRuntime) {
		rt.config = config
	}
}

func WithImageTTL(ttl time.Duration) Option {
	return func(rt *DockerRuntime) {
		rt.imageTTL = ttl
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
		images: make(map[string]time.Time),
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
	if rt.imageTTL == 0 {
		rt.imageTTL = DefaultImageTTL
	}
	go rt.puller()
	return rt, nil
}

func (d *DockerRuntime) Run(ctx context.Context, t *tork.Task) error {
	// prepare mounts
	for i, mnt := range t.Mounts {
		mnt.ID = uuid.NewUUID()
		err := d.mounter.Mount(ctx, &mnt)
		if err != nil {
			return err
		}
		defer func(m tork.Mount) {
			log.Debug().Msgf("Unmounting %s: %s", m.Type, m.Target)
			if err := d.mounter.Unmount(context.Background(), &m); err != nil {
				log.Error().
					Err(err).
					Msgf("error deleting mount: %s", m)
			}
		}(mnt)
		t.Mounts[i] = mnt
	}
	var logger io.Writer
	if d.broker != nil {
		logger = io.MultiWriter(
			broker.NewLogShipper(d.broker, t.ID),
			logging.NewZerologWriter(t.ID, zerolog.DebugLevel),
		)
	} else {
		logger = logging.NewZerologWriter(t.ID, zerolog.DebugLevel)
	}
	// excute pre-tasks
	for _, pre := range t.Pre {
		pre.ID = uuid.NewUUID()
		pre.Mounts = t.Mounts
		pre.Networks = t.Networks
		pre.Limits = t.Limits
		if err := d.doRun(ctx, pre, logger); err != nil {
			return err
		}
	}
	// run the actual task
	if err := d.doRun(ctx, t, logger); err != nil {
		return err
	}
	// execute post tasks
	for _, post := range t.Post {
		post.ID = uuid.NewUUID()
		post.Mounts = t.Mounts
		post.Networks = t.Networks
		post.Limits = t.Limits
		if err := d.doRun(ctx, post, logger); err != nil {
			return err
		}
	}
	return nil
}

func (d *DockerRuntime) doRun(ctx context.Context, t *tork.Task, logger io.Writer) error {
	if t.ID == "" {
		return errors.New("task id is required")
	}
	if err := d.imagePull(ctx, t, logger); err != nil {
		return errors.Wrapf(err, "error pulling image: %s", t.Image)
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
		case tork.MountTypeTmpfs:
			mt = mount.TypeTmpfs
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

	torkdir := &tork.Mount{
		ID:     uuid.NewUUID(),
		Type:   tork.MountTypeVolume,
		Target: "/tork",
	}
	if err := d.mounter.Mount(ctx, torkdir); err != nil {
		return err
	}
	defer func() {
		if err := d.mounter.Unmount(context.Background(), torkdir); err != nil {
			log.Error().Err(err).Msgf("error unmounting workdir")
		}
	}()
	mounts = append(mounts, mount.Mount{
		Type:   mount.TypeVolume,
		Source: torkdir.Source,
		Target: torkdir.Target,
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

	resources := container.Resources{
		NanoCPUs: cpus,
		Memory:   mem,
	}

	if t.GPUs != "" {
		gpuOpts := cliopts.GpuOpts{}
		if err := gpuOpts.Set(t.GPUs); err != nil {
			return errors.Wrapf(err, "error setting GPUs")
		}
		resources.DeviceRequests = gpuOpts.Value()
	}

	hc := container.HostConfig{
		PublishAllPorts: false,
		Mounts:          mounts,
		Resources:       resources,
		Privileged:      d.privileged,
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
		if strings.HasPrefix(nw, "container:") {
			hc.NetworkMode = container.NetworkMode(nw)
		} else {
			nc.EndpointsConfig[nw] = &network.EndpointSettings{NetworkID: nw}
		}
	}

	// we want to create the container using a background context
	// in case the task is being cancelled while the container is
	// being created. This could lead to a situation where the
	// container is created in a "zombie" state leading to a situation
	// where the attached volumes can't be removed and cleaned up.
	createCtx, createCancel := context.WithTimeout(context.Background(), time.Second*30)
	defer createCancel()
	resp, err := d.client.ContainerCreate(
		createCtx, &containerConf, &hc, &nc, nil, "")
	if err != nil {
		log.Error().Msgf(
			"Error creating container using image %s: %v\n",
			t.Image, err,
		)
		return err
	}

	// create a mapping between task id and container id
	d.tasks.Set(t.ID, resp.ID)

	log.Debug().Msgf("created container %s", resp.ID)

	// remove the container
	defer func() {
		if err := d.stop(context.Background(), t); err != nil {
			log.Error().
				Err(err).
				Str("container-id", resp.ID).
				Msg("error removing container upon completion")
		}
	}()

	// initialize the tork and, optionally, the work directory
	if err := d.initTorkdir(ctx, resp.ID, t); err != nil {
		return errors.Wrapf(err, "error initializing torkdir")
	}
	if err := d.initWorkDir(ctx, resp.ID, t); err != nil {
		return errors.Wrapf(err, "error initializing workdir")
	}

	// create a context for the sidecars
	sidecarCtx, sidecarCancel := context.WithCancel(ctx)
	// execute the sidecars
	var sidecarsWG sync.WaitGroup
	var sidecarErrCh = make(chan error, len(t.Sidecars))
	for _, sidecar := range t.Sidecars {
		sidecar.ID = uuid.NewUUID()
		sidecar.Mounts = t.Mounts
		// add the task container network to the sidecar
		sidecar.Networks = append(t.Networks, fmt.Sprintf("container:%s", resp.ID))
		sidecar.Limits = t.Limits
		sidecarsWG.Add(1)
		go func(st *tork.Task) {
			defer sidecarsWG.Done()
			if err := d.doRun(sidecarCtx, st, logger); err != nil && !errors.Is(err, context.Canceled) {
				log.Error().Err(err).Msgf("error running sidecar %s", st.ID)
				sidecarErrCh <- err
			}
		}(sidecar)
	}
	defer sidecarsWG.Wait() // wait for all sidecars to exit
	defer sidecarCancel()   // cancel the sidecars context when the task is done

	// start the container
	log.Debug().Msgf("Starting container %s", resp.ID)
	err = d.client.ContainerStart(
		ctx, resp.ID, container.StartOptions{})
	if err != nil {
		return errors.Wrapf(err, "error starting container %s: %v\n", resp.ID, err)
	}

	// report task progress
	pctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go d.reportProgress(pctx, resp.ID, t)

	// read the container's stdout
	out, err := d.client.ContainerLogs(
		ctx,
		resp.ID,
		container.LogsOptions{
			ShowStdout: true,
			ShowStderr: true,
			Follow:     true,
		},
	)
	if err != nil {
		return errors.Wrapf(err, "error getting logs for container %s: %v\n", resp.ID, err)
	}
	// close the stdout reader
	defer func() {
		if err := out.Close(); err != nil {
			log.Error().Err(err).Msgf("error closing stdout on container %s", resp.ID)
		}
	}()
	// copy the container's stdout to the logger
	go func() {
		_, err = io.Copy(logger, dockerLogsReader{reader: out})
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
			log.Error().Err(err).Msgf("error reading the std out")
		}
	}()

	// wait for the task to finish execution
	statusCh, errCh := d.client.ContainerWait(ctx, resp.ID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh: // error waiting for the container to finish
		if err != nil {
			return err
		}
	case sidecarErr := <-sidecarErrCh: // sidecar error
		return sidecarErr
	case status := <-statusCh:
		if status.StatusCode != 0 { // error
			out, err := d.client.ContainerLogs(
				ctx,
				resp.ID,
				container.LogsOptions{
					ShowStdout: true,
					ShowStderr: true,
					Tail:       "10",
				},
			)
			if err != nil {
				log.Error().Err(err).Msg("error tailing the log")
				return errors.Errorf("exit code %d", status.StatusCode)
			}
			buf, err := io.ReadAll(dockerLogsReader{reader: out})
			if err != nil {
				log.Error().Err(err).Msg("error copying the output")
			}
			return errors.Errorf("exit code %d: %s", status.StatusCode, string(buf))
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

func (d *DockerRuntime) reportProgress(ctx context.Context, containerID string, t *tork.Task) {
	for {
		progress, err := d.readProgress(ctx, containerID)
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
			if progress != t.Progress {
				t.Progress = progress
				if err := d.broker.PublishTaskProgress(ctx, t); err != nil {
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

func (d *DockerRuntime) readProgress(ctx context.Context, containerID string) (float64, error) {
	r, _, err := d.client.CopyFromContainer(ctx, containerID, "/tork/progress")
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

func (d *DockerRuntime) initTorkdir(ctx context.Context, containerID string, t *tork.Task) error {
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

	if t.Run != "" {
		if err := ar.WriteFile("entrypoint", 0555, []byte(t.Run)); err != nil {
			return err
		}
	}

	if err := d.client.CopyToContainer(ctx, containerID, "/tork", ar, types.CopyToContainerOptions{}); err != nil {
		return err
	}

	return nil
}

func (d *DockerRuntime) initWorkDir(ctx context.Context, containerID string, t *tork.Task) (err error) {
	if len(t.Files) == 0 {
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

	for filename, contents := range t.Files {
		if err := ar.WriteFile(filename, 0444, []byte(contents)); err != nil {
			return err
		}
	}

	if err := d.client.CopyToContainer(ctx, containerID, t.Workdir, ar, types.CopyToContainerOptions{}); err != nil {
		return err
	}

	return nil
}

func (d *DockerRuntime) stop(ctx context.Context, t *tork.Task) error {
	containerID, ok := d.tasks.Get(t.ID)
	if !ok {
		return nil
	}
	d.tasks.Delete(t.ID)
	log.Debug().Msgf("Attempting to stop and remove container %v", containerID)
	return d.client.ContainerRemove(ctx, containerID, container.RemoveOptions{
		RemoveVolumes: true,
		RemoveLinks:   false,
		Force:         true,
	})
}

func (d *DockerRuntime) HealthCheck(ctx context.Context) error {
	_, err := d.client.ContainerList(ctx, container.ListOptions{})
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

func (r dockerLogsReader) Read(p []byte) (int, error) {
	hdr := make([]byte, 8)
	_, err := r.reader.Read(hdr)
	if err != nil {
		if err != io.EOF {
			return 0, err
		}
	}
	count := binary.BigEndian.Uint32(hdr[4:8])
	data := make([]byte, count)
	_, err = r.reader.Read(data)
	if err != nil {
		if err != io.EOF {
			return 0, err
		}
	}
	n := copy(p, data)
	return n, err
}

func (d *DockerRuntime) imagePull(ctx context.Context, t *tork.Task, logger io.Writer) error {
	pr := &pullRequest{
		ctx:    ctx,
		image:  t.Image,
		logger: logger,
		done:   make(chan error),
	}
	if t.Registry != nil {
		pr.registry = registry{
			username: t.Registry.Username,
			password: t.Registry.Password,
		}
	}
	d.pullq <- pr
	err := <-pr.done
	return err
}

// puller is a goroutine that serializes all requests
// to pull images from the docker repo
func (d *DockerRuntime) puller() {
	for pr := range d.pullq {
		pr.done <- d.doPullRequest(pr)
	}
}

// doPullRequest handles the pull queue requests.
// Relies on the fact that pull requests are handled serially
func (d *DockerRuntime) doPullRequest(pr *pullRequest) error {
	// prune old images
	if err := d.pruneImages(context.Background(), pr.image); err != nil {
		log.Error().Err(err).Msg("error pruning images")
	}
	// check if we have the image already
	_, ok := d.images[pr.image]
	if ok {
		d.images[pr.image] = time.Now()
		return nil
	}
	// let's check if we have the image locally already
	imageExists, err := d.imageExistsLocally(pr.ctx, pr.image)
	if err != nil {
		return err
	}
	if !imageExists {
		var authConfig regtypes.AuthConfig
		if pr.registry.username != "" {
			authConfig = regtypes.AuthConfig{
				Username: pr.registry.username,
				Password: pr.registry.password,
			}
		} else {
			ref, err := parseRef(pr.image)
			if err != nil {
				return err
			}
			if ref.domain != "" {
				username, password, err := getRegistryCredentials(d.config, ref.domain)
				if err != nil {
					return err
				}
				authConfig = regtypes.AuthConfig{
					Username: username,
					Password: password,
				}
			}
		}

		encodedJSON, err := json.Marshal(authConfig)
		if err != nil {
			return err
		}
		authStr := base64.URLEncoding.EncodeToString(encodedJSON)
		reader, err := d.client.ImagePull(
			pr.ctx, pr.image, image.PullOptions{RegistryAuth: authStr})
		if err != nil {
			return err
		}
		defer fns.CloseIgnore(reader)

		if _, err := io.Copy(pr.logger, reader); err != nil {
			return err
		}
	}

	d.images[pr.image] = time.Now()

	return nil
}

// pruneImages removes all expired images from the local docker
// image store except for the one specified in the exclude argument
func (d *DockerRuntime) pruneImages(ctx context.Context, exclude string) error {
	for img, t := range d.images {
		if img != exclude && time.Since(t) > d.imageTTL {
			// remove the image if it's last use was more than
			// the imageTTL duration ago and is not currently in use
			if _, err := d.client.ImageRemove(ctx, img, image.RemoveOptions{Force: false}); err != nil {
				return err
			}
			log.Debug().Msgf("pruned image %s", img)
			delete(d.images, img)
		}
	}
	return nil
}

func (d *DockerRuntime) imageExistsLocally(ctx context.Context, name string) (bool, error) {
	images, err := d.client.ImageList(
		ctx,
		image.ListOptions{All: true},
	)
	if err != nil {
		return false, err
	}
	for _, img := range images {
		for _, tag := range img.RepoTags {
			if tag == name {
				return true, nil
			}
		}
	}
	return false, nil
}
