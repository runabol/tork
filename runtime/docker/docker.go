package docker

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"sync"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	regtypes "github.com/docker/docker/api/types/registry"
	"github.com/docker/docker/client"
	"github.com/docker/go-units"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
	"github.com/runabol/tork/broker"
	"github.com/runabol/tork/internal/fns"
	"github.com/runabol/tork/internal/logging"
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
	client      *client.Client
	tasks       int
	images      map[string]time.Time
	pullq       chan *pullRequest
	mounter     runtime.Mounter
	broker      broker.Broker
	config      string
	privileged  bool
	imageTTL    time.Duration
	imageVerify bool
	mu          sync.Mutex
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

func WithImageVerify(verify bool) Option {
	return func(rt *DockerRuntime) {
		rt.imageVerify = verify
	}
}

func NewDockerRuntime(opts ...Option) (*DockerRuntime, error) {
	dc, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return nil, err
	}
	rt := &DockerRuntime{
		client: dc,
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
	go rt.pruner()
	return rt, nil
}

func (rt *DockerRuntime) Run(ctx context.Context, t *tork.Task) error {
	rt.mu.Lock()
	rt.tasks++
	rt.mu.Unlock()

	defer func() {
		rt.mu.Lock()
		rt.tasks--
		rt.mu.Unlock()
	}()

	// if the tasks has sidecars, we need to create a network
	if len(t.Sidecars) > 0 {
		networkCreateResp, err := rt.client.NetworkCreate(ctx, uuid.NewUUID(), types.NetworkCreate{
			CheckDuplicate: true,
			Driver:         "bridge",
		})
		if err != nil {
			return errors.Wrapf(err, "error creating network")
		}
		log.Debug().Msgf("Created network with ID %s", networkCreateResp.ID)
		defer func() {
			// remove the network when the task is done
			log.Debug().Msgf("Removing network with ID %s", networkCreateResp.ID)
			if err := rt.client.NetworkRemove(context.Background(), networkCreateResp.ID); err != nil {
				log.Error().Err(err).Msgf("error removing network")
			}
		}()
		t.Networks = append(t.Networks, networkCreateResp.ID)
	}

	// prepare mounts
	for i, mnt := range t.Mounts {
		mnt.ID = uuid.NewUUID()
		err := rt.mounter.Mount(ctx, &mnt)
		if err != nil {
			return err
		}
		defer func(m tork.Mount) {
			log.Debug().Msgf("Unmounting %s: %s", m.Type, m.Target)
			if err := rt.mounter.Unmount(context.Background(), &m); err != nil {
				log.Error().
					Err(err).
					Msgf("error deleting mount: %s", m)
			}
		}(mnt)
		t.Mounts[i] = mnt
	}
	var logger io.Writer
	if rt.broker != nil {
		logger = io.MultiWriter(
			broker.NewLogShipper(rt.broker, t.ID),
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
		if err := rt.doRun(ctx, pre, logger); err != nil {
			return err
		}
	}
	// run the actual task
	if err := rt.doRun(ctx, t, logger); err != nil {
		return err
	}
	// execute post tasks
	for _, post := range t.Post {
		post.ID = uuid.NewUUID()
		post.Mounts = t.Mounts
		post.Networks = t.Networks
		post.Limits = t.Limits
		if err := rt.doRun(ctx, post, logger); err != nil {
			return err
		}
	}
	return nil
}

func (rt *DockerRuntime) doRun(ctx context.Context, t *tork.Task, logger io.Writer) error {
	// create a container for the main task
	tc, err := createTaskContainer(ctx, rt, t, logger)
	if err != nil {
		return err
	}

	// remove the container when the task is done
	defer func() {
		if err := tc.Remove(context.Background()); err != nil {
			log.Error().Err(err).Msgf("error removing container %s", tc.id)
		}
	}()

	// start the sidecar containers
	for _, sidecar := range t.Sidecars {
		sidecar.ID = uuid.NewUUID()
		sidecar.Mounts = t.Mounts
		sidecar.Networks = t.Networks
		sidecar.Limits = t.Limits
		sctc, err := createTaskContainer(ctx, rt, sidecar, logger)
		if err != nil {
			return errors.Wrapf(err, "error creating sidecar container")
		}
		// remove the sidecar container when the main task is done
		defer func() {
			if err := sctc.Remove(context.Background()); err != nil {
				log.Error().Err(err).Msgf("error removing sidecar container %s", sctc.id)
			}
		}()
		if err := sctc.Start(ctx); err != nil {
			return errors.Wrapf(err, "error starting sidecar container")
		}
	}

	// start the main task container
	if err := tc.Start(ctx); err != nil {
		return err
	}

	// wait for the task container to finish
	result, err := tc.Wait(ctx)
	if err != nil {
		return err
	}

	t.Result = result

	return nil
}

func (rt *DockerRuntime) HealthCheck(ctx context.Context) error {
	_, err := rt.client.ContainerList(ctx, container.ListOptions{})
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

func (rt *DockerRuntime) imagePull(ctx context.Context, t *tork.Task, logger io.Writer) error {
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
	rt.pullq <- pr
	err := <-pr.done
	return err
}

// puller is a goroutine that serializes all requests
// to pull images from the docker repo
func (rt *DockerRuntime) puller() {
	for pr := range rt.pullq {
		pr.done <- rt.doPullRequest(pr)
	}
}

// doPullRequest handles the pull queue requests.
// Relies on the fact that pull requests are handled serially
func (d *DockerRuntime) doPullRequest(pr *pullRequest) error {
	// check if we have the image already
	d.mu.Lock()
	_, ok := d.images[pr.image]
	d.mu.Unlock()
	if ok {
		d.mu.Lock()
		d.images[pr.image] = time.Now()
		d.mu.Unlock()
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

	// verify the intergrity of the image
	if d.imageVerify {
		if err := d.verifyImage(pr.ctx, pr.image, pr.logger); err != nil {
			log.Error().Err(err).Msgf("image %s is invalid or corrupted", pr.image)
			if _, err := d.client.ImageRemove(context.Background(), pr.image, image.RemoveOptions{Force: true}); err != nil {
				log.Error().Err(err).Msgf("error removing image %s after failed verification", pr.image)
			}
			return errors.Wrapf(err, "image %s is invalid or corrupted", pr.image)
		}
	}

	d.mu.Lock()
	d.images[pr.image] = time.Now()
	d.mu.Unlock()

	return nil
}

// verifyImage checks if the image is valid by inspecting its metadata and doing a partial read.
func (rt *DockerRuntime) verifyImage(ctx context.Context, image string, logger io.Writer) error {
	now := time.Now()

	_, _ = fmt.Fprintf(logger, "verifying image %s with container test...", image)

	// Create a test container without starting it
	resp, err := rt.client.ContainerCreate(ctx, &container.Config{
		Image: image,
		Cmd:   []string{"true"}, // minimal command
	}, nil, nil, nil, "")
	if err != nil {
		_, _ = fmt.Fprintf(logger, "image %s failed verification test: %v", image, err)
		return errors.Wrapf(err, "image %s failed verification test", image)
	}

	// Clean up the test container
	defer func() {
		if err := rt.client.ContainerRemove(context.Background(), resp.ID, container.RemoveOptions{Force: true}); err != nil {
			log.Error().Err(err).Msgf("error removing test container %s", resp.ID)
		}
	}()

	verificationDuration := time.Since(now)
	_, _ = fmt.Fprintf(logger, "image %s verified with container test in %s", image, verificationDuration)

	return nil
}

func (rt *DockerRuntime) pruner() {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for range ticker.C {
		if err := rt.prune(context.Background()); err != nil {
			log.Error().Err(err).Msg("error pruning images")
		}
	}
}

func (rt *DockerRuntime) prune(ctx context.Context) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.tasks > 0 {
		return nil
	}
	for img, t := range rt.images {
		if time.Since(t) > rt.imageTTL {
			// remove the image if its last use was more than
			// the imageTTL duration ago and is not currently in use
			if _, err := rt.client.ImageRemove(ctx, img, image.RemoveOptions{Force: false}); err != nil {
				return err
			}
			log.Debug().Msgf("pruned image %s", img)
			delete(rt.images, img)
		}
	}
	return nil
}

func (rt *DockerRuntime) imageExistsLocally(ctx context.Context, name string) (bool, error) {
	images, err := rt.client.ImageList(
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
