package podman

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
	"github.com/runabol/tork/broker"
	"github.com/runabol/tork/internal/syncx"
	"github.com/runabol/tork/internal/uuid"
	"github.com/runabol/tork/runtime"
)

const (
	defaultWorkdir = "/tork/workdir"
)

// PodmanRuntime is a runtime that uses podman to run tasks in containers
// on the host machine using podman as the container runtime engine
// and the podman CLI to manage containers.
type PodmanRuntime struct {
	broker     broker.Broker
	pullq      chan *pullRequest
	images     *syncx.Map[string, bool]
	tasks      *syncx.Map[string, string]
	mounter    runtime.Mounter
	privileged bool
}

type pullRequest struct {
	ctx    context.Context
	image  string
	logger io.Writer
	done   chan error
}

type Option = func(rt *PodmanRuntime)

func WithBroker(broker broker.Broker) Option {
	return func(rt *PodmanRuntime) {
		rt.broker = broker
	}
}

func WithPrivileged(privileged bool) Option {
	return func(rt *PodmanRuntime) {
		rt.privileged = privileged
	}
}

func WithMounter(mounter runtime.Mounter) Option {
	return func(rt *PodmanRuntime) {
		rt.mounter = mounter
	}
}

func NewPodmanRuntime(opts ...Option) *PodmanRuntime {
	rt := &PodmanRuntime{
		tasks:  new(syncx.Map[string, string]),
		pullq:  make(chan *pullRequest, 1),
		images: new(syncx.Map[string, bool]),
	}
	for _, o := range opts {
		o(rt)
	}
	if rt.mounter == nil {
		rt.mounter = NewVolumeMounter()
	}
	go rt.puller()
	return rt
}

func (d *PodmanRuntime) Run(ctx context.Context, t *tork.Task) error {
	// Validate the task
	if t.ID == "" {
		return errors.New("task id is required")
	}
	if t.Image == "" {
		return errors.New("task image is required")
	}
	// prepare mounts
	for i, mnt := range t.Mounts {
		mnt.ID = uuid.NewUUID()
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
	// setup logging
	var logger io.Writer
	if d.broker != nil {
		logger = broker.NewLogShipper(d.broker, t.ID)
	} else {
		logger = os.Stdout
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

func (d *PodmanRuntime) doRun(ctx context.Context, t *tork.Task, logger io.Writer) error {
	// Initiallize the work directory
	workDir := path.Join(os.TempDir(), "tork", t.ID)
	if err := os.MkdirAll(workDir, 0777); err != nil {
		return err
	}
	defer os.RemoveAll(workDir)

	// Create the output file
	outputFile := fmt.Sprintf("%s/output", workDir)
	if _, err := os.Create(outputFile); err != nil {
		return errors.Wrapf(err, "failed to create %s", outputFile)
	}
	if err := os.Chmod(outputFile, 0777); err != nil {
		return errors.Wrapf(err, "failed to chmod %s", outputFile)
	}
	// Create the progress file
	progressFile := fmt.Sprintf("%s/progress", workDir)
	if _, err := os.Create(progressFile); err != nil {
		return errors.Wrapf(err, "failed to create %s", progressFile)
	}
	if err := os.Chmod(progressFile, 0777); err != nil {
		return errors.Wrapf(err, "failed to chmod %s", progressFile)
	}
	// Write the task run script
	runScriptPath := fmt.Sprintf("%s/entrypoint.sh", workDir)
	var runScriptContent []byte
	if t.Run != "" {
		runScriptContent = []byte(t.Run)
	} else {
		runScriptContent = []byte(strings.Join(t.CMD, " "))
	}

	if err := os.WriteFile(runScriptPath, runScriptContent, 0755); err != nil {
		return err
	}

	// pull the image
	if err := d.imagePull(ctx, t, logger); err != nil {
		return errors.Wrapf(err, "error pulling image: %s", t.Image)
	}

	// Create the container
	createCtx, createCancel := context.WithTimeout(context.Background(), time.Second*30)
	defer createCancel()
	if len(t.Entrypoint) == 0 {
		t.Entrypoint = []string{"sh", "-c"}
	}
	createCmd := exec.CommandContext(createCtx,
		"podman", "create",
		"-v", fmt.Sprintf("%s:/tork", workDir),
		"--entrypoint", t.Entrypoint[0],
	)

	// Set the environment variables
	env := []string{}
	for name, value := range t.Env {
		env = append(env, fmt.Sprintf("%s=%s", name, value))
	}
	env = append(env, "TORK_OUTPUT=/tork/output")
	env = append(env, "TORK_PROGRESS=/tork/progress")
	for _, ev := range env {
		createCmd.Args = append(createCmd.Args, "-e", ev)
	}

	// add networks to the container
	for _, network := range t.Networks {
		createCmd.Args = append(createCmd.Args, "--network", network)
	}

	// add mounts to the container
	for _, mount := range t.Mounts {
		switch mount.Type {
		case tork.MountTypeVolume:
			createCmd.Args = append(createCmd.Args, "-v", fmt.Sprintf("%s:%s", mount.Source, mount.Target))
		case tork.MountTypeBind:
			createCmd.Args = append(createCmd.Args, "-v", fmt.Sprintf("%s:%s", mount.Source, mount.Target))
		default:
			return fmt.Errorf("unknown mount type: %s", mount.Type)
		}
	}

	// we want to override the default
	// image WORKDIR only if the task
	// introduces work files _or_ if the
	// user specifies a WORKDIR
	if t.Workdir != "" {
		createCmd.Args = append(createCmd.Args, "-w", t.Workdir)
	} else if len(t.Files) > 0 {
		t.Workdir = defaultWorkdir
		createCmd.Args = append(createCmd.Args, "-w", defaultWorkdir)
	}
	for filename, contents := range t.Files {
		if err := os.MkdirAll(path.Join(workDir, "workdir"), 0700); err != nil {
			return err
		}
		if err := os.WriteFile(path.Join(workDir, "workdir", filename), []byte(contents), 0444); err != nil {
			return err
		}
	}

	if d.privileged {
		createCmd.Args = append(createCmd.Args, "--privileged")
	}

	// container image
	createCmd.Args = append(createCmd.Args, t.Image)
	// set the entrypoint
	createCmd.Args = append(createCmd.Args, t.Entrypoint[1:]...)
	createCmd.Args = append(createCmd.Args, "/tork/entrypoint.sh")

	var stdoutBuf bytes.Buffer
	createCmd.Stdout = &stdoutBuf
	createCmd.Stderr = logger

	if err := createCmd.Run(); err != nil {
		return fmt.Errorf("failed to create container: %w", err)
	}

	// Extract container ID
	containerID := strings.TrimSpace(stdoutBuf.String())
	if containerID == "" {
		return errors.New("failed to retrieve container ID")
	}

	log.Debug().Msgf("created container %s", containerID)

	// create a mapping between task id and container id
	d.tasks.Set(t.ID, containerID)

	// Ensure the container is removed after execution
	defer func() {
		stopContext, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		if err := d.Stop(stopContext, t); err != nil {
			log.Error().
				Err(err).
				Str("container-id", containerID).
				Msg("error removing container upon completion")
		}
	}()

	// Start a goroutine to report user-reported progress
	go d.reportProgress(ctx, progressFile, t)

	// Start the container
	log.Debug().Msgf("Starting container %s", containerID)
	startCmd := exec.CommandContext(ctx, "podman", "start", containerID)
	if err := startCmd.Run(); err != nil {
		return fmt.Errorf("failed to start container %s: %w", containerID, err)
	}

	// read logs
	logsCmd := exec.CommandContext(ctx, "podman", "logs", "--follow", containerID)
	logsCmd.Stdout = logger
	logsCmd.Stderr = logger
	if err := logsCmd.Run(); err != nil {
		return fmt.Errorf("failed to read logs: %w", err)
	}

	// check the exit code
	exitCmd := exec.CommandContext(ctx, "podman", "inspect", "--format", "{{.State.ExitCode}}", containerID)
	var exitCodeBuf bytes.Buffer
	exitCmd.Stdout = &exitCodeBuf
	if err := exitCmd.Run(); err != nil {
		return fmt.Errorf("failed to inspect container: %w", err)
	}
	exitCode := strings.TrimSpace(exitCodeBuf.String())
	if exitCode != "0" {
		return fmt.Errorf("container exited with code %s", exitCode)
	}

	// Read the output
	stdout, err := os.ReadFile(outputFile)
	if err != nil {
		return fmt.Errorf("failed to read stdout: %w", err)
	}
	t.Result = string(stdout)

	return nil
}

func (d *PodmanRuntime) Stop(ctx context.Context, t *tork.Task) error {
	containerID, ok := d.tasks.Get(t.ID)
	if !ok {
		return nil
	}
	d.tasks.Delete(t.ID)
	log.Debug().Msgf("Attempting to stop and remove container %v", containerID)
	rmCmd := exec.CommandContext(ctx, "podman", "rm", "-f", "-t", "0", containerID)
	if err := rmCmd.Run(); err != nil {
		return fmt.Errorf("failed to stop container %s: %w", containerID, err)
	}
	return nil
}

func (d *PodmanRuntime) HealthCheck(ctx context.Context) error {
	cmd := exec.CommandContext(ctx, "podman", "info")
	if err := cmd.Run(); err != nil {
		return errors.Wrap(err, "podman is not running")
	}
	return nil
}

func (d *PodmanRuntime) reportProgress(ctx context.Context, progressFile string, t *tork.Task) {
	for {
		progress, err := d.readProgress(progressFile)
		if err != nil && !os.IsNotExist(err) {
			log.Error().Err(err).Msgf("error reading progress value")
		} else {
			if progress != t.Progress {
				t.Progress = progress
				if err := d.broker.PublishTaskProgress(ctx, t); err != nil {
					log.Error().Err(err).Msgf("error publishing task progress")
				}
			}
		}
		select {
		case <-time.After(time.Second * 5):
		case <-ctx.Done():
			return
		}
	}
}

func (d *PodmanRuntime) readProgress(progressFile string) (float64, error) {
	content, err := os.ReadFile(progressFile)
	if err != nil {
		return 0, err
	}
	if len(content) == 0 {
		return 0, nil
	}
	val := strings.TrimSpace(string(content))
	progress, err := strconv.ParseFloat(val, 32)
	if err != nil {
		return 0, err
	}
	return progress, nil
}

func (d *PodmanRuntime) imagePull(ctx context.Context, t *tork.Task, logger io.Writer) error {
	_, ok := d.images.Get(t.Image)
	if ok {
		return nil
	}
	pr := &pullRequest{
		ctx:    ctx,
		image:  t.Image,
		logger: logger,
		done:   make(chan error),
	}
	d.pullq <- pr
	err := <-pr.done
	if err == nil {
		d.images.Set(t.Image, true)
	}
	return err
}

// puller is a goroutine that serializes all requests
// to pull images from the docker repo
func (d *PodmanRuntime) puller() {
	for pr := range d.pullq {
		pr.done <- d.doPullRequest(pr)
	}
}

func (d *PodmanRuntime) doPullRequest(pr *pullRequest) error {
	// let's check if we have the image locally already
	imageExists, err := d.imageExistsLocally(pr.ctx, pr.image)
	if err != nil {
		return err
	}
	if !imageExists {
		// pull the image
		log.Debug().Msgf("Pulling image %s", pr.image)
		cmd := exec.CommandContext(pr.ctx, "podman", "pull", pr.image)
		cmd.Stdout = pr.logger
		cmd.Stderr = pr.logger
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("failed to pull image %s: %w", pr.image, err)
		}
	}
	return nil
}

func (d *PodmanRuntime) imageExistsLocally(ctx context.Context, name string) (bool, error) {
	cmd := exec.CommandContext(ctx, "podman", "inspect", name)
	if err := cmd.Run(); err != nil {
		return false, nil
	}
	return true, nil
}
