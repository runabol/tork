package shell

import (
	"context"
	"flag"
	"io"
	"strings"

	"fmt"
	"os"
	"os/exec"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
	"github.com/runabol/tork/internal/logging"
	"github.com/runabol/tork/internal/reexec"
	"github.com/runabol/tork/internal/syncx"
	"github.com/runabol/tork/internal/uuid"
	"github.com/runabol/tork/mq"
)

type Rexec func(args ...string) *exec.Cmd

const (
	DEFAULT_UID  = "-"
	DEFAULT_GID  = "-"
	envVarPrefix = "REEXEC_"
)

func init() {
	reexec.Register("shell", reexecRun)
}

type ShellRuntime struct {
	cmds   *syncx.Map[string, *exec.Cmd]
	shell  []string
	uid    string
	gid    string
	reexec Rexec
	broker mq.Broker
}

type Config struct {
	CMD    []string
	UID    string
	GID    string
	Rexec  Rexec
	Broker mq.Broker
}

func NewShellRuntime(cfg Config) *ShellRuntime {
	if len(cfg.CMD) == 0 {
		cfg.CMD = []string{"bash", "-c"}
	}
	if cfg.Rexec == nil {
		cfg.Rexec = reexec.Command
	}
	if cfg.UID == "" {
		cfg.UID = DEFAULT_UID
	}
	if cfg.GID == "" {
		cfg.GID = DEFAULT_GID
	}
	return &ShellRuntime{
		cmds:   new(syncx.Map[string, *exec.Cmd]),
		shell:  cfg.CMD,
		uid:    cfg.UID,
		gid:    cfg.GID,
		reexec: cfg.Rexec,
		broker: cfg.Broker,
	}
}

func (r *ShellRuntime) Run(ctx context.Context, t *tork.Task) error {
	if t.ID == "" {
		return errors.New("task id is required")
	}
	if len(t.Mounts) > 0 {
		return errors.New("mounts are not supported on shell runtime")
	}
	if len(t.Entrypoint) > 0 {
		return errors.New("entrypoint is not supported on shell runtime")
	}
	if t.Image != "" {
		return errors.New("image is not supported on shell runtime")
	}
	if t.Limits != nil && (t.Limits.CPUs != "" || t.Limits.Memory != "") {
		return errors.New("limits are not supported on shell runtime")
	}
	if len(t.Networks) > 0 {
		return errors.New("networks are not supported on shell runtime")
	}
	if t.Registry != nil {
		return errors.New("registry is not supported on shell runtime")
	}
	if len(t.CMD) > 0 {
		return errors.New("cmd is not supported on shell runtime")
	}
	var logger io.Writer
	if r.broker != nil {
		logger = &logging.Forwarder{
			Broker: r.broker,
			TaskID: t.ID,
		}
	} else {
		logger = os.Stdout
	}
	// excute pre-tasks
	for _, pre := range t.Pre {
		pre.ID = uuid.NewUUID()
		if err := r.doRun(ctx, pre, logger); err != nil {
			return err
		}
	}
	// run the actual task
	if err := r.doRun(ctx, t, logger); err != nil {
		return err
	}
	// execute post tasks
	for _, post := range t.Post {
		post.ID = uuid.NewUUID()
		if err := r.doRun(ctx, post, logger); err != nil {
			return err
		}
	}
	return nil
}

func (r *ShellRuntime) doRun(ctx context.Context, t *tork.Task, logger io.Writer) error {
	defer r.cmds.Delete(t.ID)

	workdir, err := os.MkdirTemp("", "tork")
	if err != nil {
		return err
	}
	defer os.RemoveAll(workdir)

	log.Debug().Msgf("Created workdir %s", workdir)

	if err := os.WriteFile(fmt.Sprintf("%s/stdout", workdir), []byte{}, 0606); err != nil {
		return errors.Wrapf(err, "error writing the entrypoint")
	}

	for filename, contents := range t.Files {
		filename = fmt.Sprintf("%s/%s", workdir, filename)
		if err := os.WriteFile(filename, []byte(contents), 0444); err != nil {
			return errors.Wrapf(err, "error writing file: %s", filename)
		}
	}

	env := []string{}
	for name, value := range t.Env {
		env = append(env, fmt.Sprintf("%s%s=%s", envVarPrefix, name, value))
	}
	env = append(env, fmt.Sprintf("%sTORK_OUTPUT=%s/stdout", envVarPrefix, workdir))
	env = append(env, fmt.Sprintf("WORKDIR=%s", workdir))
	env = append(env, fmt.Sprintf("PATH=%s", os.Getenv("PATH")))

	if err := os.WriteFile(fmt.Sprintf("%s/entrypoint", workdir), []byte(t.Run), 0555); err != nil {
		return errors.Wrapf(err, "error writing the entrypoint")
	}
	args := append(r.shell, fmt.Sprintf("%s/entrypoint", workdir))
	args = append([]string{"shell", "-uid", r.uid, "-gid", r.gid}, args...)
	cmd := r.reexec(args...)
	cmd.Env = env
	cmd.Dir = workdir
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	defer stdout.Close()
	cmd.Stderr = cmd.Stdout

	if err := cmd.Start(); err != nil {
		return err
	}

	r.cmds.Set(t.ID, cmd)

	go func() {
		_, err := io.Copy(logger, stdout)
		if err != nil {
			log.Error().Err(err).Msgf("[shell] error logging stdout")
		}
	}()

	errChan := make(chan error)
	doneChan := make(chan any)
	go func() {
		if err := cmd.Wait(); err != nil {
			errChan <- err
			return
		}
		close(doneChan)
	}()
	select {
	case err := <-errChan:
		return errors.Wrapf(err, "error executing command")
	case <-ctx.Done():
		if err := cmd.Process.Kill(); err != nil {
			return errors.Wrapf(err, "error cancelling command")
		}
		return ctx.Err()
	case <-doneChan:
	}

	output, err := os.ReadFile(fmt.Sprintf("%s/stdout", workdir))
	if err != nil {
		return errors.Wrapf(err, "error reading the task output")
	}

	t.Result = string(output)

	return nil
}

func reexecRun() {
	var uid string
	var gid string
	flag.StringVar(&uid, "uid", "", "the uid to use when running the process")
	flag.StringVar(&gid, "gid", "", "the gid to use when running the process")
	flag.Parse()

	SetUID(uid)
	SetGID(gid)

	workdir := os.Getenv("WORKDIR")
	if workdir == "" {
		log.Fatal().Msg("work dir not set")
	}

	env := []string{}
	for _, entry := range os.Environ() {
		kv := strings.Split(entry, "=")
		if len(kv) != 2 {
			log.Fatal().Msgf("invalid env var: %s", entry)
		}
		if strings.HasPrefix(kv[0], envVarPrefix) {
			k := strings.TrimPrefix(kv[0], envVarPrefix)
			v := kv[1]
			env = append(env, fmt.Sprintf("%s=%s", k, v))
		}
	}

	cmd := exec.Command(flag.Args()[0], flag.Args()[1:]...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = env
	cmd.Dir = workdir

	if err := cmd.Run(); err != nil {
		log.Fatal().Err(err).Msgf("error reexecing: %s", strings.Join(flag.Args(), " "))
	}
}

func (r *ShellRuntime) Stop(ctx context.Context, t *tork.Task) error {
	proc, ok := r.cmds.Get(t.ID)
	if !ok {
		return nil
	}
	if err := proc.Process.Kill(); err != nil {
		return errors.Wrapf(err, "error stopping process for task: %s", t.ID)
	}
	return nil
}

func (r *ShellRuntime) HealthCheck(ctx context.Context) error {
	return nil
}
