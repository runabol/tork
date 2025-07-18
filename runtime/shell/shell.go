package shell

import (
	"context"
	"flag"
	"io"
	"strconv"
	"strings"
	"time"

	"fmt"
	"os"
	"os/exec"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
	"github.com/runabol/tork/broker"
	"github.com/runabol/tork/internal/fns"
	"github.com/runabol/tork/internal/logging"
	"github.com/runabol/tork/internal/reexec"
	"github.com/runabol/tork/internal/syncx"
	"github.com/runabol/tork/internal/uuid"
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
	broker broker.Broker
}

type Config struct {
	CMD    []string
	UID    string
	GID    string
	Rexec  Rexec
	Broker broker.Broker
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
	if len(t.Sidecars) > 0 {
		return errors.New("sidecars are not supported on shell runtime")
	}
	var logger io.Writer
	if r.broker != nil {
		logger = io.MultiWriter(
			broker.NewLogShipper(r.broker, t.ID),
			logging.NewZerologWriter(t.ID, zerolog.DebugLevel),
		)
	} else {
		logger = logging.NewZerologWriter(t.ID, zerolog.DebugLevel)
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
	defer func() {
		if err := os.RemoveAll(workdir); err != nil {
			log.Error().Err(err).Msgf("error removing workdir %s", workdir)
		}
	}()

	log.Debug().Msgf("Created workdir %s", workdir)

	if err := os.WriteFile(fmt.Sprintf("%s/stdout", workdir), []byte{}, 0606); err != nil {
		return errors.Wrapf(err, "error writing the entrypoint")
	}

	if err := os.WriteFile(fmt.Sprintf("%s/progress", workdir), []byte{}, 0606); err != nil {
		return errors.Wrapf(err, "error writing the progress file")
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
	env = append(env, fmt.Sprintf("%sTORK_PROGRESS=%s/progress", envVarPrefix, workdir))
	env = append(env, fmt.Sprintf("WORKDIR=%s", workdir))
	env = append(env, fmt.Sprintf("PATH=%s", os.Getenv("PATH")))
	env = append(env, fmt.Sprintf("HOME=%s", os.Getenv("HOME")))

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
	defer fns.CloseIgnore(stdout)
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

	pctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func(ctx context.Context) {
		for {
			progress, err := r.readProgress(workdir)
			if err != nil {
				if os.IsNotExist(err) {
					return // progress file does not exist
				}
				log.Warn().Err(err).Msgf("error reading progress value")
			} else {
				if progress != t.Progress {
					t.Progress = progress
					if err := r.broker.PublishTaskProgress(ctx, t); err != nil {
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
	}(pctx)

	errCh := make(chan error, 1)
	doneCh := make(chan any)

	go func() {
		if err := cmd.Wait(); err != nil {
			errCh <- err
			return
		}
		close(doneCh)
	}()
	select {
	case err := <-errCh:
		return errors.Wrapf(err, "error executing command")
	case <-ctx.Done():
		if err := cmd.Process.Kill(); err != nil {
			return errors.Wrapf(err, "error cancelling command")
		}
		return ctx.Err()
	case <-doneCh:
	}

	output, err := os.ReadFile(fmt.Sprintf("%s/stdout", workdir))
	if err != nil {
		return errors.Wrapf(err, "error reading the task output")
	}

	t.Result = string(output)

	return nil
}

func (r *ShellRuntime) readProgress(workdir string) (float64, error) {
	b, err := os.ReadFile(fmt.Sprintf("%s/progress", workdir))
	if err != nil {
		return 0, err
	}
	s := strings.TrimSpace(string(b))
	if s == "" {
		return 0, nil
	}
	return strconv.ParseFloat(s, 32)
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

	env, err := buildEnv()
	if err != nil {
		log.Fatal().Err(err).Msg("error building env")
	}
	env = append(env, fmt.Sprintf("WORKDIR=%s", workdir))
	env = append(env, fmt.Sprintf("PATH=%s", os.Getenv("PATH")))
	env = append(env, fmt.Sprintf("HOME=%s", os.Getenv("HOME")))

	cmd := exec.Command(flag.Args()[0], flag.Args()[1:]...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = env
	cmd.Dir = workdir

	if err := cmd.Run(); err != nil {
		log.Fatal().Err(err).Msgf("error reexecing: %s", strings.Join(flag.Args(), " "))
	}
}

func buildEnv() ([]string, error) {
	env := []string{}
	for _, entry := range os.Environ() {
		kv := strings.SplitN(entry, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid env var: %s", entry)
		}
		if strings.HasPrefix(kv[0], envVarPrefix) {
			k := strings.TrimPrefix(kv[0], envVarPrefix)
			v := kv[1]
			env = append(env, fmt.Sprintf("%s=%s", k, v))
		}
	}
	return env, nil
}

func (r *ShellRuntime) HealthCheck(ctx context.Context) error {
	return nil
}
