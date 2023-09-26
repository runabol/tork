package engine

import (
	"github.com/pkg/errors"
	"github.com/runabol/tork/conf"
	"github.com/runabol/tork/internal/runtime"
	"github.com/runabol/tork/internal/runtime/docker"
	"github.com/runabol/tork/internal/runtime/shell"
	"github.com/runabol/tork/internal/worker"
	"github.com/runabol/tork/mount"
)

func (e *Engine) initWorker() error {
	// init the runtime
	rt, err := initRuntime()
	if err != nil {
		return err
	}

	// register bind mounter
	bm := mount.NewBindMounter(mount.BindConfig{
		Allowed:   conf.Bool("mounts.bind.allowed"),
		Allowlist: conf.Strings("mounts.bind.allowlist"),
		Denylist:  conf.Strings("mounts.bind.denylist"),
	})
	e.mounter.RegisterMounter("bind", bm)

	// register volume mounter
	vm, err := mount.NewVolumeMounter()
	if err != nil {
		return err
	}
	e.mounter.RegisterMounter("volume", vm)

	w, err := worker.NewWorker(worker.Config{
		Broker:  e.broker,
		Runtime: rt,
		Queues:  conf.IntMap("worker.queues"),
		Limits: worker.Limits{
			DefaultCPUsLimit:   conf.String("worker.limits.cpus"),
			DefaultMemoryLimit: conf.String("worker.limits.memory"),
		},
		Address:    conf.String("worker.address"),
		Mounter:    e.mounter,
		Middleware: e.cfg.Middleware.Task,
	})
	if err != nil {
		return errors.Wrapf(err, "error creating worker")
	}
	if err := w.Start(); err != nil {
		return err
	}
	e.worker = w
	return nil
}

func initRuntime() (runtime.Runtime, error) {
	runtimeType := conf.StringDefault("runtime.type", "docker")
	switch runtimeType {
	case "docker":
		return docker.NewDockerRuntime()
	case "shell":
		return shell.NewShellRuntime(shell.Config{
			CMD: conf.Strings("runtime.shell.cmd"),
			UID: conf.StringDefault("runtime.shell.uid", shell.DEFAULT_UID),
			GID: conf.StringDefault("runtime.shell.gid", shell.DEFAULT_GID),
		}), nil
	default:
		return nil, errors.Errorf("unknown runtime type: %s", runtimeType)
	}
}
