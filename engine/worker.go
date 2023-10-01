package engine

import (
	"github.com/pkg/errors"
	"github.com/runabol/tork/conf"
	"github.com/runabol/tork/internal/worker"

	"github.com/runabol/tork/runtime"
	"github.com/runabol/tork/runtime/docker"
	"github.com/runabol/tork/runtime/shell"
)

func (e *Engine) initWorker() error {
	// init the runtime
	rt, err := e.initRuntime()
	if err != nil {
		return err
	}
	w, err := worker.NewWorker(worker.Config{
		Broker:  e.broker,
		Runtime: rt,
		Queues:  conf.IntMap("worker.queues"),
		Limits: worker.Limits{
			DefaultCPUsLimit:   conf.String("worker.limits.cpus"),
			DefaultMemoryLimit: conf.String("worker.limits.memory"),
		},
		Address:    conf.String("worker.address"),
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

func (e *Engine) initRuntime() (runtime.Runtime, error) {
	if e.runtime != nil {
		return e.runtime, nil
	}
	runtimeType := conf.StringDefault("runtime.type", runtime.Docker)
	switch runtimeType {
	case runtime.Docker:
		mounter, ok := e.mounters[runtime.Docker]
		if !ok {
			mounter = runtime.NewMultiMounter()
		}
		// register bind mounter
		bm := docker.NewBindMounter(docker.BindConfig{
			Allowed:   conf.Bool("mounts.bind.allowed"),
			Allowlist: conf.Strings("mounts.bind.allowlist"),
			Denylist:  conf.Strings("mounts.bind.denylist"),
		})
		mounter.RegisterMounter("bind", bm)
		// register volume mounter
		vm, err := docker.NewVolumeMounter()
		if err != nil {
			return nil, err
		}
		mounter.RegisterMounter("volume", vm)
		return docker.NewDockerRuntime(docker.WithMounter(mounter))
	case runtime.Shell:
		return shell.NewShellRuntime(shell.Config{
			CMD: conf.Strings("runtime.shell.cmd"),
			UID: conf.StringDefault("runtime.shell.uid", shell.DEFAULT_UID),
			GID: conf.StringDefault("runtime.shell.gid", shell.DEFAULT_GID),
		}), nil
	default:
		return nil, errors.Errorf("unknown runtime type: %s", runtimeType)
	}
}
