package engine

import (
	"github.com/pkg/errors"
	"github.com/runabol/tork/conf"
	"github.com/runabol/tork/internal/worker"
	"github.com/runabol/tork/middleware/task"

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
	// register host env middleware
	hostenv, err := task.NewHostEnv(conf.Strings("middleware.task.hostenv.vars")...)
	if err != nil {
		return err
	}
	e.cfg.Middleware.Task = append(e.cfg.Middleware.Task, hostenv.Execute)
	w, err := worker.NewWorker(worker.Config{
		Name:    conf.StringDefault("worker.name", "Worker"),
		Broker:  e.broker,
		Runtime: rt,
		Queues:  conf.IntMap("worker.queues"),
		Limits: worker.Limits{
			DefaultCPUsLimit:   conf.String("worker.limits.cpus"),
			DefaultMemoryLimit: conf.String("worker.limits.memory"),
			DefaultTimeout:     conf.String("worker.limits.timeout"),
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
			Allowed: conf.Bool("mounts.bind.allowed"),
		})
		mounter.RegisterMounter("bind", bm)
		// register volume mounter
		vm, err := docker.NewVolumeMounter()
		if err != nil {
			return nil, err
		}
		mounter.RegisterMounter("volume", vm)
		// register tmpfs mounter
		mounter.RegisterMounter("tmpfs", docker.NewTmpfsMounter())
		return docker.NewDockerRuntime(
			docker.WithMounter(mounter),
			docker.WithConfig(conf.String("runtime.docker.config")),
			docker.WithBroker(e.broker),
		)
	case runtime.Shell:
		return shell.NewShellRuntime(shell.Config{
			CMD:    conf.Strings("runtime.shell.cmd"),
			UID:    conf.StringDefault("runtime.shell.uid", shell.DEFAULT_UID),
			GID:    conf.StringDefault("runtime.shell.gid", shell.DEFAULT_GID),
			Broker: e.broker,
		}), nil
	default:
		return nil, errors.Errorf("unknown runtime type: %s", runtimeType)
	}
}
