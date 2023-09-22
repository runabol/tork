package engine

import (
	"github.com/pkg/errors"
	"github.com/runabol/tork/conf"
	"github.com/runabol/tork/internal/worker"
	"github.com/runabol/tork/mount"
	"github.com/runabol/tork/runtime"
)

func (e *Engine) initWorker() error {
	queues := conf.IntMap("worker.queues")
	rt, err := runtime.NewDockerRuntime()
	if err != nil {
		return err
	}

	mounter, err := mount.NewMounter(mount.Config{
		Bind: mount.BindConfig{
			Allowed:   conf.Bool("mounts.bind.allowed"),
			Allowlist: conf.Strings("mounts.bind.allowlist"),
			Denylist:  conf.Strings("mounts.bind.denylist"),
		},
	})
	if err != nil {
		return errors.Wrapf(err, "error initializing mounter")
	}

	w, err := worker.NewWorker(worker.Config{
		Broker:  e.broker,
		Runtime: rt,
		Queues:  queues,
		Limits: worker.Limits{
			DefaultCPUsLimit:   conf.String("worker.limits.cpus"),
			DefaultMemoryLimit: conf.String("worker.limits.memory"),
		},
		TempDir: conf.String("worker.tempdir"),
		Address: conf.String("worker.address"),
		Mounter: mounter,
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
