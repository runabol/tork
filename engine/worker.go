package engine

import (
	"github.com/pkg/errors"
	"github.com/runabol/tork/conf"
	"github.com/runabol/tork/internal/worker"
	"github.com/runabol/tork/runtime"
)

func (e *Engine) initWorker() error {
	queues := conf.IntMap("worker.queues")
	rt, err := runtime.NewDockerRuntime()
	if err != nil {
		return err
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
		BindMounts: worker.Mounts{
			Allowed:   conf.Bool("worker.mounts.bind.allowed"),
			Allowlist: conf.Strings("worker.mounts.bind.allowlist"),
			Denylist:  conf.Strings("worker.mounts.bind.denylist"),
		},
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
