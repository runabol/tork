package engine

import (
	"github.com/pkg/errors"
	"github.com/runabol/tork/internal/worker"
	"github.com/runabol/tork/mq"
	"github.com/runabol/tork/pkg/conf"
	"github.com/runabol/tork/runtime"
)

func createWorker(b mq.Broker) (*worker.Worker, error) {
	queues := conf.IntMap("worker.queues")
	rt, err := runtime.NewDockerRuntime()
	if err != nil {
		return nil, err
	}
	w, err := worker.NewWorker(worker.Config{
		Broker:  b,
		Runtime: rt,
		Queues:  queues,
		Limits: worker.Limits{
			DefaultCPUsLimit:   conf.String("worker.limits.cpus"),
			DefaultMemoryLimit: conf.String("worker.limits.memory"),
		},
		TempDir: conf.String("worker.tempdir"),
		Address: conf.String("worker.address"),
	})
	if err != nil {
		return nil, errors.Wrapf(err, "error creating worker")
	}
	if err := w.Start(); err != nil {
		return nil, err
	}
	return w, nil
}
