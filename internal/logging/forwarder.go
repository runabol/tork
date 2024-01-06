package logging

import (
	"context"

	"github.com/runabol/tork"
	"github.com/runabol/tork/mq"
)

type Forwarder struct {
	Broker mq.Broker
	TaskID string
	part   int
}

func (r *Forwarder) Write(p []byte) (int, error) {
	r.part = r.part + 1
	if err := r.Broker.PublishTaskLogPart(context.Background(), &tork.TaskLogPart{
		Number:   r.part,
		TaskID:   r.TaskID,
		Contents: string(p),
	}); err != nil {
		return 0, err
	}
	return len(p), nil
}
