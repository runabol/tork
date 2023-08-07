package coordinator

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tork/mq"
	"github.com/tork/task"
)

type SimpleScheduler struct {
	broker mq.Broker
}

func NewSimpleScheduler(b mq.Broker) *SimpleScheduler {
	return &SimpleScheduler{
		broker: b,
	}
}

func (s *SimpleScheduler) Schedule(ctx context.Context, t *task.Task) error {
	log.Info().Str("task-id", t.ID).Msg("scheduling task")
	qname := t.Queue
	if qname == "" {
		qname = mq.QUEUE_DEFAULT
	}
	t.State = task.Scheduled
	n := time.Now()
	t.ScheduledAt = &n
	t.State = task.Scheduled
	return s.broker.Publish(ctx, qname, t)
}
