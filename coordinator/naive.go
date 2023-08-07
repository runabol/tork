package coordinator

import (
	"context"

	"github.com/rs/zerolog/log"
	"github.com/tork/mq"
	"github.com/tork/task"
)

type NaiveScheduler struct {
	broker mq.Broker
}

func NewNaiveScheduler(b mq.Broker) *NaiveScheduler {
	return &NaiveScheduler{
		broker: b,
	}
}

func (s *NaiveScheduler) Schedule(ctx context.Context, t task.Task) error {
	log.Info().Any("task", t).Msg("scheduling task")
	qname := t.Queue
	if qname == "" {
		qname = mq.QUEUE_DEFAULT
	}
	return s.broker.Enqueue(ctx, qname, t)
}
