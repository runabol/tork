package coordinator

import (
	"context"

	"github.com/rs/zerolog/log"
	"github.com/tork/broker"
	"github.com/tork/task"
)

type NaiveScheduler struct {
	broker broker.Broker
}

func NewNaiveScheduler(b broker.Broker) *NaiveScheduler {
	return &NaiveScheduler{
		broker: b,
	}
}

func (s *NaiveScheduler) Schedule(ctx context.Context, t task.Task) error {
	log.Info().Any("task", t).Msg("scheduling task")
	qname := t.Queue
	if qname == "" {
		qname = broker.QUEUE_DEFAULT
	}
	return s.broker.Enqueue(ctx, qname, t)
}
