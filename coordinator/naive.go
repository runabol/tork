package coordinator

import (
	"context"

	"github.com/rs/zerolog/log"
	"github.com/tork/broker"
	"github.com/tork/task"
)

type NaiveScheduler struct {
	broker broker.Broker
	qname  string
}

func NewNaiveScheduler(qname string, b broker.Broker) *NaiveScheduler {
	return &NaiveScheduler{
		broker: b,
		qname:  qname,
	}
}

func (s *NaiveScheduler) Schedule(ctx context.Context, t task.Task) error {
	log.Info().Any("task", t).Msg("scheduling task")
	return s.broker.Enqueue(ctx, s.qname, t)
}
