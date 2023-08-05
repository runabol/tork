package scheduler

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tork/broker"
	"github.com/tork/task"
	"github.com/tork/uuid"
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

func (s *NaiveScheduler) Schedule(ctx context.Context, t *task.Task) error {
	log.Info().Any("task", t).Msg("scheduling task")
	t.ID = uuid.NewUUID()
	n := time.Now()
	t.ScheduledAt = &n
	t.State = task.Scheduled
	return s.broker.Send(ctx, s.qname, *t)
}
