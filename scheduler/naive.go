package scheduler

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/tork/broker"
	"github.com/tork/task"
	"github.com/tork/uuid"
)

type NaiveScheduler struct {
	broker broker.Broker
}

func NewNaiveScheduler(b broker.Broker) *NaiveScheduler {
	return &NaiveScheduler{
		broker: b,
	}
}

func (s *NaiveScheduler) Schedule(ctx context.Context, t *task.Task) error {
	log.Info().Any("task", t).Msg("scheduling task")

	t.ID = uuid.NewUUID()

	n := time.Now()

	t.ScheduledAt = &n

	for _, q := range s.broker.Queues(ctx) {
		if strings.HasPrefix(q, "worker-") {
			t.State = task.Scheduled
			return s.broker.Send(ctx, q, *t)
		}
	}

	return errors.New("no workers found")
}
