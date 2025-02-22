package handlers

import (
	"context"
	"sync"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
	"github.com/runabol/tork/broker"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/internal/uuid"
	"github.com/runabol/tork/locker"
)

// minScheduledJobLockTTL is the minimum time a scheduled
// job lock should be held
const minScheduledJobLockTTL = 10 * time.Second

type jobSchedulerHandler struct {
	ds        datastore.Datastore
	broker    broker.Broker
	scheduler gocron.Scheduler
	mu        sync.Mutex
	m         map[string]gocron.Job
}

type glocker struct {
	locker locker.Locker
}

type glock struct {
	key       string
	lock      locker.Lock
	createdAt time.Time
}

func (l glock) Unlock(ctx context.Context) error {
	// ensure that the lock is held for at least minLockTTL
	// to mitigate drifting coordinators from re-acquiring
	// the lock
	elapsed := time.Since(l.createdAt)
	if elapsed < minScheduledJobLockTTL {
		time.Sleep(minScheduledJobLockTTL - elapsed)
	}
	log.Debug().Msgf("Releasing lock for %s", l.key)
	return l.lock.ReleaseLock(ctx)
}

func (d glocker) Lock(ctx context.Context, key string) (gocron.Lock, error) {
	lock, err := d.locker.AcquireLock(ctx, key)
	if err != nil {
		return nil, err
	}
	log.Debug().Msgf("Acquired lock for %s", key)
	return &glock{lock: lock, key: key, createdAt: time.Now()}, nil
}

func NewJobSchedulerHandler(ds datastore.Datastore, b broker.Broker, l locker.Locker) (func(ctx context.Context, s *tork.ScheduledJob) error, error) {
	sc, err := gocron.NewScheduler(gocron.WithDistributedLocker(glocker{locker: l}))
	if err != nil {
		return nil, err
	}
	sc.Start()

	h := &jobSchedulerHandler{
		ds:        ds,
		scheduler: sc,
		broker:    b,
		m:         make(map[string]gocron.Job),
	}

	// initialize all existing active jobs
	ctx := context.Background()

	activeJobs, err := ds.GetActiveScheduledJobs(ctx)
	if err != nil {
		return nil, err
	}

	for _, aj := range activeJobs {
		if err := h.handle(ctx, aj); err != nil {
			return nil, err
		}
	}

	return h.handle, nil
}

func (h *jobSchedulerHandler) handle(ctx context.Context, s *tork.ScheduledJob) error {
	switch s.State {
	case tork.ScheduledJobStateActive:
		return h.handleActive(ctx, s)
	case tork.ScheduledJobStatePaused:
		return h.handlePaused(ctx, s)
	default:
		return errors.Errorf("unknown scheduled jobs state: %s", s.State)
	}
}

func (h *jobSchedulerHandler) handleActive(ctx context.Context, s *tork.ScheduledJob) error {
	log.Info().Msgf("Scheduling job %s with cron %s", s.ID, s.Cron)
	cj, err := h.scheduler.NewJob(
		gocron.CronJob(s.Cron, false),
		gocron.NewTask(
			func(sj *tork.ScheduledJob) {
				now := time.Now().UTC()
				job := &tork.Job{
					ID:          uuid.NewUUID(),
					CreatedBy:   s.CreatedBy,
					CreatedAt:   now,
					Name:        s.Name,
					Description: s.Description,
					State:       tork.JobStatePending,
					Tasks:       s.Tasks,
					Inputs:      s.Inputs,
					Secrets:     s.Secrets,
					Context:     tork.JobContext{Inputs: s.Inputs},
					TaskCount:   len(s.Tasks),
					Output:      s.Output,
					Webhooks:    s.Webhooks,
					AutoDelete:  s.AutoDelete,
					Schedule: &tork.JobSchedule{
						ID:   s.ID,
						Cron: s.Cron,
					},
				}
				if err := h.ds.CreateJob(ctx, job); err != nil {
					log.Error().Err(err).Msgf("error creating scheduled job instance: %s", s.ID)
				}
				if err := h.broker.PublishJob(ctx, job); err != nil {
					log.Error().Err(err).Msgf("error publishing scheduled job instance: %s", s.ID)
				}
			},
			s,
		),
		gocron.WithName(s.ID),
	)
	h.mu.Lock()
	h.m[s.ID] = cj
	h.mu.Unlock()
	return err
}

func (h *jobSchedulerHandler) handlePaused(_ context.Context, s *tork.ScheduledJob) error {
	h.mu.Lock()
	gjob, ok := h.m[s.ID]
	h.mu.Unlock()
	if !ok {
		return errors.Errorf("unknown scheduled job: %s", s.ID)
	}
	log.Info().Msgf("Pausing scheduled job %s", gjob.ID())
	if err := h.scheduler.RemoveJob(gjob.ID()); err != nil {
		return err
	}
	h.mu.Lock()
	delete(h.m, s.ID)
	h.mu.Unlock()
	return nil
}
