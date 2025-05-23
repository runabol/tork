package scheduler

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/pkg/errors"
	"github.com/runabol/tork"
	"github.com/runabol/tork/broker"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/internal/eval"
	"github.com/runabol/tork/internal/uuid"
)

type Scheduler struct {
	ds     datastore.Datastore
	broker broker.Broker
}

func NewScheduler(ds datastore.Datastore, b broker.Broker) *Scheduler {
	return &Scheduler{ds: ds, broker: b}
}

func (s *Scheduler) ScheduleTask(ctx context.Context, t *tork.Task) error {
	if t.Parallel != nil {
		return s.scheduleParallelTask(ctx, t)
	} else if t.Each != nil {
		return s.scheduleEachTask(ctx, t)
	} else if t.SubJob != nil {
		return s.scheduleSubJob(ctx, t)
	}
	return s.scheduleRegularTask(ctx, t)
}

func (s *Scheduler) scheduleRegularTask(ctx context.Context, t *tork.Task) error {
	now := time.Now().UTC()
	// apply job-level defaults
	job, err := s.ds.GetJobByID(ctx, t.JobID)
	if err != nil {
		return err
	}
	if job.Defaults != nil {
		if t.Queue == "" {
			t.Queue = job.Defaults.Queue
		}
		if job.Defaults.Limits != nil {
			if t.Limits == nil {
				t.Limits = &tork.TaskLimits{}
			}
			if t.Limits.CPUs == "" {
				t.Limits.CPUs = job.Defaults.Limits.CPUs
			}
			if t.Limits.Memory == "" {
				t.Limits.Memory = job.Defaults.Limits.Memory
			}
		}
		if t.Timeout == "" {
			t.Timeout = job.Defaults.Timeout
		}
		if job.Defaults.Retry != nil {
			if t.Retry == nil {
				t.Retry = &tork.TaskRetry{}
			}
			if t.Retry.Limit == 0 {
				t.Retry.Limit = job.Defaults.Retry.Limit
			}
		}
		if t.Priority == 0 {
			t.Priority = job.Defaults.Priority
		}
	}
	if t.Queue == "" {
		t.Queue = broker.QUEUE_DEFAULT
	}
	// mark task state as scheduled
	t.State = tork.TaskStateScheduled
	t.ScheduledAt = &now
	if err := s.ds.UpdateTask(ctx, t.ID, func(u *tork.Task) error {
		u.State = t.State
		u.ScheduledAt = t.ScheduledAt
		u.Queue = t.Queue
		u.Limits = t.Limits
		u.Timeout = t.Timeout
		u.Retry = t.Retry
		u.Priority = t.Priority
		return nil
	}); err != nil {
		return errors.Wrapf(err, "error updating task in datastore")
	}
	return s.broker.PublishTask(ctx, t.Queue, t)
}

func (s *Scheduler) scheduleSubJob(ctx context.Context, t *tork.Task) error {
	if t.SubJob.Detached {
		return s.scheduleDetachedSubJob(ctx, t)
	}
	return s.scheduleAttachedSubJob(ctx, t)
}

func (s *Scheduler) scheduleAttachedSubJob(ctx context.Context, t *tork.Task) error {
	job, err := s.ds.GetJobByID(ctx, t.JobID)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	subjob := &tork.Job{
		ID:          uuid.NewUUID(),
		CreatedAt:   now,
		CreatedBy:   job.CreatedBy,
		Permissions: job.Permissions,
		ParentID:    t.ID,
		Name:        t.SubJob.Name,
		Description: t.SubJob.Description,
		State:       tork.JobStatePending,
		Tasks:       t.SubJob.Tasks,
		Inputs:      t.SubJob.Inputs,
		Secrets:     t.SubJob.Secrets,
		Context: tork.JobContext{
			Inputs:  t.SubJob.Inputs,
			Secrets: t.SubJob.Secrets,
		},
		TaskCount:  len(t.SubJob.Tasks),
		Output:     t.SubJob.Output,
		Webhooks:   t.SubJob.Webhooks,
		AutoDelete: t.SubJob.AutoDelete,
	}
	if err := s.ds.UpdateTask(ctx, t.ID, func(u *tork.Task) error {
		u.State = tork.TaskStateRunning
		u.ScheduledAt = &now
		u.StartedAt = &now
		u.SubJob.ID = subjob.ID
		return nil
	}); err != nil {
		return errors.Wrapf(err, "error updating task in datastore")
	}
	if err := s.ds.CreateJob(ctx, subjob); err != nil {
		return errors.Wrapf(err, "error creating subjob")
	}
	return s.broker.PublishJob(ctx, subjob)
}

func (s *Scheduler) scheduleDetachedSubJob(ctx context.Context, t *tork.Task) error {
	job, err := s.ds.GetJobByID(ctx, t.JobID)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	subjob := &tork.Job{
		ID:          uuid.NewUUID(),
		CreatedBy:   job.CreatedBy,
		CreatedAt:   now,
		Permissions: job.Permissions,
		Name:        t.SubJob.Name,
		Description: t.SubJob.Description,
		State:       tork.JobStatePending,
		Tasks:       t.SubJob.Tasks,
		Inputs:      t.SubJob.Inputs,
		Secrets:     t.SubJob.Secrets,
		Context:     tork.JobContext{Inputs: t.SubJob.Inputs},
		TaskCount:   len(t.SubJob.Tasks),
		Output:      t.SubJob.Output,
		Webhooks:    t.SubJob.Webhooks,
		AutoDelete:  t.SubJob.AutoDelete,
	}
	if err := s.ds.CreateJob(ctx, subjob); err != nil {
		return errors.Wrapf(err, "error creating subjob")
	}
	if err := s.broker.PublishJob(ctx, subjob); err != nil {
		return errors.Wrapf(err, "error publishing subjob")
	}
	if err := s.ds.UpdateTask(ctx, t.ID, func(u *tork.Task) error {
		u.State = tork.TaskStateRunning
		u.ScheduledAt = &now
		u.StartedAt = &now
		u.SubJob.ID = subjob.ID
		return nil
	}); err != nil {
		return errors.Wrapf(err, "error updating task in datastore")
	}
	t.CompletedAt = &now
	t.State = tork.TaskStateCompleted
	return s.broker.PublishTask(ctx, broker.QUEUE_COMPLETED, t)
}

func (s *Scheduler) scheduleEachTask(ctx context.Context, t *tork.Task) error {
	now := time.Now().UTC()
	// get the job's context
	j, err := s.ds.GetJobByID(ctx, t.JobID)
	if err != nil {
		return errors.Wrapf(err, "error getting job: %s", t.JobID)
	}
	// evaluate the list expression
	lraw, err := eval.EvaluateExpr(t.Each.List, j.Context.AsMap())
	if err != nil {
		t.Error = err.Error()
		t.State = tork.TaskStateFailed
		return s.broker.PublishTask(ctx, broker.QUEUE_ERROR, t)
	}
	var list []any
	rlist := reflect.ValueOf(lraw)
	if rlist.Kind() == reflect.Slice {
		for i := 0; i < rlist.Len(); i++ {
			list = append(list, rlist.Index(i).Interface())
		}
	} else {
		t.Error = "each.list does not evaluate to a list"
		t.State = tork.TaskStateFailed
		return s.broker.PublishTask(ctx, broker.QUEUE_ERROR, t)
	}
	// mark the task as running
	if err := s.ds.UpdateTask(ctx, t.ID, func(u *tork.Task) error {
		u.State = tork.TaskStateRunning
		u.ScheduledAt = &now
		u.StartedAt = &now
		u.Each.Size = len(list)
		u.Each.Index = u.Each.Concurrency
		return nil
	}); err != nil {
		return errors.Wrapf(err, "error updating task in datastore")
	}
	for ix, item := range list {
		cx := j.Context.Clone().AsMap()
		eachVar := t.Each.Var
		if eachVar == "" {
			eachVar = "item"
		}
		cx[eachVar] = map[string]any{
			"index": fmt.Sprintf("%d", ix),
			"value": item,
		}
		et := t.Each.Task.Clone()
		et.ID = uuid.NewUUID()
		et.JobID = j.ID
		// if "concurrency" level is configured, we only create
		// the task, but we do not fire it to the broker.
		if t.Each.Concurrency == 0 || ix < t.Each.Concurrency {
			et.State = tork.TaskStatePending
		} else {
			et.State = tork.TaskStateCreated
		}
		et.Position = t.Position
		et.CreatedAt = &now
		et.ParentID = t.ID
		if err := eval.EvaluateTask(et, cx); err != nil {
			t.Error = err.Error()
			t.State = tork.TaskStateFailed
			return s.broker.PublishTask(ctx, broker.QUEUE_ERROR, t)
		}
		if err := s.ds.CreateTask(ctx, et); err != nil {
			return err
		}
		// if "concurrency" level is configured, take that into
		// account by only firing an amount of tasks upto that
		// level of concurrency.
		if t.Each.Concurrency == 0 || ix < t.Each.Concurrency {
			if err := s.broker.PublishTask(ctx, broker.QUEUE_PENDING, et); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Scheduler) scheduleParallelTask(ctx context.Context, t *tork.Task) error {
	now := time.Now().UTC()
	// mark the task as running
	if err := s.ds.UpdateTask(ctx, t.ID, func(u *tork.Task) error {
		u.State = tork.TaskStateRunning
		u.ScheduledAt = &now
		u.StartedAt = &now
		return nil
	}); err != nil {
		return errors.Wrapf(err, "error updating task in datastore")
	}
	// get the job's context
	j, err := s.ds.GetJobByID(ctx, t.JobID)
	if err != nil {
		return errors.Wrapf(err, "error getting job: %s", t.JobID)
	}
	// fire all parallel tasks
	for _, pt := range t.Parallel.Tasks {
		pt.ID = uuid.NewUUID()
		pt.JobID = j.ID
		pt.State = tork.TaskStatePending
		pt.Position = t.Position
		pt.CreatedAt = &now
		pt.ParentID = t.ID
		if err := eval.EvaluateTask(pt, j.Context.AsMap()); err != nil {
			t.Error = err.Error()
			t.State = tork.TaskStateFailed
			return s.broker.PublishTask(ctx, broker.QUEUE_ERROR, t)
		}
		if err := s.ds.CreateTask(ctx, pt); err != nil {
			return err
		}
		if err := s.broker.PublishTask(ctx, broker.QUEUE_PENDING, pt); err != nil {
			return err
		}
	}
	return nil
}
