package mq

import (
	"context"
	"encoding/json"

	"sync"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/tork/job"
	"github.com/tork/node"
	"github.com/tork/task"
)

const defaultQueueSize = 10

// InMemoryBroker a very simple implementation of the Broker interface
// which uses in-memory channels to exchange messages. Meant for local
// development, tests etc.
type InMemoryBroker struct {
	tasks     map[string]chan task.Task
	hearbeats chan node.Node
	jobs      chan job.Job
	mu        sync.RWMutex
}

func NewInMemoryBroker() *InMemoryBroker {
	return &InMemoryBroker{
		tasks:     make(map[string]chan task.Task),
		hearbeats: make(chan node.Node, defaultQueueSize),
		jobs:      make(chan job.Job, defaultQueueSize),
	}
}

func (b *InMemoryBroker) PublishTask(ctx context.Context, qname string, t task.Task) error {
	log.Debug().Msgf("publish task %s to %s queue", t.ID, qname)
	b.mu.RLock()
	q, ok := b.tasks[qname]
	b.mu.RUnlock()
	if !ok {
		b.mu.Lock()
		q = make(chan task.Task, defaultQueueSize)
		b.tasks[qname] = q
		b.mu.Unlock()
	}
	// copy the task to prevent unintended side-effects
	// between the coordinator and worker
	bs, err := json.Marshal(t)
	if err != nil {
		return errors.Wrapf(err, "error marshalling task")
	}
	copy := task.Task{}
	if err := json.Unmarshal(bs, &copy); err != nil {
		return errors.Wrapf(err, "error marshalling task")
	}
	q <- copy
	return nil
}

func (b *InMemoryBroker) SubscribeForTasks(qname string, handler func(t task.Task) error) error {
	log.Debug().Msgf("subscribing for tasks on %s", qname)
	b.mu.RLock()
	q, ok := b.tasks[qname]
	b.mu.RUnlock()
	if !ok {
		q = make(chan task.Task, defaultQueueSize)
		b.mu.Lock()
		b.tasks[qname] = q
		b.mu.Unlock()
	}
	go func() {
		for t := range q {
			err := handler(t)
			if err != nil {
				log.Error().Err(err).Msg("unexpcted error occured while processing task")
			}
		}
	}()
	return nil
}

func (b *InMemoryBroker) Queue(ctx context.Context, qname string) (QueueInfo, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	switch qname {
	case QUEUE_JOBS:
		return QueueInfo{Name: QUEUE_JOBS, Size: len(b.jobs)}, nil
	case QUEUE_HEARBEAT:
		return QueueInfo{Name: QUEUE_HEARBEAT, Size: len(b.hearbeats)}, nil
	default:
		q, ok := b.tasks[qname]
		if !ok {
			return QueueInfo{}, errors.Errorf("unknown queue: %s", qname)
		}
		return QueueInfo{Name: qname, Size: len(q)}, nil
	}
}

func (b *InMemoryBroker) Queues(ctx context.Context) ([]QueueInfo, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	qi := make([]QueueInfo, 0)
	for k := range b.tasks {
		qi = append(qi, QueueInfo{Name: k, Size: len(b.tasks[k])})

	}
	qi = append(qi, QueueInfo{Name: QUEUE_JOBS, Size: len(b.jobs)})
	qi = append(qi, QueueInfo{Name: QUEUE_HEARBEAT, Size: len(b.hearbeats)})
	return qi, nil
}

func (b *InMemoryBroker) PublishHeartbeat(_ context.Context, n node.Node) error {
	b.hearbeats <- n
	return nil
}

func (b *InMemoryBroker) SubscribeForHeartbeats(handler func(n node.Node) error) error {
	go func() {
		for n := range b.hearbeats {
			err := handler(n)
			if err != nil {
				log.Error().Err(err).Msg("unexpcted error occured while processing registration")
			}
		}
	}()
	return nil
}

func (b *InMemoryBroker) PublishJob(ctx context.Context, j job.Job) error {
	// make a copy of the job to prevent unintended side-effects
	bs, err := json.Marshal(j)
	if err != nil {
		return errors.Wrapf(err, "error marshalling job")
	}
	copy := job.Job{}
	if err := json.Unmarshal(bs, &copy); err != nil {
		return errors.Wrapf(err, "error unmarshalling job")
	}
	b.jobs <- copy
	return nil
}
func (b *InMemoryBroker) SubscribeForJobs(handler func(j job.Job) error) error {
	go func() {
		for j := range b.jobs {
			err := handler(j)
			if err != nil {
				log.Error().Err(err).Msg("unexpcted error occured while processing job")
			}
		}
	}()
	return nil
}
