package mq

import (
	"context"
	"sync"

	"github.com/rs/zerolog/log"
	"github.com/tork/job"
	"github.com/tork/node"
	"github.com/tork/task"
)

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
		hearbeats: make(chan node.Node, 10),
		jobs:      make(chan job.Job, 10),
	}
}

func (b *InMemoryBroker) PublishTask(ctx context.Context, qname string, t task.Task) error {
	log.Debug().Msgf("publish task %s to %s queue", t.ID, qname)
	b.mu.RLock()
	q, ok := b.tasks[qname]
	b.mu.RUnlock()
	if !ok {
		b.mu.Lock()
		q = make(chan task.Task, 10)
		b.tasks[qname] = q
		b.mu.Unlock()
	}
	q <- t
	return nil
}

func (b *InMemoryBroker) SubscribeForTasks(qname string, handler func(ctx context.Context, t task.Task) error) error {
	log.Debug().Msgf("subscribing for tasks on %s", qname)
	b.mu.RLock()
	q, ok := b.tasks[qname]
	b.mu.RUnlock()
	if !ok {
		q = make(chan task.Task, 100)
		b.mu.Lock()
		b.tasks[qname] = q
		b.mu.Unlock()
	}
	go func() {
		ctx := context.TODO()
		for t := range q {
			err := handler(ctx, t)
			if err != nil {
				log.Error().Err(err).Msg("unexpcted error occured while processing task")
			}
		}
	}()
	return nil
}

func (b *InMemoryBroker) Queues(ctx context.Context) ([]QueueInfo, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	keys := make([]QueueInfo, len(b.tasks))
	i := 0
	for k := range b.tasks {
		keys[i] = QueueInfo{Name: k, Size: len(b.tasks[k])}
		i++
	}
	return keys, nil
}

func (b *InMemoryBroker) PublishHeartbeat(ctx context.Context, n node.Node) error {
	b.hearbeats <- n
	return nil
}

func (b *InMemoryBroker) SubscribeForHeartbeats(handler func(ctx context.Context, n node.Node) error) error {
	go func() {
		ctx := context.TODO()
		for n := range b.hearbeats {
			err := handler(ctx, n)
			if err != nil {
				log.Error().Err(err).Msg("unexpcted error occured while processing registration")
			}
		}
	}()
	return nil
}

func (b *InMemoryBroker) PublishJob(ctx context.Context, j job.Job) error {
	b.jobs <- j
	return nil
}
func (b *InMemoryBroker) SubscribeForJobs(handler func(ctx context.Context, j job.Job) error) error {
	go func() {
		ctx := context.TODO()
		for j := range b.jobs {
			err := handler(ctx, j)
			if err != nil {
				log.Error().Err(err).Msg("unexpcted error occured while processing job")
			}
		}
	}()
	return nil
}
