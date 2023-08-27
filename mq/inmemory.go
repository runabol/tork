package mq

import (
	"context"

	"sync"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork/job"
	"github.com/runabol/tork/node"
	"github.com/runabol/tork/task"
)

const defaultQueueSize = 10

// InMemoryBroker a very simple implementation of the Broker interface
// which uses in-memory channels to exchange messages. Meant for local
// development, tests etc.
type InMemoryBroker struct {
	queues      map[string]chan any
	subscribers map[string][]func(m any) error
	unacked     map[string]int
	nextSub     map[string]int
	mu          sync.RWMutex
}

func NewInMemoryBroker() *InMemoryBroker {
	return &InMemoryBroker{
		queues:      make(map[string]chan any),
		subscribers: make(map[string][]func(m any) error),
		nextSub:     make(map[string]int),
		unacked:     make(map[string]int),
	}
}

func (b *InMemoryBroker) PublishTask(ctx context.Context, qname string, t *task.Task) error {
	log.Debug().Msgf("publish task %s to %s queue", t.ID, qname)
	return b.publish(qname, t.Clone())
}

func (b *InMemoryBroker) SubscribeForTasks(qname string, handler func(t *task.Task) error) error {
	return b.subscribe(qname, func(m any) error {
		t, ok := m.(*task.Task)
		if !ok {
			return errors.Errorf("can't cast message to task")
		}
		return handler(t.Clone())
	})
}

func (b *InMemoryBroker) subscribe(qname string, handler func(m any) error) error {
	log.Debug().Msgf("subscribing for tasks on %s", qname)
	b.mu.Lock()
	defer b.mu.Unlock()
	q, ok := b.queues[qname]
	if !ok {
		q = make(chan any, defaultQueueSize)
		b.queues[qname] = q
	}
	subs, ok := b.subscribers[qname]
	subs = append(subs, handler)
	b.subscribers[qname] = subs
	if !ok {
		go func() {
			for m := range q {
				b.mu.Lock()
				// round-robin to next subscriber
				subs := b.subscribers[qname]
				nextSub := b.nextSub[qname]
				sub := subs[nextSub]
				nextSub = nextSub + 1
				if nextSub >= len(subs) {
					nextSub = 0
				}
				b.nextSub[qname] = nextSub
				b.unacked[qname] = b.unacked[qname] + 1
				b.mu.Unlock()
				if err := sub(m); err != nil {
					log.Error().Err(err).Msg("unexpcted error occurred while processing task")
				}
				b.mu.Lock()
				b.unacked[qname] = b.unacked[qname] - 1
				b.mu.Unlock()
			}
		}()
	}
	return nil
}

func (b *InMemoryBroker) Queues(ctx context.Context) ([]QueueInfo, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	qi := make([]QueueInfo, 0)
	for k := range b.queues {
		qi = append(qi, QueueInfo{
			Name:        k,
			Size:        len(b.queues[k]),
			Subscribers: len(b.subscribers[k]),
			Unacked:     b.unacked[k],
		})

	}
	return qi, nil
}

func (b *InMemoryBroker) PublishHeartbeat(_ context.Context, n node.Node) error {
	return b.publish(QUEUE_HEARBEAT, n)
}

func (b *InMemoryBroker) SubscribeForHeartbeats(handler func(n node.Node) error) error {
	return b.subscribe(QUEUE_HEARBEAT, func(m any) error {
		n, ok := m.(node.Node)
		if !ok {
			return errors.New("can't cast to node")
		}
		return handler(n)
	})
}

func (b *InMemoryBroker) PublishJob(ctx context.Context, j *job.Job) error {
	return b.publish(QUEUE_JOBS, j.Clone())
}

func (b *InMemoryBroker) SubscribeForJobs(handler func(j *job.Job) error) error {
	return b.subscribe(QUEUE_JOBS, func(m any) error {
		j, ok := m.(*job.Job)
		if !ok {
			return errors.New("can't cast to Job")
		}
		return handler(j)
	})
}

func (b *InMemoryBroker) publish(qname string, m any) error {
	b.mu.RLock()
	q, ok := b.queues[qname]
	b.mu.RUnlock()
	if !ok {
		b.mu.Lock()
		q = make(chan any, defaultQueueSize)
		b.queues[qname] = q
		b.mu.Unlock()
	}
	q <- m
	return nil
}
