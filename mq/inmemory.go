package mq

import (
	"context"
	"sync/atomic"

	"sync"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork/job"
	"github.com/runabol/tork/node"
	"github.com/runabol/tork/task"
)

const defaultQueueSize = 1000

// InMemoryBroker a very simple implementation of the Broker interface
// which uses in-memory channels to exchange messages. Meant for local
// development, tests etc.
type InMemoryBroker struct {
	queues       map[string]*queue
	mu           sync.RWMutex
	shuttingDown bool
}

type queue struct {
	name    string
	ch      chan any
	subs    []*subscriber
	unacked int32
	mu      sync.Mutex
}

func newQueue(name string) *queue {
	return &queue{
		name:    name,
		ch:      make(chan any, defaultQueueSize),
		subs:    make([]*subscriber, 0),
		unacked: 0,
	}
}

type subscriber struct {
	terminate  chan any
	terminated chan any
}

func (q *queue) send(m any) {
	q.ch <- m
}

func (q *queue) size() int {
	return len(q.ch)
}

func (q *queue) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, sub := range q.subs {
		close(sub.terminate)
		<-sub.terminated
	}
}

func (q *queue) subscribe(sub func(m any) error) {
	terminate := make(chan any)
	terminated := make(chan any)
	q.mu.Lock()
	q.subs = append(q.subs, &subscriber{
		terminate:  terminate,
		terminated: terminated,
	})
	q.mu.Unlock()
	go func() {
		for {
			select {
			case <-terminate:
				close(terminated)
				return
			case m := <-q.ch:
				atomic.AddInt32(&q.unacked, 1)
				if err := sub(m); err != nil {
					log.Error().
						Err(err).
						Msg("unexpcted error occurred while processing task")
				}
				atomic.AddInt32(&q.unacked, -1)
			}
		}

	}()
}

func NewInMemoryBroker() *InMemoryBroker {
	return &InMemoryBroker{
		queues: make(map[string]*queue),
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
	b.mu.Lock()
	defer b.mu.Unlock()
	log.Debug().Msgf("subscribing for tasks on %s", qname)
	q, ok := b.queues[qname]
	if !ok {
		q = newQueue(qname)
		b.queues[qname] = q
	}
	q.subscribe(handler)
	return nil
}

func (b *InMemoryBroker) Queues(ctx context.Context) ([]QueueInfo, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	qi := make([]QueueInfo, 0)
	for k := range b.queues {
		q := b.queues[k]
		qi = append(qi, QueueInfo{
			Name:        k,
			Size:        q.size(),
			Subscribers: len(q.subs),
			Unacked:     int(atomic.LoadInt32(&q.unacked)),
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
		q = newQueue(qname)
		b.mu.Lock()
		b.queues[qname] = q
		b.mu.Unlock()
	}
	q.send(m)
	return nil
}

func (b *InMemoryBroker) isShuttingDown() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.shuttingDown
}

func (b *InMemoryBroker) Shutdown(ctx context.Context) error {
	if b.isShuttingDown() {
		return nil
	}
	b.mu.Lock()
	b.shuttingDown = true
	b.mu.Unlock()
	done := make(chan int)
	go func() {
		for _, q := range b.queues {
			log.Debug().Msgf("closing %s", q.name)
			q.close()
		}
		done <- 1
	}()
	select {
	case <-ctx.Done():
	case <-done:
	}
	return nil
}
