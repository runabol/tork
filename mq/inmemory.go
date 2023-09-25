package mq

import (
	"context"
	"sync/atomic"

	"sync"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"

	"github.com/runabol/tork/internal/syncx"
	"github.com/runabol/tork/internal/wildcard"
)

const defaultQueueSize = 1000

// InMemoryBroker a very simple implementation of the Broker interface
// which uses in-memory channels to exchange messages. Meant for local
// development, tests etc.
type InMemoryBroker struct {
	queues    *syncx.Map[string, *queue]
	topics    *syncx.Map[string, *topic]
	terminate *atomic.Bool
}
type topic struct {
	name       string
	ch         chan any
	subs       []func(event any)
	terminate  chan any
	terminated chan any
	mu         sync.RWMutex
}

func newTopic(name string) *topic {
	t := &topic{
		name:       name,
		ch:         make(chan any),
		terminate:  make(chan any),
		terminated: make(chan any),
	}
	go func() {
		for {
			select {
			case <-t.terminate:
				close(t.terminated)
				return
			case m := <-t.ch:
				t.mu.RLock()
				for _, sub := range t.subs {
					sub(m)
				}
				t.mu.RUnlock()
			}
		}
	}()

	return t
}

func (t *topic) subscribe(handler func(ev any)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.subs = append(t.subs, handler)
}

func (t *topic) publish(ev any) {
	t.ch <- ev
}

func (t *topic) close() {
	t.terminate <- 1
	<-t.terminated
}

type queue struct {
	name    string
	ch      chan any
	subs    []*qsub
	unacked int32
	mu      sync.Mutex
}

func newQueue(name string) *queue {
	return &queue{
		name:    name,
		ch:      make(chan any, defaultQueueSize),
		subs:    make([]*qsub, 0),
		unacked: 0,
	}
}

type qsub struct {
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
		if IsCoordinatorQueue(q.name) {
			<-sub.terminated
		}
	}
}

func (q *queue) subscribe(sub func(m any) error) {
	terminate := make(chan any)
	terminated := make(chan any)
	q.mu.Lock()
	q.subs = append(q.subs, &qsub{
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
		queues:    new(syncx.Map[string, *queue]),
		topics:    new(syncx.Map[string, *topic]),
		terminate: new(atomic.Bool),
	}
}

func (b *InMemoryBroker) PublishTask(ctx context.Context, qname string, t *tork.Task) error {
	log.Debug().Msgf("publish task %s to %s queue", t.ID, qname)
	return b.publish(qname, t.Clone())
}

func (b *InMemoryBroker) SubscribeForTasks(qname string, handler func(t *tork.Task) error) error {
	return b.subscribe(qname, func(m any) error {
		t, ok := m.(*tork.Task)
		if !ok {
			return errors.Errorf("can't cast message to task")
		}
		return handler(t.Clone())
	})
}

func (b *InMemoryBroker) subscribe(qname string, handler func(m any) error) error {
	log.Debug().Msgf("subscribing for tasks on %s", qname)
	q, ok := b.queues.Get(qname)
	if !ok {
		q = newQueue(qname)
		b.queues.Set(qname, q)
	}
	q.subscribe(handler)
	return nil
}

func (b *InMemoryBroker) Queues(ctx context.Context) ([]QueueInfo, error) {
	qi := make([]QueueInfo, 0)
	b.queues.Iterate(func(_ string, q *queue) {
		qi = append(qi, QueueInfo{
			Name:        q.name,
			Size:        q.size(),
			Subscribers: len(q.subs),
			Unacked:     int(atomic.LoadInt32(&q.unacked)),
		})
	})
	return qi, nil
}

func (b *InMemoryBroker) PublishHeartbeat(_ context.Context, n *tork.Node) error {
	return b.publish(QUEUE_HEARTBEAT, n.Clone())
}

func (b *InMemoryBroker) SubscribeForHeartbeats(handler func(n *tork.Node) error) error {
	return b.subscribe(QUEUE_HEARTBEAT, func(m any) error {
		n, ok := m.(*tork.Node)
		if !ok {
			return errors.New("can't cast to node")
		}
		return handler(n)
	})
}

func (b *InMemoryBroker) PublishJob(ctx context.Context, j *tork.Job) error {
	return b.publish(QUEUE_JOBS, j.Clone())
}

func (b *InMemoryBroker) SubscribeForJobs(handler func(j *tork.Job) error) error {
	return b.subscribe(QUEUE_JOBS, func(m any) error {
		j, ok := m.(*tork.Job)
		if !ok {
			return errors.New("can't cast to Job")
		}
		return handler(j)
	})
}

func (b *InMemoryBroker) publish(qname string, m any) error {
	q, ok := b.queues.Get(qname)
	if !ok {
		q = newQueue(qname)
		b.queues.Set(qname, q)
	}
	q.send(m)
	return nil
}

func (b *InMemoryBroker) Shutdown(ctx context.Context) error {
	if !b.terminate.CompareAndSwap(false, true) {
		return nil
	}
	done := make(chan int)
	go func() {
		b.queues.Iterate(func(_ string, q *queue) {
			log.Debug().Msgf("shutting down channel %s", q.name)
			q.close()
		})
		b.topics.Iterate(func(_ string, t *topic) {
			log.Debug().Msgf("shutting down topic %s", t.name)
			t.close()
		})
		done <- 1
	}()
	select {
	case <-ctx.Done():
	case <-done:
	}
	return nil
}

func (b *InMemoryBroker) SubscribeForEvents(ctx context.Context, topic string, handler func(event any)) error {
	log.Debug().Msgf("subscribing for events on %s", topic)
	t, ok := b.topics.Get(topic)
	if !ok {
		t = newTopic(topic)
		b.topics.Set(topic, t)
	}
	t.subscribe(handler)
	return nil
}

func (b *InMemoryBroker) PublishEvent(ctx context.Context, topicName string, event any) error {
	b.topics.Iterate(func(name string, topic *topic) {
		if wildcard.Match(name, topicName) {
			topic.publish(event)
		}
	})
	return nil
}

func (b *InMemoryBroker) HealthCheck(ctx context.Context) error {
	if b.terminate.Load() {
		return errors.New("broker is terminated")
	}
	return nil
}
