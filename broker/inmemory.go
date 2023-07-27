package broker

import (
	"context"
	"sync"

	"github.com/rs/zerolog/log"

	"github.com/tork/task"
)

// InMemoryBroker a very simple implementation of the Broker interface
// which uses in-memory channels to exchange messages. Meant for local
// development, tests etc.
type InMemoryBroker struct {
	queues map[string]chan task.Task
	mu     sync.RWMutex
}

func NewInMemoryBroker() *InMemoryBroker {
	return &InMemoryBroker{
		queues: make(map[string]chan task.Task),
	}
}

func (b *InMemoryBroker) Send(ctx context.Context, qname string, t task.Task) error {
	log.Debug().Msgf("sending task %s to %s", t.ID, qname)
	b.mu.RLock()
	defer b.mu.RUnlock()
	q, ok := b.queues[qname]
	if !ok {
		return ErrUnknownQueue
	}
	q <- t
	return nil
}

func (b *InMemoryBroker) Receive(qname string, handler func(ctx context.Context, t task.Task) error) error {
	log.Debug().Msgf("subscribing for tasks on %s", qname)
	b.mu.Lock()
	defer b.mu.Unlock()
	q, ok := b.queues[qname]
	if !ok {
		q = make(chan task.Task, 10)
		b.queues[qname] = q
	}
	go func() {
		ctx := context.TODO()
		for t := range q {
			err := handler(ctx, t)
			if err != nil {
				panic(err)
			}
		}
	}()
	return nil
}
