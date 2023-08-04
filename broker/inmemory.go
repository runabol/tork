package broker

import (
	"context"
	"sync"

	"github.com/rs/zerolog/log"
)

// InMemoryBroker a very simple implementation of the Broker interface
// which uses in-memory channels to exchange messages. Meant for local
// development, tests etc.
type InMemoryBroker struct {
	queues map[string]chan any
	mu     sync.RWMutex
}

func NewInMemoryBroker() *InMemoryBroker {
	return &InMemoryBroker{
		queues: make(map[string]chan any),
	}
}

func (b *InMemoryBroker) Send(ctx context.Context, qname string, msg any) error {
	log.Debug().Msgf("sending msg %v to %s", msg, qname)
	b.mu.RLock()
	q, ok := b.queues[qname]
	b.mu.RUnlock()
	if !ok {
		b.mu.Lock()
		q = make(chan any, 10)
		b.queues[qname] = q
		b.mu.Unlock()
	}
	q <- msg
	return nil
}

func (b *InMemoryBroker) Receive(qname string, handler func(ctx context.Context, msg any) error) error {
	log.Debug().Msgf("subscribing for tasks on %s", qname)
	b.mu.RLock()
	q, ok := b.queues[qname]
	b.mu.RUnlock()
	if !ok {
		q = make(chan any, 10)
		b.mu.Lock()
		b.queues[qname] = q
		b.mu.Unlock()
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

func (b *InMemoryBroker) Queues(ctx context.Context) []string {
	keys := make([]string, len(b.queues))
	i := 0
	for k := range b.queues {
		keys[i] = k
		i++
	}
	return keys
}
