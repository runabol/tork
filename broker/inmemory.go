package broker

import (
	"context"
	"log"
	"sync"

	"github.com/tork/task"
)

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
	log.Printf("sending task %v to %s", t, qname)
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
	log.Printf("subscribing for tasks on %s", qname)
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
