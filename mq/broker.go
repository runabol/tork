package mq

import (
	"context"

	"github.com/tork/task"
)

const (
	BROKER_INMEMORY = "inmemory"
	BROKER_RABBITMQ = "rabbitmq"
)

// Broker is the message-queue, pub/sub mechanism used for delivering tasks.
type Broker interface {
	Queues(ctx context.Context) ([]QueueInfo, error)
	Publish(ctx context.Context, qname string, t *task.Task) error
	Subscribe(qname string, handler func(ctx context.Context, t *task.Task) error) error
}
