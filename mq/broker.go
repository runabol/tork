package mq

import (
	"context"

	"github.com/tork/node"
	"github.com/tork/task"
)

const (
	BROKER_INMEMORY = "inmemory"
	BROKER_RABBITMQ = "rabbitmq"
)

// Broker is the message-queue, pub/sub mechanism used for delivering tasks.
type Broker interface {
	Queues(ctx context.Context) ([]QueueInfo, error)
	PublishTask(ctx context.Context, qname string, t task.Task) error
	SubscribeForTasks(qname string, handler func(ctx context.Context, t task.Task) error) error
	PublishHeartbeat(ctx context.Context, n node.Node) error
	SubscribeForHeartbeats(handler func(ctx context.Context, n node.Node) error) error
}
