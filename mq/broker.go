package mq

import (
	"context"

	"github.com/runabol/tork/job"
	"github.com/runabol/tork/node"
	"github.com/runabol/tork/task"
)

const (
	BROKER_INMEMORY = "inmemory"
	BROKER_RABBITMQ = "rabbitmq"
)

// Broker is the message-queue, pub/sub mechanism used for delivering tasks.
type Broker interface {
	Queues(ctx context.Context) ([]QueueInfo, error)
	PublishTask(ctx context.Context, qname string, t *task.Task) error
	SubscribeForTasks(qname string, handler func(t *task.Task) error) error
	PublishHeartbeat(ctx context.Context, n node.Node) error
	SubscribeForHeartbeats(handler func(n node.Node) error) error
	PublishJob(ctx context.Context, j *job.Job) error
	SubscribeForJobs(handler func(j *job.Job) error) error
}
