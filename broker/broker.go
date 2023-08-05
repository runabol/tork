package broker

import (
	"context"
	"errors"

	"github.com/tork/task"
)

var ErrUnknownQueue = errors.New("unknown queue")

// Broker is the pub/sub mechanism used for delivering tasks.
type Broker interface {
	Queues(ctx context.Context) []string
	Enqueue(ctx context.Context, qname string, t task.Task) error
	Subscribe(qname string, handler func(ctx context.Context, t task.Task) error) error
}
