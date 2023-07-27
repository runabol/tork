package broker

import (
	"context"
	"errors"

	"github.com/tork/task"
)

var ErrUnknownQueue = errors.New("unknown queue")

// Broker is the medium through which the Coordinator and Worker instances communicate.
// The Coordinator enqueues tasks to be done in worker queues and the workers in turn
// pull these tasks and execute them.
type Broker interface {
	Send(ctx context.Context, qname string, t task.Task) error
	Receive(qname string, handler func(ctx context.Context, t task.Task) error) error
}
