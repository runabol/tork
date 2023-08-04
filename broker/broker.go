package broker

import (
	"context"
	"errors"
)

var ErrUnknownQueue = errors.New("unknown queue")

// Broker is the medium through which the Coordinator and Worker instances communicate.
// The Coordinator enqueues tasks to be done in worker queues and the workers in turn
// pull these tasks and execute them.
type Broker interface {
	Queues(ctx context.Context) []string
	Send(ctx context.Context, qname string, msg any) error
	Receive(qname string, handler func(ctx context.Context, msg any) error) error
}
