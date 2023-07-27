package runtime

import "github.com/tork/task"

type Runtime interface {
	Start(t task.Task) (string, error)
	Stop(containerID string) error
}
