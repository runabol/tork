package runtime

import "github.com/tork/task"

type Runtime interface {
	Start(t task.Task) (containerID string, err error)
	Stop(containerID string) error
}
