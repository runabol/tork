package mq

import "golang.org/x/exp/slices"

const (
	// The queue used by the API to insert new tasks into
	QUEUE_PENDING = "pending"
	// The queue used by workers to notify the coordinator
	// that a task has began processing
	QUEUE_STARTED = "started"
	// The queue used by workers to send tasks to when
	// a task completes successfully
	QUEUE_COMPLETED = "completed"
	// The queue used by workers to send tasks to when an error
	// occurs in processing
	QUEUE_ERROR = "error"
	// The default queue for tasks
	QUEUE_DEFAULT = "default"
	// The queue used by workers to periodically
	// notify the coordinator about their aliveness
	QUEUE_HEARBEAT = "hearbeat"
)

type QueueInfo struct {
	Name string
	Size int
}

func IsCoordinatorQueue(qname string) bool {
	coordQueues := []string{
		QUEUE_PENDING,
		QUEUE_STARTED,
		QUEUE_COMPLETED,
		QUEUE_ERROR,
		QUEUE_HEARBEAT,
	}
	return slices.Contains(coordQueues, qname)
}

func IsWorkerQueue(qname string) bool {
	return !IsCoordinatorQueue(qname)
}
