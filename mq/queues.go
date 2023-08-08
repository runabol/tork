package mq

import "golang.org/x/exp/slices"

const (
	QUEUE_PENDING   = "pending"
	QUEUE_STARTED   = "started"
	QUEUE_COMPLETED = "completed"
	QUEUE_ERROR     = "error"
	QUEUE_DEFAULT   = "default"
)

type QueueInfo struct {
	Name string
	Size int
}

func IsCoordinatorQueue(qname string) bool {
	return !IsWorkerQueue(qname)
}

func IsWorkerQueue(qname string) bool {
	coordQueues := []string{
		QUEUE_PENDING,
		QUEUE_STARTED,
		QUEUE_COMPLETED,
		QUEUE_ERROR,
	}
	return !slices.Contains(coordQueues, qname)
}
