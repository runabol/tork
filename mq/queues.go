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
	QUEUE_HEARTBEAT = "heartbeat"
	// The queue used by for job creation
	// and job-related state changes (e.g. cancellation)
	QUEUE_JOBS = "jobs"
	// The prefix used for queues that
	// are exclusive
	QUEUE_EXCLUSIVE_PREFIX = "x-"
)

type QueueInfo struct {
	Name        string `json:"name"`
	Size        int    `json:"size"`
	Subscribers int    `json:"subscribers"`
	Unacked     int    `json:"unacked"`
}

func IsCoordinatorQueue(qname string) bool {
	coordQueues := []string{
		QUEUE_PENDING,
		QUEUE_STARTED,
		QUEUE_COMPLETED,
		QUEUE_ERROR,
		QUEUE_HEARTBEAT,
		QUEUE_JOBS,
	}
	return slices.Contains(coordQueues, qname)
}

func IsWorkerQueue(qname string) bool {
	return !IsCoordinatorQueue(qname)
}
