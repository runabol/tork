package mq

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
