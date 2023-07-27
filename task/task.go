package task

import (
	"time"

	"github.com/google/uuid"
)

type State int

const (
	Pending State = iota
	Scheduled
	Running
	Completed
	Failed
)

type Task struct {
	ID            uuid.UUID
	Name          string
	State         State
	StartTime     time.Time
	FinishTime    time.Time
	CMD           []string
	Image         string
	Memory        int64
	Disk          int64
	Env           []string
	RestartPolicy string
	ContainerID   string
}

type TaskEvent struct {
	ID        uuid.UUID
	State     State
	Timestamp time.Time
	Task      Task
}
