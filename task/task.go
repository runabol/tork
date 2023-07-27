package task

import (
	"time"
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
	ID            string
	Name          string
	State         State
	StartTime     time.Time
	EndTime       time.Time
	CMD           []string
	Image         string
	Memory        int64
	Disk          int64
	Env           []string
	RestartPolicy string
}

type TaskEvent struct {
	ID        string
	State     State
	Timestamp time.Time
	Task      Task
}
