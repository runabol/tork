package task

import (
	"time"
)

type State int

const (
	// Pending this is the initial state, the starting point, for every task.
	Pending State = iota
	// Scheduled a task moves to this state once the coordinator has scheduled it onto a worker.
	Scheduled
	// Running a task moves to this state when a worker successfully starts the task (i.e. starts the container).
	Running
	// Completed a task moves to this state when it completes its work in a normal way (i.e. it does not fail).
	Completed
	// Failed if a task does fail, it moves to this state.
	Failed
)

// Task is the basic unit of work that a Worker can handle.
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
