package manager

import (
	"fmt"

	"github.com/tork/task"
)

type Coordinator struct {
	TaskDB        map[string][]task.Task
	Workers       []string
	WorkerTaskMap map[string][]string
	TaskWorkerMap map[string]string
}

func (m *Coordinator) SelectWorker() {
	fmt.Println("I will select an appropriate worker")
}

func (m *Coordinator) UpdateTasks() {
	fmt.Println("I will update tasks")
}

func (m *Coordinator) SendWork() {
	fmt.Println("I will send work to workers")
}
