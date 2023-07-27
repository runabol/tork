package manager

import (
	"fmt"

	"github.com/golang-collections/collections/queue"
	"github.com/tork/task"
)

type Manager struct {
	Pending       queue.Queue
	TaskDB        map[string][]task.Task
	EventDB       map[string][]task.TaskEvent
	Workers       []string
	WorkerTaskMap map[string][]string
	TaskWorkerMap map[string]string
}

func (m *Manager) SelectWorker() {
	fmt.Println("I will select an appropriate worker")
}

func (m *Manager) UpdateTasks() {
	fmt.Println("I will update tasks")
}

func (m *Manager) SendWork() {
	fmt.Println("I will send work to workers")
}
