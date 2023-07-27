package worker

import (
	"fmt"
	"log"
	"time"

	"github.com/docker/docker/client"
	"github.com/tork/task"
)

type Worker struct {
	tasks   map[string]string
	runtime runtime
}

type runtime interface {
	start(t task.Task) (string, error)
	stop(containerID string) error
}

func NewWorker() (*Worker, error) {
	dc, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return nil, err
	}
	return &Worker{
		runtime: &dockerRuntime{Client: dc},
		tasks:   make(map[string]string),
	}, nil
}

func (w *Worker) CollectStats() {
	fmt.Println("I will collect stats")
}

func (w *Worker) RunTask() {
	fmt.Println("I will start or stop a task")
}

func (w *Worker) StartTask(t task.Task) error {
	containerID, err := w.runtime.start(t)
	if err != nil {
		log.Printf("Err running task %v: %v\n", t.ID, err)
		return err
	}
	w.tasks[t.ID] = containerID
	return nil
}

func (w *Worker) StopTask(t task.Task) error {
	containerID := w.tasks[t.ID]
	err := w.runtime.stop(containerID)
	if err != nil {
		log.Printf("Error stopping container %v: %v", containerID, err)
	}
	t.EndTime = time.Now().UTC()
	log.Printf("Stopped and removed container %v for task %v", containerID, t.ID)
	return err
}
