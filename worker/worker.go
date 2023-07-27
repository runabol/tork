package worker

import (
	"fmt"
	"log"
	"time"

	"github.com/golang-collections/collections/queue"
	"github.com/tork/runtime"
	"github.com/tork/task"
)

type Worker struct {
	tasks   map[string]string
	runtime runtime.Runtime
	queue   *queue.Queue
}

func NewWorker() (*Worker, error) {
	r, err := runtime.NewDockerRuntime()
	if err != nil {
		return nil, err
	}
	return &Worker{
		runtime: r,
		queue:   queue.New(),
		tasks:   make(map[string]string),
	}, nil
}

func (w *Worker) EnqueueTask(t task.Task) {
	w.queue.Enqueue(t)
}

func (w *Worker) CollectStats() {
	fmt.Println("I will collect stats")
}

func (w *Worker) RunTask() error {
	dt := w.queue.Dequeue()
	if dt == nil {
		return nil
	}
	t := dt.(task.Task)
	return w.StartTask(t)
}

func (w *Worker) StartTask(t task.Task) error {
	containerID, err := w.runtime.Start(t)
	if err != nil {
		log.Printf("Err running task %v: %v\n", t.ID, err)
		return err
	}
	w.tasks[t.ID] = containerID
	return nil
}

func (w *Worker) StopTask(t task.Task) error {
	containerID := w.tasks[t.ID]
	err := w.runtime.Stop(containerID)
	if err != nil {
		log.Printf("Error stopping container %v: %v", containerID, err)
	}
	delete(w.tasks, t.ID)
	t.EndTime = time.Now().UTC()
	log.Printf("Stopped and removed container %v for task %v", containerID, t.ID)
	return err
}
