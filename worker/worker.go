package worker

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/tork/runtime"
	"github.com/tork/task"
)

type Worker struct {
	runtime runtime.Runtime
	queue   chan task.Task
}

func NewWorker() (*Worker, error) {
	r, err := runtime.NewDockerRuntime()
	if err != nil {
		return nil, err
	}
	return &Worker{
		runtime: r,
		queue:   make(chan task.Task, 10),
	}, nil
}

func (w *Worker) EnqueueTask(t task.Task) {
	w.queue <- t
}

func (w *Worker) CollectStats() {
	fmt.Println("I will collect stats")
}

func (w *Worker) RunTask() error {
	dt := <-w.queue
	return w.StartTask(dt)
}

func (w *Worker) StartTask(t task.Task) error {
	ctx := context.Background()
	err := w.runtime.Start(ctx, t)
	if err != nil {
		log.Printf("Err running task %v: %v\n", t.ID, err)
		return err
	}
	return nil
}

func (w *Worker) StopTask(t task.Task) error {
	ctx := context.Background()
	err := w.runtime.Stop(ctx, t)
	if err != nil {
		log.Printf("Error stopping task %s: %v", t.ID, err)
	}
	t.EndTime = time.Now().UTC()
	log.Printf("Stopped and removed task %s", t.ID)
	return err
}
