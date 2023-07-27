package worker

import (
	"fmt"
	"log"
	"time"

	"github.com/docker/docker/client"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
	"github.com/tork/task"
)

type Worker struct {
	Name      string
	Queue     queue.Queue
	DB        map[uuid.UUID]task.Task
	TaskCount int
	Client    *client.Client
}

func NewWorker() (*Worker, error) {
	dc, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return nil, err
	}
	return &Worker{
		Client: dc,
	}, nil
}

func (w *Worker) CollectStats() {
	fmt.Println("I will collect stats")
}

func (w *Worker) RunTask() {
	fmt.Println("I will start or stop a task")
}

func (w *Worker) StartTask(t task.Task) error {
	t.StartTime = time.Now().UTC()
	d := dockerClient{}
	containerID, err := d.run(t)
	if err != nil {
		log.Printf("Err running task %v: %v\n", t.ID, err)
		t.State = task.Failed
		w.DB[t.ID] = t
		return err
	}
	t.ContainerID = containerID
	t.State = task.Running
	w.DB[t.ID] = t
	return nil
}

func (w *Worker) StopTask(t task.Task) error {
	d := dockerClient{}
	err := d.stop(t.ContainerID)
	if err != nil {
		log.Printf("Error stopping container %v: %v", t.ContainerID, err)
	}
	t.FinishTime = time.Now().UTC()
	t.State = task.Completed
	w.DB[t.ID] = t
	log.Printf("Stopped and removed container %v for task %v", t.ContainerID, t.ID)
	return err
}
