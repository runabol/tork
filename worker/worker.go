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

type Config struct {
	Name          string
	AttachStdin   bool
	AttachStdout  bool
	AttachStderr  bool
	Cmd           []string
	Image         string
	Memory        int64
	Disk          int64
	Env           []string
	RestartPolicy string
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

func (w *Worker) StartTask(t task.Task) dockerResult {
	t.StartTime = time.Now().UTC()
	cfg := Config{
		Image: t.Image,
	}
	d := dockerClient{}
	result := d.Run(cfg)
	if result.Error != nil {
		log.Printf("Err running task %v: %v\n", t.ID, result.Error)
		t.State = task.Failed
		w.DB[t.ID] = t
		return result
	}
	t.ContainerID = result.ContainerID
	t.State = task.Running
	w.DB[t.ID] = t
	return result
}

func (w *Worker) StopTask(t task.Task) dockerResult {
	d := dockerClient{}
	result := d.Stop(t.ContainerID)
	if result.Error != nil {
		log.Printf("Error stopping container %v: %v", t.ContainerID, result.Error)
	}
	t.FinishTime = time.Now().UTC()
	t.State = task.Completed
	w.DB[t.ID] = t
	log.Printf("Stopped and removed container %v for task %v", t.ContainerID, t.ID)
	return result
}
