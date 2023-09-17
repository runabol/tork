package input

import (
	"time"

	"github.com/runabol/tork"
	"github.com/runabol/tork/internal/uuid"
)

type Job struct {
	id          string
	Name        string            `json:"name,omitempty" yaml:"name,omitempty" validate:"required"`
	Description string            `json:"description,omitempty" yaml:"description,omitempty"`
	Tasks       []Task            `json:"tasks,omitempty" yaml:"tasks,omitempty" validate:"required,min=1,dive"`
	Inputs      map[string]string `json:"inputs,omitempty" yaml:"inputs,omitempty"`
	Output      string            `json:"output,omitempty" yaml:"output,omitempty" validate:"expr"`
	Defaults    *Defaults         `json:"defaults,omitempty" yaml:"defaults,omitempty"`
}

type Defaults struct {
	Retry   *Retry  `json:"retry,omitempty" yaml:"retry,omitempty"`
	Limits  *Limits `json:"limits,omitempty" yaml:"limits,omitempty"`
	Timeout string  `json:"timeout,omitempty" yaml:"timeout,omitempty" validate:"duration"`
	Queue   string  `json:"queue,omitempty" yaml:"queue,omitempty" validate:"queue"`
}

func (ji *Job) ID() string {
	if ji.id == "" {
		ji.id = uuid.NewUUID()
	}
	return ji.id
}

func (ji *Job) ToJob() *tork.Job {
	n := time.Now().UTC()
	j := &tork.Job{}
	j.ID = ji.ID()
	j.Description = ji.Description
	j.Inputs = ji.Inputs
	j.Name = ji.Name
	tasks := make([]*tork.Task, len(ji.Tasks))
	for i, ti := range ji.Tasks {
		tasks[i] = ti.toTask()
	}
	j.Tasks = tasks
	j.State = tork.JobStatePending
	j.CreatedAt = n
	j.Context = tork.JobContext{}
	j.Context.Inputs = ji.Inputs
	j.TaskCount = len(tasks)
	j.Output = ji.Output
	if ji.Defaults != nil {
		j.Defaults = ji.Defaults.ToJobDefaults()
	}
	return j
}

func (d Defaults) ToJobDefaults() *tork.JobDefaults {
	jd := tork.JobDefaults{}
	if d.Retry != nil {
		jd.Retry = d.Retry.toTaskRetry()
	}
	if d.Limits != nil {
		jd.Limits = d.Limits.toTaskLimits()
	}
	jd.Timeout = d.Timeout
	jd.Queue = d.Queue
	return &jd
}
