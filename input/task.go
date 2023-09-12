package input

import (
	"github.com/runabol/tork"
	"github.com/runabol/tork/internal/clone"
)

type Task struct {
	Name        string            `json:"name,omitempty" yaml:"name,omitempty" validate:"required"`
	Description string            `json:"description,omitempty" yaml:"description,omitempty"`
	CMD         []string          `json:"cmd,omitempty" yaml:"cmd,omitempty"`
	Entrypoint  []string          `json:"entrypoint,omitempty" yaml:"entrypoint,omitempty"`
	Run         string            `json:"run,omitempty" yaml:"run,omitempty"`
	Image       string            `json:"image,omitempty" yaml:"image,omitempty"`
	Env         map[string]string `json:"env,omitempty" yaml:"env,omitempty"`
	Files       map[string]string `json:"files,omitempty" yaml:"files,omitempty"`
	Queue       string            `json:"queue,omitempty" yaml:"queue,omitempty" validate:"queue"`
	Pre         []AuxTask         `json:"pre,omitempty" yaml:"pre,omitempty" validate:"dive"`
	Post        []AuxTask         `json:"post,omitempty" yaml:"post,omitempty" validate:"dive"`
	Volumes     []string          `json:"volumes,omitempty" yaml:"volumes,omitempty"`
	Networks    []string          `json:"networks,omitempty" yaml:"networks,omitempty"`
	Retry       *Retry            `json:"retry,omitempty" yaml:"retry,omitempty"`
	Limits      *Limits           `json:"limits,omitempty" yaml:"limits,omitempty"`
	Timeout     string            `json:"timeout,omitempty" yaml:"timeout,omitempty" validate:"duration"`
	Var         string            `json:"var,omitempty" yaml:"var,omitempty"`
	If          string            `json:"if,omitempty" yaml:"if,omitempty" validate:"expr"`
	Parallel    *Parallel         `json:"parallel,omitempty" yaml:"parallel,omitempty"`
	Each        *Each             `json:"each,omitempty" yaml:"each,omitempty"`
	SubJob      *SubJob           `json:"subjob,omitempty" yaml:"subjob,omitempty"`
}

type AuxTask struct {
	Name        string            `json:"name,omitempty" yaml:"name,omitempty" validate:"required"`
	Description string            `json:"description,omitempty" yaml:"description,omitempty"`
	CMD         []string          `json:"cmd,omitempty" yaml:"cmd,omitempty"`
	Entrypoint  []string          `json:"entrypoint,omitempty" yaml:"entrypoint,omitempty"`
	Run         string            `json:"run,omitempty" yaml:"run,omitempty"`
	Image       string            `json:"image,omitempty" yaml:"image,omitempty" validate:"required"`
	Env         map[string]string `json:"env,omitempty" yaml:"env,omitempty"`
	Timeout     string            `json:"timeout,omitempty" yaml:"timeout,omitempty"`
}

func (i AuxTask) toTask() *tork.Task {
	return &tork.Task{
		Name:        i.Name,
		Description: i.Description,
		CMD:         i.CMD,
		Entrypoint:  i.Entrypoint,
		Run:         i.Run,
		Image:       i.Image,
		Env:         i.Env,
		Timeout:     i.Timeout,
	}
}

func (i Task) toTask() *tork.Task {
	pre := toAuxTasks(i.Pre)
	post := toAuxTasks(i.Post)
	var retry *tork.TaskRetry
	if i.Retry != nil {
		retry = &tork.TaskRetry{
			Limit: i.Retry.Limit,
		}
	}
	var limits *tork.TaskLimits
	if i.Limits != nil {
		limits = &tork.TaskLimits{
			CPUs:   i.Limits.CPUs,
			Memory: i.Limits.Memory,
		}
	}
	var each *tork.EachTask
	if i.Each != nil {
		each = &tork.EachTask{
			List: i.Each.List,
			Task: i.Each.Task.toTask(),
		}
	}
	var subjob *tork.SubJobTask
	if i.SubJob != nil {
		subjob = &tork.SubJobTask{
			Name:        i.SubJob.Name,
			Description: i.SubJob.Description,
			Tasks:       toTasks(i.SubJob.Tasks),
			Inputs:      clone.CloneStringMap(i.SubJob.Inputs),
			Output:      i.SubJob.Output,
		}
	}
	var parallel *tork.ParallelTask
	if i.Parallel != nil {
		parallel = &tork.ParallelTask{
			Tasks: toTasks(i.Parallel.Tasks),
		}
	}
	return &tork.Task{
		Name:        i.Name,
		Description: i.Description,
		CMD:         i.CMD,
		Entrypoint:  i.Entrypoint,
		Run:         i.Run,
		Image:       i.Image,
		Env:         i.Env,
		Files:       i.Files,
		Queue:       i.Queue,
		Pre:         pre,
		Post:        post,
		Volumes:     i.Volumes,
		Networks:    i.Networks,
		Retry:       retry,
		Limits:      limits,
		Timeout:     i.Timeout,
		Var:         i.Var,
		If:          i.If,
		Parallel:    parallel,
		Each:        each,
		SubJob:      subjob,
	}
}

func toAuxTasks(tis []AuxTask) []*tork.Task {
	result := make([]*tork.Task, len(tis))
	for i, ti := range tis {
		result[i] = ti.toTask()
	}
	return result
}

func toTasks(tis []Task) []*tork.Task {
	result := make([]*tork.Task, len(tis))
	for i, ti := range tis {
		result[i] = ti.toTask()
	}
	return result
}

type SubJob struct {
	ID          string            `json:"id,omitempty"`
	Name        string            `json:"name,omitempty" yaml:"name,omitempty" validate:"required"`
	Description string            `json:"description,omitempty" yaml:"description,omitempty"`
	Tasks       []Task            `json:"tasks,omitempty" yaml:"tasks,omitempty" validate:"required"`
	Inputs      map[string]string `json:"inputs,omitempty" yaml:"inputs,omitempty"`
	Output      string            `json:"output,omitempty" yaml:"output,omitempty"`
}

type Each struct {
	List string `json:"list,omitempty" yaml:"list,omitempty" validate:"required,expr"`
	Task Task   `json:"task,omitempty" yaml:"task,omitempty" validate:"required"`
}

type Parallel struct {
	Tasks []Task `json:"tasks,omitempty" yaml:"tasks,omitempty" validate:"required,min=1,dive"`
}

type Retry struct {
	Limit int `json:"limit,omitempty" yaml:"limit,omitempty" validate:"required,min=1,max=10"`
}

type Limits struct {
	CPUs   string `json:"cpus,omitempty" yaml:"cpus,omitempty"`
	Memory string `json:"memory,omitempty" yaml:"memory,omitempty"`
}
