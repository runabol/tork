package input

import (
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/runabol/tork"
	"github.com/runabol/tork/internal/clone"
	"github.com/runabol/tork/internal/eval"
	"github.com/runabol/tork/mq"

	"github.com/runabol/tork/internal/uuid"
)

type Job struct {
	Name        string            `json:"name,omitempty" yaml:"name,omitempty" validate:"required"`
	Description string            `json:"description,omitempty" yaml:"description,omitempty"`
	Tasks       []Task            `json:"tasks,omitempty" yaml:"tasks,omitempty" validate:"required,min=1,dive"`
	Inputs      map[string]string `json:"inputs,omitempty" yaml:"inputs,omitempty"`
	Output      string            `json:"output,omitempty" yaml:"output,omitempty" validate:"expr"`
}

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

func (ji Job) ToJob() *tork.Job {
	n := time.Now().UTC()
	j := &tork.Job{}
	j.ID = uuid.NewUUID()
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
	return j
}

func (ji Job) Validate() error {
	validate := validator.New()
	if err := validate.RegisterValidation("duration", validateDuration); err != nil {
		return err
	}
	if err := validate.RegisterValidation("queue", validateQueue); err != nil {
		return err
	}
	if err := validate.RegisterValidation("expr", validateExpr); err != nil {
		return err
	}
	validate.RegisterStructValidation(taskInputValidation, Task{})
	return validate.Struct(ji)
}

func taskInputValidation(sl validator.StructLevel) {
	taskTypeValidation(sl)
	regularTaskValidation(sl)
	compositeTaskValidation(sl)
}

func regularTaskValidation(sl validator.StructLevel) {
	t := sl.Current().Interface().(Task)
	if t.Parallel != nil || t.Each != nil || t.SubJob != nil {
		return
	}
	if t.Image == "" {
		sl.ReportError(t.Image, "image", "Image", "required", "")
	}
}

func compositeTaskValidation(sl validator.StructLevel) {
	t := sl.Current().Interface().(Task)
	if t.Parallel == nil && t.Each == nil && t.SubJob == nil {
		return
	}
	if t.Image != "" {
		sl.ReportError(t.Parallel, "image", "Image", "invalidcompositetask", "")
	}
	if len(t.CMD) > 0 {
		sl.ReportError(t.Parallel, "cmd", "CMD", "invalidcompositetask", "")
	}
	if len(t.Entrypoint) > 0 {
		sl.ReportError(t.Entrypoint, "entrypoint", "Entrypoint", "invalidcompositetask", "")
	}
	if t.Run != "" {
		sl.ReportError(t.Run, "run", "Run", "invalidcompositetask", "")
	}
	if len(t.Env) > 0 {
		sl.ReportError(t.Env, "env", "Env", "invalidcompositetask", "")
	}
	if t.Queue != "" {
		sl.ReportError(t.Queue, "queue", "Queue", "invalidcompositetask", "")
	}
	if len(t.Pre) > 0 {
		sl.ReportError(t.Pre, "pre", "Pre", "invalidcompositetask", "")
	}
	if len(t.Post) > 0 {
		sl.ReportError(t.Post, "post", "Post", "invalidcompositetask", "")
	}
	if len(t.Volumes) > 0 {
		sl.ReportError(t.Volumes, "volumes", "Volumes", "invalidcompositetask", "")
	}
	if t.Retry != nil {
		sl.ReportError(t.Retry, "retry", "Retry", "invalidcompositetask", "")
	}
	if t.Limits != nil {
		sl.ReportError(t.Limits, "limits", "Limits", "invalidcompositetask", "")
	}
	if t.Timeout != "" {
		sl.ReportError(t.Timeout, "timeout", "Timeout", "invalidcompositetask", "")
	}
}

func taskTypeValidation(sl validator.StructLevel) {
	ti := sl.Current().Interface().(Task)

	if ti.Parallel != nil && ti.Each != nil {
		sl.ReportError(ti.Each, "each", "Each", "paralleloreach", "")
		sl.ReportError(ti.Parallel, "parallel", "Parallel", "paralleloreach", "")
	}

	if ti.Parallel != nil && ti.SubJob != nil {
		sl.ReportError(ti.Each, "subjob", "SubJob", "parallelorsubjob", "")
		sl.ReportError(ti.Parallel, "parallel", "Parallel", "parallelorsubjob", "")
	}

	if ti.Each != nil && ti.SubJob != nil {
		sl.ReportError(ti.Each, "subjob", "SubJob", "eachorsubjob", "")
		sl.ReportError(ti.Parallel, "each", "Each", "eachorsubjob", "")
	}

}

func validateExpr(fl validator.FieldLevel) bool {
	v := fl.Field().String()
	if v == "" {
		return true
	}
	return eval.ValidExpr(v)
}

func validateQueue(fl validator.FieldLevel) bool {
	v := fl.Field().String()
	if v == "" {
		return true
	}
	if strings.HasPrefix(v, mq.QUEUE_EXCLUSIVE_PREFIX) {
		return false
	}
	if mq.IsCoordinatorQueue(v) {
		return false
	}
	return true
}

func validateDuration(fl validator.FieldLevel) bool {
	v := fl.Field().String()
	if v == "" {
		return true
	}
	_, err := time.ParseDuration(v)
	return err == nil
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
