package input

import (
	"time"

	"github.com/runabol/tork"
	"github.com/runabol/tork/internal/uuid"
	"golang.org/x/exp/maps"
)

type Job struct {
	id          string
	Name        string            `json:"name,omitempty" yaml:"name,omitempty" validate:"required"`
	Description string            `json:"description,omitempty" yaml:"description,omitempty"`
	Tags        []string          `json:"tags,omitempty" yaml:"tags,omitempty"`
	Tasks       []Task            `json:"tasks,omitempty" yaml:"tasks,omitempty" validate:"required,min=1,dive"`
	Inputs      map[string]string `json:"inputs,omitempty" yaml:"inputs,omitempty"`
	Secrets     map[string]string `json:"secrets,omitempty" yaml:"secrets,omitempty"`
	Output      string            `json:"output,omitempty" yaml:"output,omitempty" validate:"expr"`
	Defaults    *Defaults         `json:"defaults,omitempty" yaml:"defaults,omitempty"`
	Webhooks    []Webhook         `json:"webhooks,omitempty" yaml:"webhooks,omitempty" validate:"dive"`
	Permissions []Permission      `json:"permissions,omitempty" yaml:"permissions,omitempty" validate:"dive"`
	AutoDelete  *AutoDelete       `json:"autoDelete,omitempty" yaml:"autoDelete,omitempty"`
}

type ScheduledJob struct {
	id          string
	Name        string            `json:"name,omitempty" yaml:"name,omitempty" validate:"required"`
	Description string            `json:"description,omitempty" yaml:"description,omitempty"`
	Tags        []string          `json:"tags,omitempty" yaml:"tags,omitempty"`
	Tasks       []Task            `json:"tasks,omitempty" yaml:"tasks,omitempty" validate:"required,min=1,dive"`
	Inputs      map[string]string `json:"inputs,omitempty" yaml:"inputs,omitempty"`
	Secrets     map[string]string `json:"secrets,omitempty" yaml:"secrets,omitempty"`
	Output      string            `json:"output,omitempty" yaml:"output,omitempty" validate:"expr"`
	Defaults    *Defaults         `json:"defaults,omitempty" yaml:"defaults,omitempty"`
	Webhooks    []Webhook         `json:"webhooks,omitempty" yaml:"webhooks,omitempty" validate:"dive"`
	Permissions []Permission      `json:"permissions,omitempty" yaml:"permissions,omitempty" validate:"dive"`
	AutoDelete  *AutoDelete       `json:"autoDelete,omitempty" yaml:"autoDelete,omitempty"`
	Schedule    *Schedule         `json:"schedule,omitempty" yaml:"schedule,omitempty" validate:"required"`
}

type Defaults struct {
	Retry    *Retry  `json:"retry,omitempty" yaml:"retry,omitempty"`
	Limits   *Limits `json:"limits,omitempty" yaml:"limits,omitempty"`
	Timeout  string  `json:"timeout,omitempty" yaml:"timeout,omitempty" validate:"duration"`
	Queue    string  `json:"queue,omitempty" yaml:"queue,omitempty" validate:"queue"`
	Priority int     `json:"priority,omitempty" yaml:"priority,omitempty" validate:"min=0,max=9"`
}

type Schedule struct {
	Cron string `json:"cron" yaml:"cron" validate:"required,cron"`
}

type AutoDelete struct {
	After string `json:"after,omitempty" yaml:"after,omitempty" validate:"duration"`
}

type Webhook struct {
	URL     string            `json:"url,omitempty" yaml:"url,omitempty" validate:"required"`
	Headers map[string]string `json:"headers,omitempty" yaml:"headers,omitempty"`
	Event   string            `json:"event,omitempty" yaml:"event,omitempty"`
	If      string            `json:"if,omitempty" yaml:"if,omitempty" validate:"expr"`
}

type Permission struct {
	User string `json:"user,omitempty" yaml:"user,omitempty"`
	Role string `json:"role,omitempty" yaml:"role,omitempty"`
}

func (ji *Job) ID() string {
	if ji.id == "" {
		ji.id = uuid.NewUUID()
	}
	return ji.id
}

func (ji *ScheduledJob) ID() string {
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
	j.Secrets = ji.Secrets
	j.Tags = ji.Tags
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
	j.Context.Secrets = ji.Secrets
	j.Context.Job = map[string]string{
		"id":   j.ID,
		"name": j.Name,
	}
	j.TaskCount = len(tasks)
	j.Output = ji.Output
	if ji.Defaults != nil {
		j.Defaults = ji.Defaults.ToJobDefaults()
	}
	webhooks := make([]*tork.Webhook, len(ji.Webhooks))
	for i, wh := range ji.Webhooks {
		webhooks[i] = wh.toWebhook()
	}
	j.Webhooks = webhooks
	perms := make([]*tork.Permission, len(ji.Permissions))
	for i, p := range ji.Permissions {
		perms[i] = p.toPermission()
	}
	j.Permissions = perms
	if ji.AutoDelete != nil {
		j.AutoDelete = &tork.AutoDelete{
			After: ji.AutoDelete.After,
		}
	}
	return j
}

func (ji *ScheduledJob) ToScheduledJob() *tork.ScheduledJob {
	n := time.Now().UTC()
	j := &tork.ScheduledJob{}
	j.ID = ji.ID()
	j.Description = ji.Description
	j.Inputs = ji.Inputs
	j.Secrets = ji.Secrets
	j.Tags = ji.Tags
	j.Name = ji.Name
	tasks := make([]*tork.Task, len(ji.Tasks))
	for i, ti := range ji.Tasks {
		tasks[i] = ti.toTask()
	}
	j.Tasks = tasks
	j.State = tork.ScheduledJobStateActive
	j.CreatedAt = n
	j.Output = ji.Output
	if ji.Defaults != nil {
		j.Defaults = ji.Defaults.ToJobDefaults()
	}
	webhooks := make([]*tork.Webhook, len(ji.Webhooks))
	for i, wh := range ji.Webhooks {
		webhooks[i] = wh.toWebhook()
	}
	j.Webhooks = webhooks
	perms := make([]*tork.Permission, len(ji.Permissions))
	for i, p := range ji.Permissions {
		perms[i] = p.toPermission()
	}
	j.Permissions = perms
	if ji.AutoDelete != nil {
		j.AutoDelete = &tork.AutoDelete{
			After: ji.AutoDelete.After,
		}
	}
	j.Cron = ji.Schedule.Cron
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
	jd.Priority = d.Priority
	return &jd
}

func (w Webhook) toWebhook() *tork.Webhook {
	return &tork.Webhook{
		URL:     w.URL,
		Headers: maps.Clone(w.Headers),
		Event:   w.Event,
		If:      w.If,
	}
}

func (p Permission) toPermission() *tork.Permission {
	tp := &tork.Permission{}
	if p.Role != "" {
		tp.Role = &tork.Role{
			Slug: p.Role,
		}
	} else {
		tp.User = &tork.User{
			Username: p.User,
		}
	}
	return tp
}
