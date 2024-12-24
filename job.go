package tork

import (
	"time"

	"golang.org/x/exp/maps"
)

type JobState = string

const (
	JobStatePending   JobState = "PENDING"
	JobStateScheduled JobState = "SCHEDULED"
	JobStateRunning   JobState = "RUNNING"
	JobStateCancelled JobState = "CANCELLED"
	JobStateCompleted JobState = "COMPLETED"
	JobStateFailed    JobState = "FAILED"
	JobStateRestart   JobState = "RESTART"
)

type ScheduledJobState string

const (
	ScheduledJobStateActive ScheduledJobState = "ACTIVE"
	ScheduledJobStatePaused ScheduledJobState = "PAUSED"
)

type Job struct {
	ID          string            `json:"id,omitempty"`
	ParentID    string            `json:"parentId,omitempty"`
	Name        string            `json:"name,omitempty"`
	Description string            `json:"description,omitempty"`
	Tags        []string          `json:"tags,omitempty"`
	State       JobState          `json:"state,omitempty"`
	CreatedAt   time.Time         `json:"createdAt,omitempty"`
	CreatedBy   *User             `json:"createdBy,omitempty"`
	StartedAt   *time.Time        `json:"startedAt,omitempty"`
	CompletedAt *time.Time        `json:"completedAt,omitempty"`
	FailedAt    *time.Time        `json:"failedAt,omitempty"`
	Tasks       []*Task           `json:"tasks"`
	Execution   []*Task           `json:"execution"`
	Position    int               `json:"position"`
	Inputs      map[string]string `json:"inputs,omitempty"`
	Context     JobContext        `json:"context,omitempty"`
	TaskCount   int               `json:"taskCount,omitempty"`
	Output      string            `json:"output,omitempty"`
	Result      string            `json:"result,omitempty"`
	Error       string            `json:"error,omitempty"`
	Defaults    *JobDefaults      `json:"defaults,omitempty"`
	Webhooks    []*Webhook        `json:"webhooks,omitempty"`
	Permissions []*Permission     `json:"permissions,omitempty"`
	AutoDelete  *AutoDelete       `json:"autoDelete,omitempty"`
	DeleteAt    *time.Time        `json:"deleteAt,omitempty"`
	Secrets     map[string]string `json:"secrets,omitempty"`
	Progress    float64           `json:"progress,omitempty"`
	Schedule    *JobSchedule      `json:"schedule,omitempty"`
}

type ScheduledJob struct {
	ID          string            `json:"id,omitempty"`
	Name        string            `json:"name,omitempty"`
	Description string            `json:"description,omitempty"`
	Cron        string            `json:"cron,omitempty"`
	State       ScheduledJobState `json:"state,omitempty"`
	Inputs      map[string]string `json:"inputs,omitempty"`
	Tasks       []*Task           `json:"tasks"`
	CreatedBy   *User             `json:"createdBy,omitempty"`
	Defaults    *JobDefaults      `json:"defaults,omitempty"`
	AutoDelete  *AutoDelete       `json:"autoDelete,omitempty"`
	Webhooks    []*Webhook        `json:"webhooks,omitempty"`
	Permissions []*Permission     `json:"permissions,omitempty"`
	CreatedAt   time.Time         `json:"createdAt,omitempty"`
	Tags        []string          `json:"tags,omitempty"`
	Secrets     map[string]string `json:"secrets,omitempty"`
	Output      string            `json:"output,omitempty"`
}

type JobSchedule struct {
	ID   string `json:"id,omitempty"`
	Cron string `json:"cron,omitempty"`
}

func (s *JobSchedule) Clone() *JobSchedule {
	return &JobSchedule{
		ID:   s.ID,
		Cron: s.Cron,
	}
}

type JobSummary struct {
	ID          string            `json:"id,omitempty"`
	CreatedBy   *User             `json:"createdBy,omitempty"`
	ParentID    string            `json:"parentId,omitempty"`
	Inputs      map[string]string `json:"inputs,omitempty"`
	Name        string            `json:"name,omitempty"`
	Description string            `json:"description,omitempty"`
	Tags        []string          `json:"tags,omitempty"`
	State       JobState          `json:"state,omitempty"`
	CreatedAt   time.Time         `json:"createdAt,omitempty"`
	StartedAt   *time.Time        `json:"startedAt,omitempty"`
	CompletedAt *time.Time        `json:"completedAt,omitempty"`
	FailedAt    *time.Time        `json:"failedAt,omitempty"`
	Position    int               `json:"position"`
	TaskCount   int               `json:"taskCount,omitempty"`
	Result      string            `json:"result,omitempty"`
	Error       string            `json:"error,omitempty"`
	Progress    float64           `json:"progress,omitempty"`
	Schedule    *JobSchedule      `json:"schedule,omitempty"`
}

type ScheduledJobSummary struct {
	ID          string            `json:"id,omitempty"`
	CreatedBy   *User             `json:"createdBy,omitempty"`
	Inputs      map[string]string `json:"inputs,omitempty"`
	State       ScheduledJobState `json:"state,omitempty"`
	Name        string            `json:"name,omitempty"`
	Description string            `json:"description,omitempty"`
	Tags        []string          `json:"tags,omitempty"`
	CreatedAt   time.Time         `json:"createdAt,omitempty"`
	Cron        string            `json:"cron,omitempty"`
}

type Permission struct {
	Role *Role `json:"role,omitempty"`
	User *User `json:"user,omitempty"`
}

type AutoDelete struct {
	After string `json:"after,omitempty"`
}

type JobContext struct {
	Job     map[string]string `json:"job,omitempty"`
	Inputs  map[string]string `json:"inputs,omitempty"`
	Secrets map[string]string `json:"secrets,omitempty"`
	Tasks   map[string]string `json:"tasks,omitempty"`
}

type JobDefaults struct {
	Retry    *TaskRetry  `json:"retry,omitempty"`
	Limits   *TaskLimits `json:"limits,omitempty"`
	Timeout  string      `json:"timeout,omitempty"`
	Queue    string      `json:"queue,omitempty"`
	Priority int         `json:"priority,omitempty"`
}

type Webhook struct {
	URL     string            `json:"url,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Event   string            `json:"event,omitempty"`
	If      string            `json:"if,omitempty"`
}

func (j *Job) Clone() *Job {
	var defaults *JobDefaults
	if j.Defaults != nil {
		defaults = j.Defaults.Clone()
	}
	var createdBy *User
	if j.CreatedBy != nil {
		createdBy = j.CreatedBy.Clone()
	}
	var autoDelete *AutoDelete
	if j.AutoDelete != nil {
		autoDelete = j.AutoDelete.Clone()
	}
	var schedule *JobSchedule
	if j.Schedule != nil {
		schedule = j.Schedule.Clone()
	}
	return &Job{
		ID:          j.ID,
		Name:        j.Name,
		Description: j.Description,
		Tags:        j.Tags,
		State:       j.State,
		CreatedAt:   j.CreatedAt,
		CreatedBy:   createdBy,
		StartedAt:   j.StartedAt,
		CompletedAt: j.CompletedAt,
		FailedAt:    j.FailedAt,
		Tasks:       CloneTasks(j.Tasks),
		Execution:   CloneTasks(j.Execution),
		Position:    j.Position,
		Inputs:      maps.Clone(j.Inputs),
		Secrets:     maps.Clone(j.Secrets),
		Context:     j.Context.Clone(),
		ParentID:    j.ParentID,
		TaskCount:   j.TaskCount,
		Output:      j.Output,
		Result:      j.Result,
		Error:       j.Error,
		Defaults:    defaults,
		Webhooks:    CloneWebhooks(j.Webhooks),
		Permissions: ClonePermissions(j.Permissions),
		AutoDelete:  autoDelete,
		Progress:    j.Progress,
		Schedule:    schedule,
	}
}

func (j *ScheduledJob) Clone() *ScheduledJob {
	var defaults *JobDefaults
	if j.Defaults != nil {
		defaults = j.Defaults.Clone()
	}
	var createdBy *User
	if j.CreatedBy != nil {
		createdBy = j.CreatedBy.Clone()
	}
	var autoDelete *AutoDelete
	if j.AutoDelete != nil {
		autoDelete = j.AutoDelete.Clone()
	}
	return &ScheduledJob{
		ID:          j.ID,
		Cron:        j.Cron,
		Name:        j.Name,
		Description: j.Description,
		Tags:        j.Tags,
		CreatedAt:   j.CreatedAt,
		CreatedBy:   createdBy,
		Tasks:       CloneTasks(j.Tasks),
		Inputs:      maps.Clone(j.Inputs),
		Secrets:     maps.Clone(j.Secrets),
		Output:      j.Output,
		Defaults:    defaults,
		Webhooks:    CloneWebhooks(j.Webhooks),
		Permissions: ClonePermissions(j.Permissions),
		AutoDelete:  autoDelete,
		State:       j.State,
	}
}

func (c JobContext) Clone() JobContext {
	return JobContext{
		Inputs:  maps.Clone(c.Inputs),
		Secrets: maps.Clone(c.Secrets),
		Tasks:   maps.Clone(c.Tasks),
		Job:     maps.Clone(c.Job),
	}
}

func (c JobContext) AsMap() map[string]any {
	return map[string]any{
		"inputs":  c.Inputs,
		"secrets": c.Secrets,
		"tasks":   c.Tasks,
		"job":     c.Job,
	}
}

func (d *JobDefaults) Clone() *JobDefaults {
	clone := JobDefaults{}
	if d.Limits != nil {
		clone.Limits = d.Limits.Clone()
	}
	if d.Retry != nil {
		clone.Retry = d.Retry.Clone()
	}
	clone.Queue = d.Queue
	clone.Timeout = d.Timeout
	clone.Priority = d.Priority
	return &clone
}

func NewJobSummary(j *Job) *JobSummary {
	return &JobSummary{
		ID:          j.ID,
		CreatedBy:   j.CreatedBy,
		ParentID:    j.ParentID,
		Name:        j.Name,
		Description: j.Description,
		Tags:        j.Tags,
		Inputs:      maps.Clone(j.Inputs),
		State:       j.State,
		CreatedAt:   j.CreatedAt,
		StartedAt:   j.StartedAt,
		CompletedAt: j.CompletedAt,
		FailedAt:    j.FailedAt,
		Position:    j.Position,
		TaskCount:   j.TaskCount,
		Result:      j.Result,
		Error:       j.Error,
		Progress:    j.Progress,
		Schedule:    j.Schedule,
	}
}

func NewScheduledJobSummary(sj *ScheduledJob) *ScheduledJobSummary {
	return &ScheduledJobSummary{
		ID:          sj.ID,
		CreatedBy:   sj.CreatedBy,
		Name:        sj.Name,
		State:       sj.State,
		Description: sj.Description,
		Tags:        sj.Tags,
		Inputs:      maps.Clone(sj.Inputs),
		Cron:        sj.Cron,
		CreatedAt:   sj.CreatedAt,
	}
}

func CloneWebhooks(webhooks []*Webhook) []*Webhook {
	copy := make([]*Webhook, len(webhooks))
	for i, w := range webhooks {
		copy[i] = w.Clone()
	}
	return copy
}

func (w *Webhook) Clone() *Webhook {
	return &Webhook{
		URL:     w.URL,
		Headers: maps.Clone(w.Headers),
		Event:   w.Event,
		If:      w.If,
	}
}

func ClonePermissions(perms []*Permission) []*Permission {
	copy := make([]*Permission, len(perms))
	for i, p := range perms {
		copy[i] = p.Clone()
	}
	return copy
}

func (p *Permission) Clone() *Permission {
	c := &Permission{}
	if p.Role != nil {
		c.Role = p.Role.Clone()
	} else {
		c.User = p.User.Clone()
	}
	return c
}

func (a *AutoDelete) Clone() *AutoDelete {
	return &AutoDelete{
		After: a.After,
	}
}
