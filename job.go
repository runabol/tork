package tork

import (
	"time"

	"golang.org/x/exp/maps"
)

type JobState string

const (
	JobStatePending   JobState = "PENDING"
	JobStateScheduled JobState = "SCHEDULED"
	JobStateRunning   JobState = "RUNNING"
	JobStateCancelled JobState = "CANCELLED"
	JobStateCompleted JobState = "COMPLETED"
	JobStateFailed    JobState = "FAILED"
	JobStateRestart   JobState = "RESTART"
)

type Job struct {
	ID          string            `json:"id,omitempty"`
	ParentID    string            `json:"parentId,omitempty"`
	Name        string            `json:"name,omitempty"`
	Description string            `json:"description,omitempty"`
	State       JobState          `json:"state,omitempty"`
	CreatedAt   time.Time         `json:"createdAt,omitempty"`
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
}

type JobSummary struct {
	ID          string            `json:"id,omitempty"`
	ParentID    string            `json:"parentId,omitempty"`
	Inputs      map[string]string `json:"inputs,omitempty"`
	Name        string            `json:"name,omitempty"`
	Description string            `json:"description,omitempty"`
	State       JobState          `json:"state,omitempty"`
	CreatedAt   time.Time         `json:"createdAt,omitempty"`
	StartedAt   *time.Time        `json:"startedAt,omitempty"`
	CompletedAt *time.Time        `json:"completedAt,omitempty"`
	FailedAt    *time.Time        `json:"failedAt,omitempty"`
	Position    int               `json:"position"`
	TaskCount   int               `json:"taskCount,omitempty"`
	Result      string            `json:"result,omitempty"`
	Error       string            `json:"error,omitempty"`
}

type JobContext struct {
	Job    map[string]string `json:"job,omitempty"`
	Inputs map[string]string `json:"inputs,omitempty"`
	Tasks  map[string]string `json:"tasks,omitempty"`
}

type JobDefaults struct {
	Retry   *TaskRetry  `json:"retry,omitempty"`
	Limits  *TaskLimits `json:"limits,omitempty"`
	Timeout string      `json:"timeout,omitempty"`
	Queue   string      `json:"queue,omitempty"`
}

type Webhook struct {
	URL     string            `json:"url,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Event   string            `json:"event,omitempty"`
}

func (j *Job) Clone() *Job {
	var defaults *JobDefaults
	if j.Defaults != nil {
		defaults = j.Defaults.Clone()
	}
	return &Job{
		ID:          j.ID,
		Name:        j.Name,
		Description: j.Description,
		State:       j.State,
		CreatedAt:   j.CreatedAt,
		StartedAt:   j.StartedAt,
		CompletedAt: j.CompletedAt,
		FailedAt:    j.FailedAt,
		Tasks:       CloneTasks(j.Tasks),
		Execution:   CloneTasks(j.Execution),
		Position:    j.Position,
		Inputs:      maps.Clone(j.Inputs),
		Context:     j.Context.Clone(),
		ParentID:    j.ParentID,
		TaskCount:   j.TaskCount,
		Output:      j.Output,
		Result:      j.Result,
		Error:       j.Error,
		Defaults:    defaults,
		Webhooks:    CloneWebhooks(j.Webhooks),
	}
}

func (c JobContext) Clone() JobContext {
	return JobContext{
		Inputs: maps.Clone(c.Inputs),
		Tasks:  maps.Clone(c.Tasks),
		Job:    maps.Clone(c.Job),
	}
}

func (c JobContext) AsMap() map[string]any {
	return map[string]any{
		"inputs": c.Inputs,
		"tasks":  c.Tasks,
		"job":    c.Job,
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
	return &clone
}

func NewJobSummary(j *Job) *JobSummary {
	return &JobSummary{
		ID:          j.ID,
		ParentID:    j.ParentID,
		Name:        j.Name,
		Description: j.Description,
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
	}
}

func CloneWebhooks(webhooks []*Webhook) []*Webhook {
	copy := make([]*Webhook, len(webhooks))
	for i, w := range webhooks {
		copy[i] = w.Clone()
	}
	return copy
}

func (w Webhook) Clone() *Webhook {
	return &Webhook{
		URL:     w.URL,
		Headers: maps.Clone(w.Headers),
		Event:   w.Event,
	}
}
