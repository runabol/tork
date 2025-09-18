package redact

import (
	"context"
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/internal/wildcard"
)

const (
	redactedStr = "[REDACTED]"
)

type Redacter struct {
	matchers []Matcher
	ds       datastore.Datastore
}

func NewRedacter(ds datastore.Datastore, matchers ...Matcher) *Redacter {
	if len(matchers) == 0 {
		matchers = defaultMatchers
	}
	return &Redacter{
		matchers: matchers,
		ds:       ds,
	}
}

type Matcher func(string) bool

var defaultMatchers = []Matcher{
	Contains("SECRET"),
	Contains("PASSWORD"),
	Contains("ACCESS_KEY"),
}

func Contains(substr string) func(s string) bool {
	return func(s string) bool {
		return strings.Contains(strings.ToUpper(s), strings.ToUpper(substr))
	}
}

func Wildcard(pattern string) func(s string) bool {
	return func(s string) bool {
		return wildcard.Match(pattern, s)
	}
}

func (r *Redacter) RedactTask(t *tork.Task) {
	job, err := r.ds.GetJobByID(context.Background(), t.JobID)
	if err != nil {
		log.Error().Err(err).Msgf("error getting job for task %s", t.ID)
		return
	}
	r.doRedactTask(t, job.Secrets)
}

func (r *Redacter) RedactTaskLogPart(p *tork.TaskLogPart, secrets map[string]string) {
	contents := p.Contents
	for _, secret := range secrets {
		contents = strings.ReplaceAll(contents, secret, redactedStr)
	}
	p.Contents = contents
}

func (r *Redacter) doRedactTask(t *tork.Task, secrets map[string]string) {
	redacted := t
	// redact env vars
	redacted.Env = r.redactVars(redacted.Env, secrets)
	// redact mounts
	for _, m := range redacted.Mounts {
		m.Opts = r.redactVars(m.Opts, secrets)
	}
	// redact pre tasks
	for _, p := range redacted.Pre {
		r.doRedactTask(p, secrets)
	}
	// redact post tasks
	for _, p := range redacted.Post {
		r.doRedactTask(p, secrets)
	}
	// redact parallel tasks
	if redacted.Parallel != nil {
		for _, p := range redacted.Parallel.Tasks {
			r.doRedactTask(p, secrets)
		}
	}
	// registry creds
	if redacted.Registry != nil {
		redacted.Registry.Password = redactedStr
	}
	if redacted.SubJob != nil {
		for k := range redacted.SubJob.Secrets {
			redacted.SubJob.Secrets[k] = redactedStr
		}
	}
}

func (r *Redacter) RedactJob(j *tork.Job) {
	redacted := j
	// redact inputs
	redacted.Inputs = r.redactVars(redacted.Inputs, j.Secrets)
	// redact webhook headers
	for _, w := range j.Webhooks {
		if w.Headers != nil {
			w.Headers = r.redactVars(w.Headers, j.Secrets)
		}
	}
	// redact context
	redacted.Context.Inputs = r.redactVars(redacted.Context.Inputs, j.Secrets)
	redacted.Context.Secrets = r.redactVars(redacted.Context.Secrets, j.Secrets)
	redacted.Context.Tasks = r.redactVars(redacted.Context.Tasks, j.Secrets)
	// redact tasks
	for _, t := range redacted.Tasks {
		r.doRedactTask(t, j.Secrets)
	}
	// redact execution
	for _, t := range redacted.Execution {
		r.doRedactTask(t, j.Secrets)
	}
	for k := range j.Secrets {
		redacted.Secrets[k] = redactedStr
	}
}

func (r *Redacter) redactVars(m map[string]string, secrets map[string]string) map[string]string {
	redacted := make(map[string]string)
	for k, v := range m {
		for _, m := range r.matchers {
			if m(k) {
				v = redactedStr
				break
			}
		}
		for _, secret := range secrets {
			if secret == v {
				v = redactedStr
			}
		}
		redacted[k] = v
	}
	return redacted
}
