package redact

import (
	"context"
	"maps"
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
	matchers   []Matcher
	exclusions []Matcher
	ds         datastore.Datastore
}

func NewRedacter(ds datastore.Datastore) *Redacter {
	return NewRedacterWithMatchers(ds, nil)
}

func NewRedacterWithMatchers(ds datastore.Datastore, exclusions []Matcher, matchers ...Matcher) *Redacter {
	if len(matchers) == 0 {
		matchers = defaultMatchers
	}
	return &Redacter{
		matchers:   matchers,
		exclusions: exclusions,
		ds:         ds,
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
	for name, secret := range secrets {
		if r.isExcluded(name) {
			continue
		}
		if strings.TrimSpace(secret) == "" {
			continue
		}
		// if the secret appears more than once, redact the whole line
		if strings.Count(contents, secret) > 1 {
			contents = redactedStr
			break
		}
		contents = strings.ReplaceAll(contents, secret, redactedStr)
	}
	p.Contents = contents
}

func (r *Redacter) RedactTaskLogParts(ctx context.Context, parts []*tork.TaskLogPart) error {
	if len(parts) == 0 {
		return nil
	}
	task, err := r.ds.GetTaskByID(ctx, parts[0].TaskID)
	if err != nil {
		log.Error().Err(err).Msg("error getting task for log")
		return err
	}
	job, err := r.ds.GetJobByID(ctx, task.JobID)
	if err != nil {
		log.Error().Err(err).Msg("error getting job for log")
		return err
	}
	for _, p := range parts {
		r.RedactTaskLogPart(p, job.Secrets)
	}
	return nil
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
	// redact subjob secrets
	if redacted.SubJob != nil {
		for k := range redacted.SubJob.Secrets {
			redacted.SubJob.Secrets[k] = redactedStr
		}
		// redact webhook headers
		for _, w := range redacted.SubJob.Webhooks {
			if w.Headers != nil {
				w.Headers = r.redactVars(w.Headers, secrets)
			}
		}
	}
}

func (r *Redacter) RedactJob(j *tork.Job) {
	secrets := maps.Clone(j.Secrets)
	redacted := j
	// redact inputs
	redacted.Inputs = r.redactVars(redacted.Inputs, secrets)
	// redact secrets
	redacted.Secrets = r.redactVars(redacted.Secrets, secrets)
	// redact webhook headers
	for _, w := range j.Webhooks {
		if w.Headers != nil {
			w.Headers = r.redactVars(w.Headers, secrets)
		}
	}
	// redact context
	redacted.Context.Inputs = r.redactVars(redacted.Context.Inputs, secrets)
	redacted.Context.Secrets = r.redactVars(redacted.Context.Secrets, secrets)
	redacted.Context.Tasks = r.redactVars(redacted.Context.Tasks, secrets)
	// redact tasks
	for _, t := range redacted.Tasks {
		r.doRedactTask(t, secrets)
	}
	// redact execution
	for _, t := range redacted.Execution {
		r.doRedactTask(t, secrets)
	}
}

func (r *Redacter) redactVars(m map[string]string, secrets map[string]string) map[string]string {
	redacted := make(map[string]string)
	for k, v := range m {
		redacted[k] = r.redactVar(k, v, secrets)
	}
	return redacted
}

func (r *Redacter) redactVar(k, v string, secrets map[string]string) string {
	if !r.isExcluded(k) {
		for _, m := range r.matchers {
			if m(k) {
				return redactedStr
			}
		}
	}
	for name, secret := range secrets {
		if r.isExcluded(name) {
			continue
		}
		if strings.TrimSpace(secret) == "" {
			continue
		}
		if strings.Contains(v, secret) {
			return redactedStr
		}
	}
	return v
}

func (r *Redacter) isExcluded(k string) bool {
	for _, m := range r.exclusions {
		if m(k) {
			return true
		}
	}
	return false
}
