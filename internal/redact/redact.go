package redact

import (
	"strings"

	"github.com/runabol/tork"
	"github.com/runabol/tork/internal/wildcard"
)

const (
	redactedStr = "[REDACTED]"
)

type Redacter struct {
	matchers []Matcher
}

func NewRedacter(matchers ...Matcher) *Redacter {
	if len(matchers) == 0 {
		matchers = defaultMatchers
	}
	return &Redacter{
		matchers: matchers,
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
	redacted := t
	// redact env vars
	redacted.Env = r.redactVars(redacted.Env)
	// redact pre tasks
	for _, p := range redacted.Pre {
		r.RedactTask(p)
	}
	// redact post tasks
	for _, p := range redacted.Post {
		r.RedactTask(p)
	}
	// redact parallel tasks
	if redacted.Parallel != nil {
		for _, p := range redacted.Parallel.Tasks {
			r.RedactTask(p)
		}
	}
	// registry creds
	if redacted.Registry != nil {
		redacted.Registry.Password = redactedStr
	}
}

func (r *Redacter) RedactJob(j *tork.Job) {
	redacted := j
	// redact inputs
	redacted.Inputs = r.redactVars(redacted.Inputs)
	// redact webhook headers
	for _, w := range j.Webhooks {
		if w.Headers != nil {
			w.Headers = r.redactVars(w.Headers)
		}
	}
	// redact context
	redacted.Context.Inputs = r.redactVars(redacted.Context.Inputs)
	redacted.Context.Tasks = r.redactVars(redacted.Context.Tasks)
	// redact tasks
	for _, t := range redacted.Tasks {
		r.RedactTask(t)
	}
	// redact execution
	for _, t := range redacted.Execution {
		r.RedactTask(t)
	}
}

func (r *Redacter) redactVars(m map[string]string) map[string]string {
	redacted := make(map[string]string)
	for k, v := range m {
		for _, m := range r.matchers {
			if m(k) {
				v = redactedStr
				break
			}
		}
		redacted[k] = v
	}
	return redacted
}
