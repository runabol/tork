package redact

import (
	"strings"

	"github.com/runabol/tork"
)

const (
	redactedStr = "[REDACTED]"
)

type matcher func(string) bool

var matchers = []matcher{
	contains("SECRET"),
	contains("PASSWORD"),
	contains("ACCESS_KEY"),
}

func contains(substr string) func(s string) bool {
	return func(s string) bool {
		return strings.Contains(strings.ToUpper(s), substr)
	}
}

func Task(t *tork.Task) *tork.Task {
	redacted := t.Clone()
	// redact env vars
	redacted.Env = redactVars(redacted.Env)
	// redact pre tasks
	for i, p := range redacted.Pre {
		redacted.Pre[i] = Task(p)
	}
	// redact post tasks
	for i, p := range redacted.Post {
		redacted.Post[i] = Task(p)
	}
	// redact parallel tasks
	if redacted.Parallel != nil {
		for i, p := range redacted.Parallel.Tasks {
			redacted.Parallel.Tasks[i] = Task(p)
		}
	}
	// registry creds
	if redacted.Registry != nil {
		redacted.Registry.Password = redactedStr
	}
	return redacted
}

func Job(j *tork.Job) {
	redacted := j
	// redact inputs
	redacted.Inputs = redactVars(redacted.Inputs)
	// redact context
	redacted.Context.Inputs = redactVars(redacted.Context.Inputs)
	redacted.Context.Tasks = redactVars(redacted.Context.Tasks)
	// redact tasks
	for i, t := range redacted.Tasks {
		redacted.Tasks[i] = Task(t)
	}
	// redact execution
	for i, t := range redacted.Execution {
		redacted.Execution[i] = Task(t)
	}
}

func redactVars(m map[string]string) map[string]string {
	redacted := make(map[string]string)
	for k, v := range m {
		for _, m := range matchers {
			if m(k) {
				v = redactedStr
				break
			}
		}
		redacted[k] = v
	}
	return redacted
}
