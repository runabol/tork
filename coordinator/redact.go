package coordinator

import (
	"strings"

	"github.com/tork/job"
	"github.com/tork/task"
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

func redactTask(t task.Task) task.Task {
	// redact env vars
	t.Env = redactVars(t.Env)
	// redact pre tasks
	for i, p := range t.Pre {
		t.Pre[i] = redactTask(p)
	}
	// redact post tasks
	for i, p := range t.Post {
		t.Post[i] = redactTask(p)
	}
	return t
}

func redactJob(j job.Job) job.Job {
	// redact inputs
	j.Inputs = redactVars(j.Inputs)
	// redact context
	j.Context = redactRaw(j.Context)
	// redact tasks
	for i, t := range j.Tasks {
		j.Tasks[i] = redactTask(t)
	}
	return j
}

func redactRaw(m map[string]any) map[string]any {
	redacted := make(map[string]any)
	for k, rv := range m {
		for _, m := range matchers {
			if m(k) {
				rv = "[REDACTED]"
				break
			}
		}
		switch v := rv.(type) {
		case map[string]string:
			redacted[k] = redactVars(v)
		case map[string]any:
			redacted[k] = redactRaw(v)
		default:
			redacted[k] = v
		}
	}
	return redacted
}

func redactVars(m map[string]string) map[string]string {
	redacted := make(map[string]string)
	for k, v := range m {
		for _, m := range matchers {
			if m(k) {
				v = "[REDACTED]"
				break
			}
		}
		redacted[k] = v
	}
	return redacted
}
