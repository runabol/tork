package coordinator

import (
	"strings"

	"github.com/runabol/tork/job"
	"github.com/runabol/tork/task"
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

func redactTask(t *task.Task) *task.Task {
	redacted := t.Clone()
	// redact env vars
	redacted.Env = redactVars(redacted.Env)
	// redact pre tasks
	for i, p := range redacted.Pre {
		redacted.Pre[i] = redactTask(p)
	}
	// redact post tasks
	for i, p := range redacted.Post {
		redacted.Post[i] = redactTask(p)
	}
	// redact parallel tasks
	for i, p := range redacted.Parallel {
		redacted.Parallel[i] = redactTask(p)
	}
	return redacted
}

func redactJob(j *job.Job) *job.Job {
	redacted := j.Clone()
	// redact inputs
	redacted.Inputs = redactVars(redacted.Inputs)
	// redact context
	redacted.Context.Inputs = redactVars(redacted.Context.Inputs)
	redacted.Context.Tasks = redactVars(redacted.Context.Tasks)
	// redact tasks
	for i, t := range redacted.Tasks {
		redacted.Tasks[i] = redactTask(t)
	}
	// redact execution
	for i, t := range redacted.Execution {
		redacted.Execution[i] = redactTask(t)
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
