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
	redacted := make(map[string]string)
	for k, v := range t.Env {
		for _, m := range matchers {
			if m(k) {
				v = "[REDACTED]"
			}
		}
		redacted[k] = v
	}
	t.Env = redacted
	return t
}

func redactJob(j job.Job) job.Job {
	tasks := make([]task.Task, len(j.Tasks))
	for i, t := range j.Tasks {
		tasks[i] = redactTask(t)
	}
	j.Tasks = tasks
	return j
}
