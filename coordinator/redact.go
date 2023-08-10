package coordinator

import (
	"strings"

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

func redact(t task.Task) *task.Task {
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
	return &t
}
