package task

import (
	"context"
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/runabol/tork"
)

type HostEnv struct {
	vars map[string]string
}

func NewHostEnv(vars ...string) (*HostEnv, error) {
	varsMap := make(map[string]string, 0)
	for _, varSpec := range vars {
		parsed := strings.Split(varSpec, ":")
		if len(parsed) == 1 {
			varsMap[varSpec] = varSpec
		} else if len(parsed) == 2 {
			varsMap[parsed[0]] = parsed[1]
		} else {
			return nil, errors.Errorf("invalid env var spec: %s", varSpec)
		}
	}
	return &HostEnv{vars: varsMap}, nil
}

func (m *HostEnv) Execute(next HandlerFunc) HandlerFunc {
	return func(ctx context.Context, et EventType, t *tork.Task) error {
		if et == StateChange && t.State == tork.TaskStateScheduled {
			m.setHostVars(t)
		}
		return next(ctx, et, t)
	}
}

func (m *HostEnv) setHostVars(t *tork.Task) {
	if t.Env == nil {
		t.Env = make(map[string]string)
	}
	for name, alias := range m.vars {
		t.Env[alias] = os.Getenv(name)
	}
	for _, pre := range t.Pre {
		m.setHostVars(pre)
	}
	for _, post := range t.Post {
		m.setHostVars(post)
	}
}
