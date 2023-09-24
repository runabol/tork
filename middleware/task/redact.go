package task

import (
	"context"

	"github.com/runabol/tork"
	"github.com/runabol/tork/internal/redact"
)

func Redact(next HandlerFunc) HandlerFunc {
	return func(ctx context.Context, et EventType, t *tork.Task) error {
		if et == Read {
			redact.Task(t)
		}
		return next(ctx, et, t)
	}
}
