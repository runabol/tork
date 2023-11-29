package task

import (
	"context"

	"github.com/runabol/tork"
	"github.com/runabol/tork/internal/redact"
)

func Redact(redacter *redact.Redacter) MiddlewareFunc {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, et EventType, t *tork.Task) error {
			if et == Read {
				redacter.RedactTask(t)
			}
			return next(ctx, et, t)
		}
	}
}
