package log

import (
	"context"

	"github.com/runabol/tork"
	"github.com/runabol/tork/internal/redact"
)

func Redact(redacter *redact.Redacter) MiddlewareFunc {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, et EventType, l []*tork.TaskLogPart) error {
			if et != Read {
				return next(ctx, et, l)
			}
			if err := redacter.RedactTaskLogParts(ctx, l); err != nil {
				return err
			}
			return next(ctx, et, l)
		}
	}
}
