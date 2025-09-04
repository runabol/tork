package log

import (
	"context"

	"github.com/runabol/tork"
)

type EventType string

const Read = "READ"

type HandlerFunc func(ctx context.Context, et EventType, l []*tork.TaskLogPart) error

func NoOpHandlerFunc(ctx context.Context, et EventType, l []*tork.TaskLogPart) error { return nil }

type MiddlewareFunc func(next HandlerFunc) HandlerFunc

func ApplyMiddleware(h HandlerFunc, mws []MiddlewareFunc) HandlerFunc {
	return func(ctx context.Context, et EventType, l []*tork.TaskLogPart) error {
		nx := next(ctx, 0, mws, h)
		return nx(ctx, et, l)
	}
}

func next(ctx context.Context, index int, mws []MiddlewareFunc, h HandlerFunc) HandlerFunc {
	if index >= len(mws) {
		return h
	}
	return mws[index](next(ctx, index+1, mws, h))
}
