package task

import (
	"context"

	"github.com/runabol/tork"
)

type HandlerFunc func(context.Context, *tork.Task) error

type MiddlewareFunc func(next HandlerFunc) HandlerFunc

func ApplyMiddleware(h HandlerFunc, mws []MiddlewareFunc) HandlerFunc {
	return func(ctx context.Context, t *tork.Task) error {
		nx := next(ctx, 0, mws, h)
		return nx(ctx, t)
	}
}

func next(ctx context.Context, index int, mws []MiddlewareFunc, h HandlerFunc) HandlerFunc {
	if index >= len(mws) {
		return h
	}
	return mws[index](next(ctx, index+1, mws, h))
}
