package job

import (
	"context"
	"errors"
	"testing"

	"github.com/runabol/tork"

	"github.com/stretchr/testify/assert"
)

func TestMiddlewareBefore(t *testing.T) {
	order := 1
	h := func(ctx context.Context, _ EventType, j *tork.Job) error {
		assert.Equal(t, 3, order)
		return nil
	}
	mw1 := func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, et EventType, j *tork.Job) error {
			assert.Equal(t, 1, order)
			order = order + 1
			return next(ctx, et, j)
		}
	}
	mw2 := func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, et EventType, j *tork.Job) error {
			assert.Equal(t, 2, order)
			order = order + 1
			return next(ctx, et, j)
		}
	}
	hm := ApplyMiddleware(h, []MiddlewareFunc{mw1, mw2})
	assert.NoError(t, hm(context.Background(), StateChange, &tork.Job{}))
}

func TestMiddlewareAfter(t *testing.T) {
	order := 1
	h := func(ctx context.Context, _ EventType, j *tork.Job) error {
		assert.Equal(t, 1, order)
		order = order + 1
		return nil
	}
	mw1 := func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, et EventType, j *tork.Job) error {
			assert.NoError(t, next(ctx, et, j))
			assert.Equal(t, 3, order)
			order = order + 1
			return nil
		}
	}
	mw2 := func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, et EventType, j *tork.Job) error {
			assert.NoError(t, next(ctx, et, j))
			assert.Equal(t, 2, order)
			order = order + 1
			return nil
		}
	}
	hm := ApplyMiddleware(h, []MiddlewareFunc{mw1, mw2})
	assert.NoError(t, hm(context.Background(), StateChange, &tork.Job{}))
}

func TestNoMiddleware(t *testing.T) {
	order := 1
	h := func(ctx context.Context, _ EventType, j *tork.Job) error {
		assert.Equal(t, 1, order)
		order = order + 1
		return nil
	}
	hm := ApplyMiddleware(h, []MiddlewareFunc{})
	assert.NoError(t, hm(context.Background(), StateChange, &tork.Job{}))
}

func TestMiddlewareError(t *testing.T) {
	Err := errors.New("something bad happened")
	h := func(ctx context.Context, _ EventType, j *tork.Job) error {
		panic(1) // should not get here
	}
	mw1 := func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, _ EventType, j *tork.Job) error {
			return Err
		}
	}
	mw2 := func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, _ EventType, j *tork.Job) error {
			panic(1) // should not get here
		}
	}
	hm := ApplyMiddleware(h, []MiddlewareFunc{mw1, mw2})
	assert.ErrorIs(t, hm(context.Background(), StateChange, &tork.Job{}), Err)
}
