package handlers

import (
	"context"

	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore"
)

type logHandler struct {
	ds datastore.Datastore
}

func NewLogHandler(ds datastore.Datastore) func(p *tork.TaskLogPart) {
	h := &logHandler{
		ds: ds,
	}
	return h.handle
}

func (h *logHandler) handle(p *tork.TaskLogPart) {
	ctx := context.Background()
	log.Debug().Msgf("[Task][%s] %s", p.TaskID, p.Contents)
	if err := h.ds.CreateTaskLogPart(ctx, p); err != nil {
		log.Error().Err(err).Msgf("error writing task log: %s", err.Error())
	}
}
