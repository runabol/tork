package handlers

import (
	"context"

	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/middleware/node"
)

type heartbeatHandler struct {
	ds datastore.Datastore
}

func NewHeartbeatHandler(ds datastore.Datastore) node.HandlerFunc {
	h := &heartbeatHandler{
		ds: ds,
	}
	return h.handle
}

func (h *heartbeatHandler) handle(ctx context.Context, n *tork.Node) error {
	_, err := h.ds.GetNodeByID(ctx, n.ID)
	if err == datastore.ErrNodeNotFound {
		log.Info().
			Str("node-id", n.ID).
			Str("hostname", n.Hostname).
			Msg("received first heartbeat")
		return h.ds.CreateNode(ctx, n)
	}
	return h.ds.UpdateNode(ctx, n.ID, func(u *tork.Node) error {
		// ignore "old" heartbeats
		if u.LastHeartbeatAt.After(n.LastHeartbeatAt) {
			return nil
		}
		u.LastHeartbeatAt = n.LastHeartbeatAt
		u.CPUPercent = n.CPUPercent
		u.Status = n.Status
		u.TaskCount = n.TaskCount
		return nil
	})
}
