package handlers

import (
	"context"
	"testing"
	"time"

	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore/inmemory"
	"github.com/runabol/tork/internal/uuid"
	"github.com/stretchr/testify/assert"
)

func Test_handleHeartbeat(t *testing.T) {
	ctx := context.Background()

	ds := inmemory.NewInMemoryDatastore()
	handler := NewHeartbeatHandler(ds)
	assert.NotNil(t, handler)

	n1 := tork.Node{
		ID:              uuid.NewUUID(),
		LastHeartbeatAt: time.Now().UTC().Add(-time.Minute * 5),
		CPUPercent:      75,
		Hostname:        "host-1",
		Status:          tork.NodeStatusUP,
	}

	err := handler(ctx, &n1)
	assert.NoError(t, err)

	n11, err := ds.GetNodeByID(ctx, n1.ID)
	assert.NoError(t, err)
	assert.Equal(t, n1.LastHeartbeatAt, n11.LastHeartbeatAt)
	assert.Equal(t, n1.CPUPercent, n11.CPUPercent)
	assert.Equal(t, n1.Status, n11.Status)
	assert.Equal(t, n1.TaskCount, n11.TaskCount)

	n2 := tork.Node{
		ID:              n1.ID,
		LastHeartbeatAt: time.Now().UTC().Add(-time.Minute * 2),
		CPUPercent:      75,
		Status:          tork.NodeStatusDown,
		TaskCount:       3,
	}

	err = handler(ctx, &n2)
	assert.NoError(t, err)

	n22, err := ds.GetNodeByID(ctx, n1.ID)
	assert.NoError(t, err)
	assert.Equal(t, n2.LastHeartbeatAt, n22.LastHeartbeatAt)
	assert.Equal(t, n2.CPUPercent, n22.CPUPercent)
	assert.Equal(t, n2.Status, n22.Status)
	assert.Equal(t, n2.TaskCount, n22.TaskCount)

	n3 := tork.Node{
		ID:              n1.ID,
		LastHeartbeatAt: time.Now().UTC().Add(-time.Minute * 7),
		CPUPercent:      75,
	}

	err = handler(ctx, &n3)
	assert.NoError(t, err)

	n33, err := ds.GetNodeByID(ctx, n1.ID)
	assert.NoError(t, err)
	assert.Equal(t, n2.LastHeartbeatAt, n33.LastHeartbeatAt) // should keep the latest
	assert.Equal(t, n3.CPUPercent, n33.CPUPercent)

}
