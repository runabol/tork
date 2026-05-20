package handlers

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/datastore/postgres"
	"github.com/runabol/tork/internal/uuid"
	"github.com/stretchr/testify/assert"
)

func Test_handleHeartbeat(t *testing.T) {
	ctx := context.Background()

	ds, err := postgres.NewTestDatastore()
	assert.NoError(t, err)
	handler := NewHeartbeatHandler(ds)
	assert.NotNil(t, handler)

	n1 := tork.Node{
		ID:              uuid.NewUUID(),
		LastHeartbeatAt: time.Now().UTC().Add(-time.Minute * 5),
		CPUPercent:      75,
		Hostname:        "host-1",
		Status:          tork.NodeStatusUP,
	}

	err = handler(ctx, &n1)
	assert.NoError(t, err)

	n11, err := ds.GetNodeByID(ctx, n1.ID)
	assert.NoError(t, err)
	assert.Equal(t, n1.LastHeartbeatAt.Unix(), n11.LastHeartbeatAt.Unix())
	assert.Equal(t, n1.CPUPercent, n11.CPUPercent)
	assert.Equal(t, tork.NodeStatusOffline, n11.Status)
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
	assert.Equal(t, n2.LastHeartbeatAt.Unix(), n22.LastHeartbeatAt.Unix())
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
	assert.Equal(t, n2.LastHeartbeatAt.Unix(), n33.LastHeartbeatAt.Unix()) // should keep the latest
	assert.Equal(t, n3.CPUPercent, n33.CPUPercent)
	assert.NoError(t, ds.Close())
}

func Test_handleHeartbeatLogFields(t *testing.T) {
	oldLogger := log.Logger
	var buf bytes.Buffer
	log.Logger = log.Logger.Output(&buf)
	defer func() {
		log.Logger = oldLogger
	}()

	ds := &nodeStubDatastore{}
	handler := NewHeartbeatHandler(ds)

	n := &tork.Node{
		ID:              uuid.NewUUID(),
		LastHeartbeatAt: time.Now().UTC(),
		Hostname:        "worker-42",
	}

	err := handler(context.Background(), n)
	assert.NoError(t, err)

	logOutput := buf.String()
	assert.Contains(t, logOutput, `"node-hostname":"worker-42"`)
	assert.Contains(t, logOutput, `"node-id":"`)
	assert.Contains(t, logOutput, "received first heartbeat")
	assert.NotContains(t, logOutput, `"hostname":`)
}

type nodeStubDatastore struct{}

func (s *nodeStubDatastore) GetNodeByID(context.Context, string) (*tork.Node, error) {
	return nil, datastore.ErrNodeNotFound
}
func (s *nodeStubDatastore) CreateNode(_ context.Context, _ *tork.Node) error { return nil }
func (s *nodeStubDatastore) UpdateNode(_ context.Context, _ string, _ func(*tork.Node) error) error {
	return nil
}
func (s *nodeStubDatastore) GetActiveNodes(context.Context) ([]*tork.Node, error) {
	return nil, nil
}
func (s *nodeStubDatastore) CreateTask(context.Context, *tork.Task) error { return nil }
func (s *nodeStubDatastore) UpdateTask(context.Context, string, func(*tork.Task) error) error {
	return nil
}
func (s *nodeStubDatastore) GetTaskByID(context.Context, string) (*tork.Task, error) {
	return nil, nil
}
func (s *nodeStubDatastore) GetActiveTasks(context.Context, string) ([]*tork.Task, error) {
	return nil, nil
}
func (s *nodeStubDatastore) GetNextTask(context.Context, string) (*tork.Task, error) {
	return nil, nil
}
func (s *nodeStubDatastore) CreateTaskLogPart(context.Context, *tork.TaskLogPart) error { return nil }
func (s *nodeStubDatastore) GetTaskLogParts(context.Context, string, string, int, int) (*datastore.Page[*tork.TaskLogPart], error) {
	return nil, nil
}
func (s *nodeStubDatastore) CreateJob(context.Context, *tork.Job) error { return nil }
func (s *nodeStubDatastore) UpdateJob(context.Context, string, func(*tork.Job) error) error {
	return nil
}
func (s *nodeStubDatastore) GetJobByID(context.Context, string) (*tork.Job, error) { return nil, nil }
func (s *nodeStubDatastore) GetJobLogParts(context.Context, string, string, int, int) (*datastore.Page[*tork.TaskLogPart], error) {
	return nil, nil
}
func (s *nodeStubDatastore) GetJobs(context.Context, string, string, int, int) (*datastore.Page[*tork.JobSummary], error) {
	return nil, nil
}
func (s *nodeStubDatastore) CreateScheduledJob(context.Context, *tork.ScheduledJob) error {
	return nil
}
func (s *nodeStubDatastore) GetActiveScheduledJobs(context.Context) ([]*tork.ScheduledJob, error) {
	return nil, nil
}
func (s *nodeStubDatastore) GetScheduledJobs(context.Context, string, int, int) (*datastore.Page[*tork.ScheduledJobSummary], error) {
	return nil, nil
}
func (s *nodeStubDatastore) GetScheduledJobByID(context.Context, string) (*tork.ScheduledJob, error) {
	return nil, nil
}
func (s *nodeStubDatastore) UpdateScheduledJob(context.Context, string, func(*tork.ScheduledJob) error) error {
	return nil
}
func (s *nodeStubDatastore) DeleteScheduledJob(context.Context, string) error    { return nil }
func (s *nodeStubDatastore) CreateUser(context.Context, *tork.User) error        { return nil }
func (s *nodeStubDatastore) GetUser(context.Context, string) (*tork.User, error) { return nil, nil }
func (s *nodeStubDatastore) CreateRole(context.Context, *tork.Role) error        { return nil }
func (s *nodeStubDatastore) GetRole(context.Context, string) (*tork.Role, error) { return nil, nil }
func (s *nodeStubDatastore) GetRoles(context.Context) ([]*tork.Role, error)      { return nil, nil }
func (s *nodeStubDatastore) GetUserRoles(context.Context, string) ([]*tork.Role, error) {
	return nil, nil
}
func (s *nodeStubDatastore) AssignRole(context.Context, string, string) error   { return nil }
func (s *nodeStubDatastore) UnassignRole(context.Context, string, string) error { return nil }
func (s *nodeStubDatastore) GetMetrics(context.Context) (*tork.Metrics, error)  { return nil, nil }
func (s *nodeStubDatastore) WithTx(_ context.Context, _ func(datastore.Datastore) error) error {
	return nil
}
func (s *nodeStubDatastore) HealthCheck(context.Context) error { return nil }
