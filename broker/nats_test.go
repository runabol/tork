package broker

import (
	"context"
	"testing"

	"github.com/runabol/tork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewNATSBroker(t *testing.T) {
	broker, err := NewNATSBroker("")
	require.NoError(t, err)
	assert.NotNil(t, broker)
	assert.NotNil(t, broker.server)
	assert.NotNil(t, broker.conn)
	broker.Shutdown(context.Background())
}

func TestPublishTask(t *testing.T) {
	broker, err := NewNATSBroker("")
	require.NoError(t, err)
	defer broker.Shutdown(context.Background())

	task := &tork.Task{ID: "test-task"}
	err = broker.PublishTask(context.Background(), "test-queue", task)
	assert.NoError(t, err)
}

func TestSubscribeForTasks(t *testing.T) {
	broker, err := NewNATSBroker("")
	require.NoError(t, err)
	defer broker.Shutdown(context.Background())

	task := &tork.Task{ID: "test-task"}
	err = broker.PublishTask(context.Background(), "test-queue", task)
	require.NoError(t, err)

	err = broker.SubscribeForTasks("test-queue", func(tk *tork.Task) error {
		assert.Equal(t, "test-task", tk.ID)
		return nil
	})
	assert.NoError(t, err)
}

func TestPublishHeartbeat(t *testing.T) {
	broker, err := NewNATSBroker("")
	require.NoError(t, err)
	defer broker.Shutdown(context.Background())

	node := &tork.Node{ID: "test-node"}
	err = broker.PublishHeartbeat(context.Background(), node)
	assert.NoError(t, err)
}

func TestSubscribeForHeartbeats(t *testing.T) {
	broker, err := NewNATSBroker("")
	require.NoError(t, err)
	defer broker.Shutdown(context.Background())

	node := &tork.Node{ID: "test-node"}
	err = broker.PublishHeartbeat(context.Background(), node)
	require.NoError(t, err)

	err = broker.SubscribeForHeartbeats(func(n *tork.Node) error {
		assert.Equal(t, "test-node", n.ID)
		return nil
	})
	assert.NoError(t, err)
}

func TestPublishJob(t *testing.T) {
	broker, err := NewNATSBroker("")
	require.NoError(t, err)
	defer broker.Shutdown(context.Background())

	job := &tork.Job{ID: "test-job"}
	err = broker.PublishJob(context.Background(), job)
	assert.NoError(t, err)
}

func TestSubscribeForJobs(t *testing.T) {
	broker, err := NewNATSBroker("")
	require.NoError(t, err)
	defer broker.Shutdown(context.Background())

	job := &tork.Job{ID: "test-job"}
	err = broker.PublishJob(context.Background(), job)
	require.NoError(t, err)

	err = broker.SubscribeForJobs(func(j *tork.Job) error {
		assert.Equal(t, "test-job", j.ID)
		return nil
	})
	assert.NoError(t, err)
}

func TestHealthCheck(t *testing.T) {
	broker, err := NewNATSBroker("")
	require.NoError(t, err)
	defer broker.Shutdown(context.Background())

	err = broker.HealthCheck(context.Background())
	assert.NoError(t, err)

	broker.Shutdown(context.Background())
	err = broker.HealthCheck(context.Background())
	assert.Error(t, err)
}

func TestPublishEvent(t *testing.T) {
	broker, err := NewNATSBroker("")
	require.NoError(t, err)
	defer broker.Shutdown(context.Background())

	event := map[string]string{"key": "value"}
	err = broker.PublishEvent(context.Background(), "test-topic", event)
	assert.NoError(t, err)
}

func TestSubscribeForEvents(t *testing.T) {
	broker, err := NewNATSBroker("")
	require.NoError(t, err)
	defer broker.Shutdown(context.Background())

	event := map[string]string{"key": "value"}
	err = broker.PublishEvent(context.Background(), "test-topic", event)
	require.NoError(t, err)

	err = broker.SubscribeForEvents(context.Background(), "test-topic", func(e any) {
		data, ok := e.(map[string]string)
		require.True(t, ok)
		assert.Equal(t, "value", data["key"])
	})
	assert.NoError(t, err)
}

func TestPublishTaskLogPart(t *testing.T) {
	broker, err := NewNATSBroker("")
	require.NoError(t, err)
	defer broker.Shutdown(context.Background())

	logPart := &tork.TaskLogPart{TaskID: "test-task"}
	err = broker.PublishTaskLogPart(context.Background(), logPart)
	assert.NoError(t, err)
}

func TestSubscribeForTaskLogPart(t *testing.T) {
	broker, err := NewNATSBroker("")
	require.NoError(t, err)
	defer broker.Shutdown(context.Background())

	logPart := &tork.TaskLogPart{TaskID: "test-task"}
	err = broker.PublishTaskLogPart(context.Background(), logPart)
	require.NoError(t, err)

	err = broker.SubscribeForTaskLogPart(func(p *tork.TaskLogPart) {
		assert.Equal(t, "test-task", p.TaskID)
	})
	assert.NoError(t, err)
}

func TestPublishTaskProgress(t *testing.T) {
	broker, err := NewNATSBroker("")
	require.NoError(t, err)
	defer broker.Shutdown(context.Background())

	task := &tork.Task{ID: "test-task"}
	err = broker.PublishTaskProgress(context.Background(), task)
	assert.NoError(t, err)
}

func TestSubscribeForTaskProgress(t *testing.T) {
	broker, err := NewNATSBroker("")
	require.NoError(t, err)
	defer broker.Shutdown(context.Background())

	task := &tork.Task{ID: "test-task"}
	err = broker.PublishTaskProgress(context.Background(), task)
	require.NoError(t, err)

	err = broker.SubscribeForTaskProgress(func(tp *tork.Task) error {
		assert.Equal(t, "test-task", tp.ID)
		return nil
	})
	assert.NoError(t, err)
}
