package broker_test

import (
	"testing"

	"github.com/runabol/tork/broker"

	"github.com/stretchr/testify/assert"
)

func TestIsWorkerQueue(t *testing.T) {
	assert.Equal(t, true, broker.IsWorkerQueue("some-special-work-queues"))
	assert.Equal(t, true, broker.IsWorkerQueue(broker.QUEUE_DEFAULT))
	assert.Equal(t, false, broker.IsWorkerQueue(broker.QUEUE_COMPLETED))
	assert.Equal(t, false, broker.IsWorkerQueue(broker.QUEUE_ERROR))
	assert.Equal(t, false, broker.IsWorkerQueue(broker.QUEUE_STARTED))
	assert.Equal(t, false, broker.IsWorkerQueue(broker.QUEUE_PENDING))
}

func TestIsCoordinatorQueue(t *testing.T) {
	assert.Equal(t, false, broker.IsCoordinatorQueue("some-special-work-queues"))
	assert.Equal(t, false, broker.IsCoordinatorQueue(broker.QUEUE_DEFAULT))
	assert.Equal(t, true, broker.IsCoordinatorQueue(broker.QUEUE_COMPLETED))
	assert.Equal(t, true, broker.IsCoordinatorQueue(broker.QUEUE_ERROR))
	assert.Equal(t, true, broker.IsCoordinatorQueue(broker.QUEUE_STARTED))
	assert.Equal(t, true, broker.IsCoordinatorQueue(broker.QUEUE_PENDING))
}
