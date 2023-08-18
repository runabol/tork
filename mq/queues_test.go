package mq_test

import (
	"testing"

	"github.com/runabol/tork/mq"
	"github.com/stretchr/testify/assert"
)

func TestIsWorkerQueue(t *testing.T) {
	assert.Equal(t, true, mq.IsWorkerQueue("some-special-work-queues"))
	assert.Equal(t, true, mq.IsWorkerQueue(mq.QUEUE_DEFAULT))
	assert.Equal(t, false, mq.IsWorkerQueue(mq.QUEUE_COMPLETED))
	assert.Equal(t, false, mq.IsWorkerQueue(mq.QUEUE_ERROR))
	assert.Equal(t, false, mq.IsWorkerQueue(mq.QUEUE_STARTED))
	assert.Equal(t, false, mq.IsWorkerQueue(mq.QUEUE_PENDING))
}

func TestIsCoordinatorQueue(t *testing.T) {
	assert.Equal(t, false, mq.IsCoordinatorQueue("some-special-work-queues"))
	assert.Equal(t, false, mq.IsCoordinatorQueue(mq.QUEUE_DEFAULT))
	assert.Equal(t, true, mq.IsCoordinatorQueue(mq.QUEUE_COMPLETED))
	assert.Equal(t, true, mq.IsCoordinatorQueue(mq.QUEUE_ERROR))
	assert.Equal(t, true, mq.IsCoordinatorQueue(mq.QUEUE_STARTED))
	assert.Equal(t, true, mq.IsCoordinatorQueue(mq.QUEUE_PENDING))
}
