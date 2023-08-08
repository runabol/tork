package mq_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tork/mq"
)

func TestIsWorkQueue(t *testing.T) {
	assert.Equal(t, true, mq.IsWorkQueue("some-special-work-queues"))
	assert.Equal(t, true, mq.IsWorkQueue(mq.QUEUE_DEFAULT))
	assert.Equal(t, false, mq.IsWorkQueue(mq.QUEUE_COMPLETED))
	assert.Equal(t, false, mq.IsWorkQueue(mq.QUEUE_ERROR))
	assert.Equal(t, false, mq.IsWorkQueue(mq.QUEUE_STARTED))
	assert.Equal(t, false, mq.IsWorkQueue(mq.QUEUE_PENDING))
}
