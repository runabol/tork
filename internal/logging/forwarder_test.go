package logging

import (
	"testing"

	"github.com/runabol/tork"
	"github.com/runabol/tork/mq"
	"github.com/stretchr/testify/assert"
)

func TestForward(t *testing.T) {
	b := mq.NewInMemoryBroker()

	processed := make(chan any)
	err := b.SubscribeForTaskLogPart(func(p *tork.TaskLogPart) {
		close(processed)
	})
	assert.NoError(t, err)

	fwd := &Forwarder{
		Broker: b,
		TaskID: "some-task-id",
	}

	_, err = fwd.Write([]byte("log line"))
	assert.NoError(t, err)

	<-processed
}
