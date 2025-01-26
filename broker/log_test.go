package broker

import (
	"fmt"
	"testing"
	"time"

	"github.com/runabol/tork"
	"github.com/stretchr/testify/assert"
)

func TestForwardTimeout(t *testing.T) {
	b := NewInMemoryBroker()

	processed := make(chan any)
	err := b.SubscribeForTaskLogPart(func(p *tork.TaskLogPart) {
		assert.Equal(t, "hello\n", p.Contents)
		processed <- 1
	})
	assert.NoError(t, err)

	fwd := NewLogShipper(b, "some-task-id")
	for i := 0; i < 1; i++ {
		_, err = fwd.Write([]byte("hello\n"))
		assert.NoError(t, err)
		<-time.After(time.Millisecond * 1100)
	}

	<-processed
}

func TestForwardBatch(t *testing.T) {
	b := NewInMemoryBroker()

	processed := make(chan any)
	err := b.SubscribeForTaskLogPart(func(p *tork.TaskLogPart) {
		assert.Equal(t, "hello 0\nhello 1\nhello 2\nhello 3\nhello 4\n", p.Contents)
		close(processed)
	})
	assert.NoError(t, err)

	fwd := NewLogShipper(b, "some-task-id")

	for i := 0; i < 5; i++ {
		_, err = fwd.Write([]byte(fmt.Sprintf("hello %d\n", i)))
		assert.NoError(t, err)
	}

	<-processed
}

func TestLogShipperWriteBufferFull(t *testing.T) {
	b := NewInMemoryBroker()
	err := b.SubscribeForTaskLogPart(func(p *tork.TaskLogPart) {

	})
	assert.NoError(t, err)
	fwd := NewLogShipper(b, "some-task-id")
	for i := 0; i < 10_000; i++ {
		_, err := fwd.Write([]byte("some log message\n"))
		assert.NoError(t, err)
	}
}
