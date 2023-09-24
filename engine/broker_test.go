package engine

import (
	"testing"

	"github.com/runabol/tork/mq"
	"github.com/stretchr/testify/assert"
)

func Test_createBroker(t *testing.T) {
	eng := New(Config{Mode: ModeStandalone})
	assert.Equal(t, StateIdle, eng.state)
	b, err := eng.createBroker(mq.BROKER_INMEMORY)
	assert.NoError(t, err)
	assert.IsType(t, &mq.InMemoryBroker{}, b)
}

func Test_createBrokerProvider(t *testing.T) {
	eng := New(Config{Mode: ModeStandalone})
	assert.Equal(t, StateIdle, eng.state)
	eng.RegisterBrokerProvider("inmem2", func() (mq.Broker, error) {
		return mq.NewInMemoryBroker(), nil
	})
	br, err := eng.createBroker("inmem2")
	assert.NoError(t, err)
	assert.IsType(t, &mq.InMemoryBroker{}, br)
}
