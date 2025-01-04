package engine

import (
	"testing"

	"github.com/runabol/tork/broker"
	"github.com/stretchr/testify/assert"
)

func Test_createBroker(t *testing.T) {
	eng := New(Config{Mode: ModeStandalone})
	assert.Equal(t, StateIdle, eng.state)
	b, err := eng.createBroker(broker.BROKER_INMEMORY)
	assert.NoError(t, err)
	assert.IsType(t, &broker.InMemoryBroker{}, b)
}

func Test_createBrokerProvider(t *testing.T) {
	eng := New(Config{Mode: ModeStandalone})
	assert.Equal(t, StateIdle, eng.state)
	eng.RegisterBrokerProvider("inmem2", func() (broker.Broker, error) {
		return broker.NewInMemoryBroker(), nil
	})
	br, err := eng.createBroker("inmem2")
	assert.NoError(t, err)
	assert.IsType(t, &broker.InMemoryBroker{}, br)
}
