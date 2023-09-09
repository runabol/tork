package datastore

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestCustomProvider(t *testing.T) {
	RegisterProvider("inmem2", func() (Datastore, error) {
		return NewInMemoryDatastore(), nil
	})
	p, err := NewFromProvider("inmem2")
	assert.NoError(t, err)
	assert.NotNil(t, p)
}

func TestUnknownProvider(t *testing.T) {
	p, err := NewFromProvider("not_a_thing")
	assert.ErrorIs(t, err, ErrProviderNotFound)
	assert.Nil(t, p)
}

func TestBadProvider(t *testing.T) {
	Err := errors.Errorf("some bad thing happened")
	RegisterProvider("inmem3", func() (Datastore, error) {
		return nil, Err
	})
	p, err := NewFromProvider("inmem3")
	assert.ErrorIs(t, err, Err)
	assert.Nil(t, p)
}
