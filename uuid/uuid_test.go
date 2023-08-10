package uuid_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tork/uuid"
)

func TestNewUUID(t *testing.T) {
	assert.Equal(t, 32, len(uuid.NewUUID()))
}

func TestNewUUIDAsBase64(t *testing.T) {
	assert.Less(t, len(uuid.NewUUIDBase64()), 32)
}
