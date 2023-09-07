package uuid_test

import (
	"testing"

	"github.com/runabol/tork/internal/uuid"
	"github.com/stretchr/testify/assert"
)

func TestNewUUID(t *testing.T) {
	assert.Equal(t, 32, len(uuid.NewUUID()))
}
