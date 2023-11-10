package uuid_test

import (
	"testing"

	"github.com/runabol/tork/internal/uuid"
	"github.com/stretchr/testify/assert"
)

func TestNewUUID(t *testing.T) {
	assert.Equal(t, 32, len(uuid.NewUUID()))
}

func TestNewShortUUID(t *testing.T) {
	ids := map[string]string{}
	for i := 0; i < 100; i++ {
		uid := uuid.NewShortUUID()
		assert.Len(t, uid, 22)
		ids[uid] = uid
	}
	assert.Len(t, ids, 100)
}
