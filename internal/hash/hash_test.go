package hash_test

import (
	"testing"

	"github.com/runabol/tork/internal/hash"
	"github.com/stretchr/testify/assert"
)

func TestHashPassword(t *testing.T) {
	hashed, err := hash.Password("1234")
	assert.NoError(t, err)
	match := hash.CheckPasswordHash("1234", hashed)
	assert.True(t, match)
}
