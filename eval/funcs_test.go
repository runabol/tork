package eval

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRange(t *testing.T) {
	result := range_(1, 3)
	assert.Equal(t, []int{1, 2}, result)

	result = range_(-5, -2)
	assert.Equal(t, []int{-5, -4, -3}, result)

	result = range_(5, 2)
	assert.Equal(t, []int{}, result)

	result = range_(2, 2)
	assert.Equal(t, []int{}, result)
}
