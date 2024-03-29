package eval

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSequence(t *testing.T) {
	result := sequence(1, 3)
	assert.Equal(t, []int{1, 2}, result)

	result = sequence(-5, -2)
	assert.Equal(t, []int{-5, -4, -3}, result)

	result = sequence(5, 2)
	assert.Equal(t, []int{}, result)

	result = sequence(2, 2)
	assert.Equal(t, []int{}, result)
}

func TestRandomInt(t *testing.T) {
	for i := 0; i < 100; i++ {
		result, err := randomInt(5)
		assert.Less(t, result, 5)
		assert.NoError(t, err)

		result, err = randomInt(int64(5))
		assert.Less(t, result, 5)
		assert.NoError(t, err)

		result, err = randomInt(int32(5))
		assert.Less(t, result, 5)
		assert.NoError(t, err)

		_, err = randomInt(nil)
		assert.Error(t, err)

		_, err = randomInt("100")
		assert.Error(t, err)

		_, err = randomInt(1, 10)
		assert.Error(t, err)
	}
	ls := map[int]int{}
	for i := 0; i < 100; i++ {
		result, err := randomInt()
		ls[result] = result
		assert.NoError(t, err)
	}
	assert.Len(t, ls, 100)
	for i := 0; i < 100; i++ {
		result, err := randomInt(int32(5))
		assert.Less(t, result, 5)
		assert.NoError(t, err)
	}
}
