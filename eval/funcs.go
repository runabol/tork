package eval

import "math/rand"

// randomInt returns a non-negative pseudo-random int from the default Source.
func randomInt() int {
	return rand.Int()
}

func coinflip() bool {
	return rand.Int()%2 == 0
}

// range_  returns a sequence of numbers, starting from start,
// and increments by 1 and stops before a specified number.
func range_(start, stop int) []int {
	if start > stop {
		return []int{}
	}
	result := make([]int, stop-start)
	for ix := range result {
		result[ix] = start
		start = start + 1
	}
	return result
}
