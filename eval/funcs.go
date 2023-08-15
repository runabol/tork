package eval

import "math/rand"

// randomInt returns a non-negative pseudo-random int from the default Source.
func randomInt() int {
	return rand.Int()
}
