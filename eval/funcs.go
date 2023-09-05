package eval

import (
	"math/rand"
	"reflect"

	"github.com/pkg/errors"
)

func randomInt(args ...any) (int, error) {
	if len(args) == 1 {
		if args[0] == nil {
			return 0, errors.Errorf("not expecting nil argument")
		}
		v := reflect.ValueOf(args[0])
		if !v.CanInt() {
			return 0, errors.Errorf("invalid arg type %s", v.Type())
		}
		return rand.Intn(int(v.Int())), nil
	} else if len(args) == 0 {
		return rand.Int(), nil
	} else {
		return 0, errors.Errorf("invalid number of arguments for trim (expected 0 or 1, got %d)", len(args))
	}
}

func sequence(start, stop int) []int {
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
