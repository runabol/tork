package slices

func Intersect[T comparable](a []T, b []T) bool {
	elements := make(map[T]struct{})

	for _, item := range a {
		elements[item] = struct{}{}
	}

	for _, item := range b {
		if _, found := elements[item]; found {
			return true
		}
	}

	return false
}
