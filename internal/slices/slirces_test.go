package slices

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHasIntersection(t *testing.T) {
	tests := []struct {
		slice1 []int
		slice2 []int
		want   bool
	}{
		{slice1: []int{1, 2, 3, 4, 5}, slice2: []int{4, 5, 6, 7, 8}, want: true},
		{slice1: []int{1, 2, 3, 4, 5}, slice2: []int{6, 7, 8, 9, 10}, want: false},
		{slice1: []int{}, slice2: []int{1, 2, 3}, want: false},
		{slice1: []int{1, 2, 3}, slice2: []int{}, want: false},
		{slice1: []int{}, slice2: []int{}, want: false},
		{slice1: []int{1, 2, 3}, slice2: []int{3, 4, 5}, want: true},
	}

	for _, tt := range tests {
		got := Intersect(tt.slice1, tt.slice2)
		assert.Equal(t, tt.want, got)
	}
}
