package wildcard

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type wildPatternTestCase struct {
	p string
	m bool
}

type matchTestCase struct {
	p string
	s string
	m bool
}

func TestIsWildPattern(t *testing.T) {
	testCases1 := []wildPatternTestCase{
		{"*", true},
		{"**", true},
		{".", false},
		{"a", false},
	}

	for _, tc := range testCases1 {
		b := isWildPattern(tc.p)
		if !assert.Equal(t, b, tc.m) {
			println(tc.p, tc.m)
		}
	}

}

func TestMatch(t *testing.T) {

	testCases1 := []matchTestCase{
		{"", "", true},
		{"*", "", true},
		{"", "a", false},
		{"abc", "abc", true},
		{"abc", "ac", false},
		{"abc", "abd", false},
		{"a*c", "abc", true},
		{"a*c", "abcbc", true},
		{"a*c", "abcbd", false},
		{"a*b*c", "ajkembbcldkcedc", true},
	}

	for _, tc := range testCases1 {
		m := Match(tc.p, tc.s)
		if !assert.Equal(t, m, tc.m) {
			println(tc.p, tc.s, tc.m)
		}

	}

}

func TestMatch2(t *testing.T) {
	testCases1 := []matchTestCase{
		{"jobs.*", "jobs.completed", true},
		{"jobs.*", "jobs.long.completed", true},
		{"tasks.*", "jobs.completed", false},
		{"*.completed", "jobs.completed", true},
		{"*.completed.thing", "jobs.completed", false},
	}
	for _, tc := range testCases1 {
		m := Match(tc.p, tc.s)
		assert.Equal(t, m, tc.m)
	}
}
