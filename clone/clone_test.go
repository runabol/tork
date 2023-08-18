package clone_test

import (
	"testing"

	"github.com/runabol/tork/clone"
	"github.com/stretchr/testify/assert"
)

func TestCloneStringMap(t *testing.T) {
	m := map[string]string{
		"ke1": "val1",
	}
	c := clone.CloneStringMap(m)
	assert.Equal(t, m, c)
	m["key2"] = "val2"
	assert.NotEqual(t, m, c)
}
