package fts

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrepareQuery(t *testing.T) {
	assert.Equal(t, "", PrepareQuery(""))
	assert.Equal(t, "Hello:*", PrepareQuery("Hello"))
	assert.Equal(t, "Hello:* & World:*", PrepareQuery("Hello World"))
	assert.Equal(t, "Hello:* & World:*", PrepareQuery("Hello ! World"))
	assert.Equal(t, "Hello:* & World:*", PrepareQuery("Hello ! World    "))
}
