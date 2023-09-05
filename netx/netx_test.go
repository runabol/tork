package netx_test

import (
	"net"
	"testing"

	"github.com/runabol/tork/netx"
	"github.com/stretchr/testify/assert"
)

func TestCanConnect(t *testing.T) {
	ln, err := net.Listen("tcp", "localhost:9999")
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, ln.Close())
	}()
	assert.True(t, netx.CanConnect("localhost:9999"))
	assert.False(t, netx.CanConnect("localhost:8888"))
}
