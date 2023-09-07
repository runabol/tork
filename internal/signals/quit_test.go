package signals_test

import (
	"syscall"
	"testing"
	"time"

	"github.com/runabol/tork/internal/signals"
	"github.com/stretchr/testify/assert"
)

func TestAwaitTerm(t *testing.T) {
	var terminated bool
	go func() {
		signals.AwaitTerm()
		terminated = true
	}()
	// wait for the hook
	time.Sleep(time.Millisecond * 100)
	err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM)
	assert.NoError(t, err)
	// wait for termination to be caught
	time.Sleep(time.Millisecond * 100)
	assert.True(t, terminated)
}
