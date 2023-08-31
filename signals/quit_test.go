package signals_test

import (
	"syscall"
	"testing"
	"time"

	"github.com/runabol/tork/signals"
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
	syscall.Kill(syscall.Getpid(), syscall.SIGTERM)
	// wait for termination to be caught
	time.Sleep(time.Millisecond * 100)
	assert.True(t, terminated)
}
