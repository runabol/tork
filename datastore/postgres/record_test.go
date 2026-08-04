package postgres

import (
	"testing"
	"time"

	"github.com/runabol/tork"
	"github.com/stretchr/testify/assert"
)

// TestNodeRecordToNodeStatus covers the status-derivation logic in
// nodeRecord.toNode(), including the CORDONED handling: a cordoned node whose
// heartbeats have gone stale must still be derived as OFFLINE (rather than
// staying stuck at CORDONED), while a freshly-heartbeating cordoned node keeps
// its CORDONED status.
func TestNodeRecordToNodeStatus(t *testing.T) {
	fresh := time.Now().UTC()
	stale := time.Now().UTC().Add(-tork.HEARTBEAT_RATE * 3)

	cases := []struct {
		name    string
		status  tork.NodeStatus
		hb      time.Time
		expects tork.NodeStatus
	}{
		{"fresh up stays up", tork.NodeStatusUP, fresh, tork.NodeStatusUP},
		{"stale up becomes offline", tork.NodeStatusUP, stale, tork.NodeStatusOffline},
		{"fresh cordoned stays cordoned", tork.NodeStatusCordoned, fresh, tork.NodeStatusCordoned},
		{"stale cordoned becomes offline", tork.NodeStatusCordoned, stale, tork.NodeStatusOffline},
		{"stale down stays down", tork.NodeStatusDown, stale, tork.NodeStatusDown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := nodeRecord{
				Status:          string(tc.status),
				LastHeartbeatAt: tc.hb,
			}
			assert.Equal(t, tc.expects, r.toNode().Status)
		})
	}
}
