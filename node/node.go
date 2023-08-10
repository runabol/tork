package node

import "time"

var LAST_HEARTBEAT_TIMEOUT = time.Minute * 5

type Node struct {
	ID              string    `json:"id,omitempty"`
	StartedAt       time.Time `json:"startedAt,omitempty"`
	CPUPercent      float64   `json:"cpuPercent,omitempty"`
	LastHeartbeatAt time.Time `json:"lastHeartbeatAt,omitempty"`
	Queue           string    `json:"queue,omitempty"`
}
