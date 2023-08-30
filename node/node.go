package node

import "time"

var LAST_HEARTBEAT_TIMEOUT = time.Minute * 5
var HEARTBEAT_RATE = time.Second * 30

type Status string

const (
	UP      Status = "UP"
	Down    Status = "DOWN"
	Offline Status = "OFFLINE"
)

type Node struct {
	ID              string    `json:"id,omitempty"`
	StartedAt       time.Time `json:"startedAt,omitempty"`
	CPUPercent      float64   `json:"cpuPercent,omitempty"`
	LastHeartbeatAt time.Time `json:"lastHeartbeatAt,omitempty"`
	Queue           string    `json:"queue,omitempty"`
	Status          Status    `json:"status,omitempty"`
	Hostname        string    `json:"hostname,omitempty"`
	TaskCount       int       `json:"taskCount"`
}
