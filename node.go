package tork

import (
	"time"
)

var LAST_HEARTBEAT_TIMEOUT = time.Minute * 5
var HEARTBEAT_RATE = time.Second * 30

type NodeStatus string

const (
	NodeStatusUP      NodeStatus = "UP"
	NodeStatusDown    NodeStatus = "DOWN"
	NodeStatusOffline NodeStatus = "OFFLINE"
)

type Node struct {
	ID              string     `json:"id,omitempty"`
	StartedAt       time.Time  `json:"startedAt,omitempty"`
	CPUPercent      float64    `json:"cpuPercent,omitempty"`
	LastHeartbeatAt time.Time  `json:"lastHeartbeatAt,omitempty"`
	Queue           string     `json:"queue,omitempty"`
	Status          NodeStatus `json:"status,omitempty"`
	Hostname        string     `json:"hostname,omitempty"`
	TaskCount       int        `json:"taskCount"`
	Version         string     `json:"version"`
}
