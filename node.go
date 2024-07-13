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
	Name            string     `json:"name,omitempty"`
	StartedAt       time.Time  `json:"startedAt,omitempty"`
	CPUPercent      float64    `json:"cpuPercent,omitempty"`
	LastHeartbeatAt time.Time  `json:"lastHeartbeatAt,omitempty"`
	Queue           string     `json:"queue,omitempty"`
	Status          NodeStatus `json:"status,omitempty"`
	Hostname        string     `json:"hostname,omitempty"`
	Port            int        `json:"port,omitempty"`
	TaskCount       int        `json:"taskCount,omitempty"`
	Version         string     `json:"version"`
}

func (n *Node) Clone() *Node {
	return &Node{
		ID:              n.ID,
		Name:            n.Name,
		StartedAt:       n.StartedAt,
		CPUPercent:      n.CPUPercent,
		LastHeartbeatAt: n.LastHeartbeatAt,
		Queue:           n.Queue,
		Status:          n.Status,
		Hostname:        n.Hostname,
		Port:            n.Port,
		TaskCount:       n.TaskCount,
		Version:         n.Version,
	}
}
