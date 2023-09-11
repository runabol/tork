package tork

type Metrics struct {
	Jobs  JobMetrics  `json:"jobs"`
	Tasks TaskMetrics `json:"tasks"`
	Nodes NodeMetrics `json:"nodes"`
}

type JobMetrics struct {
	Running int `json:"running"`
}

type TaskMetrics struct {
	Running int `json:"running"`
}

type NodeMetrics struct {
	Running    int     `json:"online"`
	CPUPercent float64 `json:"cpuPercent"`
}
