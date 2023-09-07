package tork

type Stats struct {
	Jobs  JobStats   `json:"jobs"`
	Tasks TasksStats `json:"tasks"`
	Nodes NodeStats  `json:"nodes"`
}

type JobStats struct {
	Running int `json:"running"`
}

type TasksStats struct {
	Running int `json:"running"`
}

type NodeStats struct {
	Running    int     `json:"online"`
	CPUPercent float64 `json:"cpuPercent"`
}
