package broker

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
)

type LogShipper struct {
	Broker Broker
	TaskID string
	part   int
	q      chan []byte
}

func NewLogShipper(broker Broker, taskID string) *LogShipper {
	f := &LogShipper{
		Broker: broker,
		TaskID: taskID,
		q:      make(chan []byte, 1000),
	}
	go f.startFlushTimer()
	return f
}

func (r *LogShipper) Write(p []byte) (int, error) {
	pc := make([]byte, len(p))
	copy(pc, p)
	r.q <- pc
	return len(p), nil
}

func (r *LogShipper) startFlushTimer() {
	ticker := time.NewTicker(time.Second)
	buffer := make([]byte, 0)
	for {
		select {
		case p := <-r.q:
			buffer = append(buffer, p...)
		case <-ticker.C:
			if len(buffer) > 0 {
				r.part = r.part + 1
				if err := r.Broker.PublishTaskLogPart(context.Background(), &tork.TaskLogPart{
					Number:   r.part,
					TaskID:   r.TaskID,
					Contents: string(buffer),
				}); err != nil {
					log.Error().Err(err).Msgf("error forwarding task log part")
				}
				buffer = buffer[:0] // clear buffer
			}
		}
	}
}
