package logging

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
	"github.com/runabol/tork/mq"
)

type Forwarder struct {
	Broker mq.Broker
	TaskID string
	part   int
	q      chan []byte
}

func NewForwarder(broker mq.Broker, taskID string) *Forwarder {
	f := &Forwarder{
		Broker: broker,
		TaskID: taskID,
		q:      make(chan []byte, 1000),
	}
	go f.startFlushTimer()
	return f
}

func (r *Forwarder) Write(p []byte) (int, error) {
	pc := make([]byte, len(p))
	copy(pc, p)
	select {
	case r.q <- pc:
		return len(p), nil
	default:
		return 0, fmt.Errorf("buffer full, unable to write")
	}
}

func (r *Forwarder) startFlushTimer() {
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
