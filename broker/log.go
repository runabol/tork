package broker

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
)

// maxLogPartSize keeps each log part under PostgreSQL's tsvector input limit (~1 MiB).
const maxLogPartSize = 512 * 1024

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
			if len(buffer) >= maxLogPartSize {
				r.flush(&buffer)
			}
		case <-ticker.C:
			r.flush(&buffer)
		}
	}
}

func (r *LogShipper) flush(buffer *[]byte) {
	for len(*buffer) > 0 {
		n := len(*buffer)
		if n > maxLogPartSize {
			n = maxLogPartSize
		}
		r.part++
		if err := r.Broker.PublishTaskLogPart(context.Background(), &tork.TaskLogPart{
			Number:   r.part,
			TaskID:   r.TaskID,
			Contents: string((*buffer)[:n]),
		}); err != nil {
			log.Error().Err(err).Msgf("error forwarding task log part")
		}
		*buffer = (*buffer)[n:]
	}
}
