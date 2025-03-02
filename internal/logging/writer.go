package logging

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// ZerologWriter is a writer that adapts the io.Writer interface to the zerolog.Logger.
type ZerologWriter struct {
	taskID string
	level  zerolog.Level
}

func NewZerologWriter(taskID string, level zerolog.Level) *ZerologWriter {
	return &ZerologWriter{
		taskID: taskID,
		level:  level,
	}
}

func (zw *ZerologWriter) Write(p []byte) (n int, err error) {
	logLine := string(p[:])
	log.WithLevel(zw.level).Str("task-id", zw.taskID).Msg(logLine)
	return len(p), nil
}
