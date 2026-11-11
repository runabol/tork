package docker

import (
	"bytes"
	"io"
	"net"
	"net/http"
	"testing"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
)

func dockerStdoutFrame(msg string) []byte {
	frame := make([]byte, 8+len(msg))
	frame[0] = 1
	frame[4] = byte(len(msg) >> 24)
	frame[5] = byte(len(msg) >> 16)
	frame[6] = byte(len(msg) >> 8)
	frame[7] = byte(len(msg))
	copy(frame[8:], msg)
	return frame
}

type readErrCloser struct {
	r   io.Reader
	err error
}

func (c readErrCloser) Read(p []byte) (int, error) {
	if c.r == nil {
		return 0, c.err
	}
	n, err := c.r.Read(p)
	if err == io.EOF && c.err != nil {
		return n, c.err
	}
	return n, err
}

func (c readErrCloser) Close() error {
	return nil
}

func TestStreamLogs_copiesMultiplexedOutput(t *testing.T) {
	var out bytes.Buffer
	in := io.NopCloser(bytes.NewReader(dockerStdoutFrame("hello")))
	finish := streamLogs(in, &out)
	finish()
	assert.Equal(t, "hello", out.String())
}

func TestStreamLogs_waitsForCopyBeforeReturn(t *testing.T) {
	var out bytes.Buffer
	pr, pw := io.Pipe()
	finish := streamLogs(pr, &out)
	_, err := pw.Write(dockerStdoutFrame("hello"))
	assert.NoError(t, err)
	assert.NoError(t, pw.Close())
	finish()
	assert.Equal(t, "hello", out.String())
}

func TestStreamLogs_logOnUnexpectedReadError(t *testing.T) {
	var logBuf bytes.Buffer
	prev := log.Logger
	t.Cleanup(func() { log.Logger = prev })
	log.Logger = zerolog.New(&logBuf).Level(zerolog.ErrorLevel)

	finish := streamLogs(readErrCloser{err: errors.New("connection reset by peer")}, io.Discard)
	finish()
	assert.Contains(t, logBuf.String(), "error reading the std out")
}

func TestStreamLogs_noLogOnBenignReadError(t *testing.T) {
	benign := []error{
		net.ErrClosed,
		http.ErrBodyReadAfterClose,
		errors.New("http: read on closed response body"),
		&net.OpError{Op: "read", Net: "unix", Err: net.ErrClosed}, // prod: read unix @->/run/docker.sock: ...
	}
	for _, benignErr := range benign {
		t.Run(benignErr.Error(), func(t *testing.T) {
			var logBuf bytes.Buffer
			prev := log.Logger
			t.Cleanup(func() { log.Logger = prev })
			log.Logger = zerolog.New(&logBuf).Level(zerolog.ErrorLevel)

			finish := streamLogs(readErrCloser{err: benignErr}, io.Discard)
			finish()
			assert.NotContains(t, logBuf.String(), "error reading the std out")
		})
	}
}
