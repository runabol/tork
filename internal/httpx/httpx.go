package httpx

import (
	"net/http"
	"time"

	"github.com/pkg/errors"
	"github.com/runabol/tork/internal/netx"
)

func StartAsync(s *http.Server) error {
	errChan := make(chan error)
	go func() {
		if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
	}()
	for i := 0; i < 100; i++ {
		select {
		case err := <-errChan:
			return err
		case <-time.After(time.Millisecond * 100):
		}
		if netx.CanConnect(s.Addr) {
			return nil
		}
	}
	return errors.Errorf("unable to start API server")
}
