package worker

import (
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

// requireCordonToken enforces the bearer token on the cordon endpoints.
func (s *api) requireCordonToken(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		auth := c.Request().Header.Get(echo.HeaderAuthorization)
		const prefix = "Bearer "
		if !strings.HasPrefix(auth, prefix) {
			return echo.NewHTTPError(http.StatusUnauthorized, "missing bearer token")
		}
		provided := strings.TrimPrefix(auth, prefix)
		if subtle.ConstantTimeCompare([]byte(provided), []byte(s.cordonToken)) != 1 {
			return echo.NewHTTPError(http.StatusUnauthorized, "invalid cordon token")
		}
		return next(c)
	}
}

type workerStatus struct {
	ID        string `json:"id"`
	Cordoned  bool   `json:"cordoned"`
	TaskCount int    `json:"taskCount"`
}

func (s *api) workerStatus() workerStatus {
	return workerStatus{
		ID:        s.worker.id,
		Cordoned:  s.worker.isCordoned(),
		TaskCount: s.worker.TaskCount(),
	}
}

// status reports the worker's cordon state and in-flight task count -- enough
// to drive a cordon/drain/stop loop from the host.
func (s *api) status(c echo.Context) error {
	return c.JSON(http.StatusOK, s.workerStatus())
}

func (s *api) cordon(c echo.Context) error {
	if err := s.worker.Cordon(); err != nil {
		log.Error().Err(err).Msg("error cordoning worker")
		return echo.NewHTTPError(http.StatusInternalServerError, "error cordoning worker")
	}
	return c.JSON(http.StatusOK, s.workerStatus())
}

func (s *api) uncordon(c echo.Context) error {
	if err := s.worker.Uncordon(); err != nil {
		log.Error().Err(err).Msg("error uncordoning worker")
		return echo.NewHTTPError(http.StatusInternalServerError, "error uncordoning worker")
	}
	return c.JSON(http.StatusOK, s.workerStatus())
}
