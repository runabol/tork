package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"syscall"

	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"

	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/input"
	"github.com/runabol/tork/internal/httpx"

	"github.com/runabol/tork/middleware"

	"github.com/runabol/tork"
	"github.com/runabol/tork/mq"
	"gopkg.in/yaml.v3"
)

const (
	MIN_PORT = 8000
	MAX_PORT = 8100
)

type api struct {
	server *http.Server
	broker mq.Broker
	ds     datastore.Datastore
}

type apiContext struct {
	ctx  echo.Context
	api  *api
	err  error
	code int
}

func (c *apiContext) Request() *http.Request {
	return c.ctx.Request()
}

func (c *apiContext) String(code int, s string) error {
	return c.ctx.String(code, s)
}

func (c *apiContext) JSON(code int, data any) error {
	return c.ctx.JSON(code, data)
}

func (c *apiContext) SubmitJob(j *input.Job) (*tork.Job, error) {
	job, err := c.api.submitJob(c.ctx.Request().Context(), j)
	if err != nil {
		return nil, err
	}
	return job.Clone(), nil
}

func (c *apiContext) Error(code int, err error) {
	c.err = err
	c.code = code
}

func newAPI(cfg Config) (*api, error) {
	r := echo.New()

	s := &api{
		broker: cfg.Broker,
		server: &http.Server{
			Addr:    cfg.Address,
			Handler: r,
		},
		ds: cfg.DataStore,
	}

	// registering custom middlewares
	for _, m := range cfg.Middlewares {
		r.Use(s.middlewareAdapter(m))
	}

	// built-in endpoints
	r.GET("/health", s.health)
	r.GET("/tasks/:id", s.getTask)
	r.GET("/queues", s.listQueues)
	r.GET("/nodes", s.listActiveNodes)
	r.POST("/jobs", s.createJob)
	r.GET("/jobs/:id", s.getJob)
	r.GET("/jobs", s.listJobs)
	r.PUT("/jobs/:id/cancel", s.cancelJob)
	r.PUT("/jobs/:id/restart", s.restartJob)
	r.GET("/stats", s.getStats)

	// register additional custom endpoints
	for spec, h := range cfg.Endpoints {
		parsed := strings.Split(spec, " ")
		if len(parsed) != 2 {
			return nil, errors.Errorf("invalid endpoint spec: %s. Expecting: METHOD /path", spec)
		}
		r.Add(parsed[0], parsed[1], func(c echo.Context) error {
			ctx := &apiContext{ctx: c, api: s}
			err := h(ctx)
			if err != nil {
				return err
			}
			if ctx.err != nil {
				return echo.NewHTTPError(ctx.code, ctx.err.Error())
			}
			return nil
		})
	}

	return s, nil
}

func (s *api) middlewareAdapter(m middleware.MiddlewareFunc) echo.MiddlewareFunc {
	nextAdapter := func(next echo.HandlerFunc, ec echo.Context) middleware.HandlerFunc {
		return func(c middleware.Context) error {
			return next(ec)
		}
	}
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(ec echo.Context) error {
			ctx := &apiContext{
				ctx: ec,
				api: s,
			}
			err := m(nextAdapter(next, ec))(ctx)
			if err != nil {
				return err
			}
			if ctx.err != nil {
				return echo.NewHTTPError(ctx.code, ctx.err.Error())
			}
			return nil
		}
	}
}

func (s *api) health(c echo.Context) error {
	return c.JSON(http.StatusOK, map[string]string{
		"status":  "UP",
		"version": fmt.Sprintf("%s (%s)", tork.Version, tork.GitCommit),
	})
}

func (s *api) listQueues(c echo.Context) error {
	qs, err := s.broker.Queues(c.Request().Context())
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	return c.JSON(http.StatusOK, qs)
}

func (s *api) listActiveNodes(c echo.Context) error {
	nodes, err := s.ds.GetActiveNodes(c.Request().Context())
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	return c.JSON(http.StatusOK, nodes)
}

func (s *api) createJob(c echo.Context) error {
	var ji *input.Job
	var err error
	contentType := c.Request().Header.Get("content-type")
	switch contentType {
	case "application/json":
		ji, err = bindJobInputJSON(c.Request().Body)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}
	case "text/yaml":
		ji, err = bindJobInputYAML(c.Request().Body)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}
	default:
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("unknown content type: %s", contentType))
	}
	if j, err := s.submitJob(c.Request().Context(), ji); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	} else {
		return c.JSON(http.StatusOK, redactJob(j))
	}
}

func (s *api) submitJob(ctx context.Context, ji *input.Job) (*tork.Job, error) {
	if err := ji.Validate(); err != nil {
		return nil, err
	}
	j := ji.ToJob()
	if err := s.ds.CreateJob(ctx, j); err != nil {
		return nil, err
	}
	log.Info().Str("job-id", j.ID).Msg("created job")
	if err := s.broker.PublishJob(ctx, j); err != nil {
		return nil, err
	}
	return j, nil
}

func bindJobInputJSON(r io.ReadCloser) (*input.Job, error) {
	ji := input.Job{}
	body, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&ji); err != nil {
		return nil, err
	}
	return &ji, nil
}

func bindJobInputYAML(r io.ReadCloser) (*input.Job, error) {
	ji := input.Job{}
	body, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	dec := yaml.NewDecoder(strings.NewReader(string(body)))
	dec.KnownFields(true)
	if err := dec.Decode(&ji); err != nil {
		return nil, err
	}
	return &ji, nil
}

func (s *api) getJob(c echo.Context) error {
	id := c.Param("id")
	j, err := s.ds.GetJobByID(c.Request().Context(), id)
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, err.Error())
	}
	return c.JSON(http.StatusOK, redactJob(j))
}

func (s *api) listJobs(c echo.Context) error {
	ps := c.QueryParam("page")
	if ps == "" {
		ps = "1"
	}
	page, err := strconv.Atoi(ps)
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, fmt.Sprintf("invalid page number: %s", ps))
	}
	if page < 1 {
		page = 1
	}
	si := c.QueryParam("size")
	if si == "" {
		si = "10"
	}
	size, err := strconv.Atoi(si)
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, fmt.Sprintf("invalid size: %s", ps))
	}
	if size < 1 {
		size = 1
	} else if size > 20 {
		size = 20
	}
	q := c.QueryParam("q")
	res, err := s.ds.GetJobs(c.Request().Context(), q, page, size)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, datastore.Page[*tork.Job]{
		Number:     res.Number,
		Size:       res.Size,
		TotalPages: res.TotalPages,
		Items:      redactJobs(res.Items),
		TotalItems: res.TotalItems,
	})
}

func (s *api) getTask(c echo.Context) error {
	id := c.Param("id")
	t, err := s.ds.GetTaskByID(c.Request().Context(), id)
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, err.Error())
	}
	return c.JSON(http.StatusOK, redactTask(t))
}

func (s *api) getStats(c echo.Context) error {
	stats, err := s.ds.GetStats(c.Request().Context())
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, stats)
}

func (s *api) restartJob(c echo.Context) error {
	id := c.Param("id")
	j, err := s.ds.GetJobByID(c.Request().Context(), id)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	if j.State != tork.JobStateFailed && j.State != tork.JobStateCancelled {
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("job is %s and can not be restarted", j.State))
	}
	if j.Position > len(j.Tasks) {
		return echo.NewHTTPError(http.StatusBadRequest, "job has no more tasks to run")
	}
	j.State = tork.JobStateRestart
	if err := s.broker.PublishJob(c.Request().Context(), j); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, map[string]string{"status": "OK"})
}

func (s *api) cancelJob(c echo.Context) error {
	id := c.Param("id")
	j, err := s.ds.GetJobByID(c.Request().Context(), id)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	if j.State != tork.JobStateRunning {
		return echo.NewHTTPError(http.StatusBadRequest, "job is not running")
	}
	j.State = tork.JobStateCancelled
	if err := s.broker.PublishJob(c.Request().Context(), j); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, map[string]string{"status": "OK"})
}

func (s *api) start() error {
	if s.server.Addr != "" {
		if err := httpx.StartAsync(s.server); err != nil {
			return err
		}
	} else {
		// attempting to dynamically assign port
		for port := MIN_PORT; port <= MAX_PORT; port++ {
			s.server.Addr = fmt.Sprintf("localhost:%d", port)
			if err := httpx.StartAsync(s.server); err != nil {
				if errors.Is(err, syscall.EADDRINUSE) {
					continue
				}
				log.Fatal().Err(err).Msgf("error starting up server")
			}
			break
		}
	}
	log.Info().Msgf("Coordinator listening on http://%s", s.server.Addr)
	return nil
}

func (s *api) shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}
