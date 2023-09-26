package api

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
	"github.com/runabol/tork/health"

	"github.com/runabol/tork/input"
	"github.com/runabol/tork/internal/httpx"
	"github.com/runabol/tork/middleware/job"
	"github.com/runabol/tork/middleware/task"
	"github.com/runabol/tork/middleware/web"

	"github.com/runabol/tork"
	"github.com/runabol/tork/mq"
	"gopkg.in/yaml.v3"
)

const (
	MIN_PORT = 8000
	MAX_PORT = 8100
)

type HealthResponse struct {
	Status string `json:"status"`
}

type API struct {
	server     *http.Server
	broker     mq.Broker
	ds         datastore.Datastore
	terminate  chan any
	onReadJob  job.HandlerFunc
	onReadTask task.HandlerFunc
}

type Config struct {
	Broker     mq.Broker
	DataStore  datastore.Datastore
	Address    string
	Middleware Middleware
	Endpoints  map[string]web.HandlerFunc
	Enabled    map[string]bool
}

type Middleware struct {
	Web  []web.MiddlewareFunc
	Job  []job.MiddlewareFunc
	Task []task.MiddlewareFunc
	Echo []echo.MiddlewareFunc
}

// @title Tork API
// @version 1.0

// @contact.name Arik Cohen
// @contact.url https://tork.run
// @contact.email contact@tork.run

// @license.name MIT
// @license.url https://github.com/runabol/tork/blob/main/LICENSE

// @host localhost:8000
// @BasePath /
// @schemes http
func NewAPI(cfg Config) (*API, error) {
	r := echo.New()

	s := &API{
		broker: cfg.Broker,
		server: &http.Server{
			Addr:    cfg.Address,
			Handler: r,
		},
		ds:        cfg.DataStore,
		terminate: make(chan any),
		onReadJob: job.ApplyMiddleware(
			job.NoOpHandlerFunc,
			cfg.Middleware.Job,
		),
		onReadTask: task.ApplyMiddleware(
			task.NoOpHandlerFunc,
			cfg.Middleware.Task,
		),
	}

	// registering custom middleware
	for _, m := range cfg.Middleware.Web {
		r.Use(s.middlewareAdapter(m))
	}

	// registering echo middleware
	for _, m := range cfg.Middleware.Echo {
		r.Use(m)
	}

	// built-in endpoints
	if v, ok := cfg.Enabled["health"]; !ok || v {
		r.GET("/health", s.health)
	}
	if v, ok := cfg.Enabled["tasks"]; !ok || v {
		r.GET("/tasks/:id", s.getTask)
	}
	if v, ok := cfg.Enabled["queues"]; !ok || v {
		r.GET("/queues", s.listQueues)
	}
	if v, ok := cfg.Enabled["nodes"]; !ok || v {
		r.GET("/nodes", s.listActiveNodes)
	}
	if v, ok := cfg.Enabled["jobs"]; !ok || v {
		r.POST("/jobs", s.createJob)
		r.GET("/jobs/:id", s.getJob)
		r.GET("/jobs", s.listJobs)
		r.PUT("/jobs/:id/cancel", s.cancelJob)
		r.PUT("/jobs/:id/restart", s.restartJob)
	}
	if v, ok := cfg.Enabled["metrics"]; !ok || v {
		r.GET("/metrics", s.getMetrics)
	}

	// register additional custom endpoints
	for spec, handler := range cfg.Endpoints {
		parsed := strings.Split(spec, " ")
		if len(parsed) != 2 {
			return nil, errors.Errorf("invalid endpoint spec: %s. Expecting: METHOD /path", spec)
		}
		method := parsed[0]
		path := parsed[1]
		r.Add(method, path, func(h web.HandlerFunc) func(c echo.Context) error {
			return func(c echo.Context) error {
				ctx := &Context{ctx: c, api: s}
				err := h(ctx)
				if err != nil {
					return err
				}
				if ctx.err != nil {
					return echo.NewHTTPError(ctx.code, ctx.err.Error())
				}
				return nil
			}
		}(handler))
	}

	return s, nil
}

func (s *API) middlewareAdapter(m web.MiddlewareFunc) echo.MiddlewareFunc {
	nextAdapter := func(next echo.HandlerFunc, ec echo.Context) web.HandlerFunc {
		return func(c web.Context) error {
			return next(ec)
		}
	}
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(ec echo.Context) error {
			ctx := &Context{
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

// health
// @Summary Shows application health information.
// @Description get the status of server.
// @Tags management
// @Produce json
// @Success 200 {object} HealthResponse
// @Router /health [get]
func (s *API) health(c echo.Context) error {
	result := health.NewHealthCheck().
		WithIndicator(health.ServiceDatastore, s.ds.HealthCheck).
		WithIndicator(health.ServiceBroker, s.broker.HealthCheck).
		Do(c.Request().Context())
	if result.Status == health.StatusDown {
		return c.JSON(http.StatusServiceUnavailable, result)
	} else {
		return c.JSON(http.StatusOK, result)
	}
}

// listQueues
// @Summary get a list of queues
// @Tags queues
// @Produce application/json
// @Success 200 {object} []mq.QueueInfo
// @Router /queues [get]
func (s *API) listQueues(c echo.Context) error {
	qs, err := s.broker.Queues(c.Request().Context())
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	return c.JSON(http.StatusOK, qs)
}

// listActiveNodes
// @Summary Get a list of active worker nodes
// @Tags nodes
// @Produce application/json
// @Success 200 {object} []tork.Node
// @Router /nodes [get]
func (s *API) listActiveNodes(c echo.Context) error {
	nodes, err := s.ds.GetActiveNodes(c.Request().Context())
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	return c.JSON(http.StatusOK, nodes)
}

// createJob
// @Summary Create a new job
// @Tags jobs
// @Accept json
// @Produce json
// @Success 200 {object} tork.JobSummary
// @Router /jobs [post]
// @Param request body input.Job true "body"
func (s *API) createJob(c echo.Context) error {
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
	if j, err := s.SubmitJob(c.Request().Context(), ji); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	} else {
		return c.JSON(http.StatusOK, tork.NewJobSummary(j))
	}
}

func (s *API) SubmitJob(ctx context.Context, ji *input.Job) (*tork.Job, error) {
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

// getJob
// @Summary Get a job by id
// @Tags jobs
// @Produce application/json
// @Success 200 {object} tork.Job
// @Failure 404 {object} echo.HTTPError
// @Router /jobs/{id} [get]
// @Param id path string true "Job ID"
func (s *API) getJob(c echo.Context) error {
	ctx := c.Request().Context()
	id := c.Param("id")
	j, err := s.ds.GetJobByID(ctx, id)
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, err)
	}
	if err := s.onReadJob(ctx, job.Read, j); err != nil {
		return err
	}
	return c.JSON(http.StatusOK, j)
}

// listJobs
// @Summary Show a list of jobs
// @Tags jobs
// @Produce application/json
// @Success 200 {object} []tork.JobSummary
// @Router /jobs [get]
// @Param q query string false "search string"
// @Param page query int false "page number"
// @Param size query int false "page size"
func (s *API) listJobs(c echo.Context) error {
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
	return c.JSON(http.StatusOK, datastore.Page[*tork.JobSummary]{
		Number:     res.Number,
		Size:       res.Size,
		TotalPages: res.TotalPages,
		Items:      res.Items,
		TotalItems: res.TotalItems,
	})
}

// getTask
// @Summary Get a task by id
// @Tags tasks
// @Produce application/json
// @Success 200 {object} tork.Task
// @Router /tasks/{id} [get]
// @Param id path string true "Task ID"
func (s *API) getTask(c echo.Context) error {
	id := c.Param("id")
	t, err := s.ds.GetTaskByID(c.Request().Context(), id)
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, err.Error())
	}
	if err := s.onReadTask(c.Request().Context(), task.Read, t); err != nil {
		return err
	}
	return c.JSON(http.StatusOK, t)
}

func (s *API) getMetrics(c echo.Context) error {
	metrics, err := s.ds.GetMetrics(c.Request().Context())
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, metrics)
}

// Job
// @Summary Restart a cancelled/failed job
// @Tags jobs
// @Produce application/json
// @Success 200 {string} string "OK"
// @Failure 404 {object} echo.HTTPError
// @Failure 400 {object} echo.HTTPError
// @Router /jobs/{id}/restart [put]
// @Param id path string true "Job ID"
func (s *API) restartJob(c echo.Context) error {
	id := c.Param("id")
	j, err := s.ds.GetJobByID(c.Request().Context(), id)
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, err.Error())
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

// Job
// @Summary Cancel a running job
// @Tags jobs
// @Produce application/json
// @Success 200 {string} string "OK"
// @Router /jobs/{id}/cancel [put]
// @Param id path string true "Job ID"
// @Failure 404 {object} echo.HTTPError
// @Failure 400 {object} echo.HTTPError
func (s *API) cancelJob(c echo.Context) error {
	id := c.Param("id")
	j, err := s.ds.GetJobByID(c.Request().Context(), id)
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, err.Error())
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

func (s *API) Start() error {
	if s.server.Addr != "" {
		if err := httpx.StartAsync(s.server); err != nil {
			return err
		}
	} else {
		// attempting to dynamically assign port
		for port := MIN_PORT; port < MAX_PORT; port++ {
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

func (s *API) Shutdown(ctx context.Context) error {
	close(s.terminate)
	return s.server.Shutdown(ctx)
}
