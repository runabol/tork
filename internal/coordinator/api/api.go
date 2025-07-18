package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"syscall"
	"time"

	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"

	"github.com/runabol/tork/broker"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/health"

	"github.com/runabol/tork/input"
	"github.com/runabol/tork/internal/hash"
	"github.com/runabol/tork/internal/httpx"
	"github.com/runabol/tork/middleware/job"
	"github.com/runabol/tork/middleware/task"
	"github.com/runabol/tork/middleware/web"

	"github.com/runabol/tork"

	"gopkg.in/yaml.v3"
)

const (
	MIN_PORT          = 8000
	MAX_PORT          = 8100
	MAX_LOG_PAGE_SIZE = 100
)

type HealthResponse struct {
	Status string `json:"status"`
}

type API struct {
	server     *http.Server
	broker     broker.Broker
	ds         datastore.Datastore
	terminate  chan any
	onReadJob  job.HandlerFunc
	onReadTask task.HandlerFunc
}

type Config struct {
	Broker     broker.Broker
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
		r.GET("/tasks/:id/log", s.getTaskLog)
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
		r.GET("/jobs/:id/log", s.getJobLog)
		r.GET("/jobs", s.listJobs)
		r.PUT("/jobs/:id/cancel", s.cancelJob)
		r.PUT("/jobs/:id/restart", s.restartJob)

		r.POST("/scheduled-jobs", s.createScheduledJob)
		r.GET("/scheduled-jobs", s.listScheduledJobs)
		r.PUT("/scheduled-jobs/:id/pause", s.pauseScheduledJob)
		r.PUT("/scheduled-jobs/:id/resume", s.resumeScheduledJob)
		r.DELETE("/scheduled-jobs/:id", s.deleteScheduledJob)
	}
	if v, ok := cfg.Enabled["metrics"]; !ok || v {
		r.GET("/metrics", s.getMetrics)
	}
	if v, ok := cfg.Enabled["users"]; !ok || v {
		r.POST("/users", s.createUser)
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
// @Success 200 {object} []broker.QueueInfo
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
	contentType := c.Request().Header.Get("content-type")
	var ji input.Job
	switch contentType {
	case "application/json": // json
		if err := bindInputJSON(&ji, c.Request().Body); err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}
	case "text/yaml", "application/x-yaml": // yaml
		if err := bindInputYAML(&ji, c.Request().Body); err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}
	case "": // no content type
		return echo.NewHTTPError(http.StatusBadRequest, "missing content type")
	default:
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("unknown content type: %s", contentType))
	}
	if ji.Wait != nil { // wait for job to complete before responding
		timeout, err := time.ParseDuration(ji.Wait.Timeout)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}
		ch := make(chan any)
		if err := s.broker.SubscribeForEvents(c.Request().Context(), broker.TOPIC_JOB, func(ev any) {
			j, ok := ev.(*tork.Job)
			if !ok {
				log.Error().Msg("unable to cast event to *tork.Job")
			}
			// check if the job is the one we are waiting for
			// and if it's in a terminal state (completed, failed, cancelled)
			if ji.ID() == j.ID && (j.State == tork.JobStateCompleted || j.State == tork.JobStateFailed || j.State == tork.JobStateCancelled) {
				if err := c.JSON(http.StatusOK, j); err != nil {
					log.Error().Msg("error sending job summary")
				}
				close(ch)
			}

		}); err != nil {
			return errors.New("error subscribing for job events")
		}
		if _, err := s.SubmitJob(c.Request().Context(), &ji); err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}
		select {
		case <-c.Request().Context().Done():
			return echo.NewHTTPError(http.StatusRequestTimeout, "request cancelled")
		case <-s.terminate:
			return echo.NewHTTPError(http.StatusInternalServerError, "server shutting down")
		case <-time.After(timeout): // timeout
			return echo.NewHTTPError(http.StatusRequestTimeout, "timeout waiting for job to complete")
		case <-ch: // job terminated
			return nil
		}
	} else {
		if j, err := s.SubmitJob(c.Request().Context(), &ji); err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		} else {
			return c.JSON(http.StatusOK, tork.NewJobSummary(j))
		}
	}
}

func (s *API) SubmitJob(ctx context.Context, ji *input.Job) (*tork.Job, error) {
	if err := ji.Validate(s.ds); err != nil {
		return nil, err
	}
	j := ji.ToJob()
	currentUser := ctx.Value(tork.USERNAME)
	if currentUser != nil {
		cu, ok := currentUser.(string)
		if !ok {
			return nil, errors.Errorf("error casting current user")
		}
		u, err := s.ds.GetUser(ctx, cu)
		if err != nil {
			return nil, err
		}
		j.CreatedBy = u
	}
	if err := s.ds.CreateJob(ctx, j); err != nil {
		return nil, err
	}
	if err := s.broker.PublishJob(ctx, j); err != nil {
		return nil, err
	}
	return j, nil
}

func bindInputJSON(target any, r io.ReadCloser) error {
	body, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(target); err != nil {
		return err
	}
	return nil
}

func bindInputYAML(target any, r io.ReadCloser) error {
	body, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	dec := yaml.NewDecoder(strings.NewReader(string(body)))
	dec.KnownFields(true)
	if err := dec.Decode(target); err != nil {
		return err
	}
	return nil
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

// getJobLog
// @Summary Get a jobs's log
// @Tags jobs
// @Produce application/json
// @Success 200 {object} []tork.TaskLogPart
// @Router /jobs/{id}/log [get]
// @Param id path string true "Job ID"
// @Param page query int false "page number"
// @Param size query int false "page size"
func (s *API) getJobLog(c echo.Context) error {
	id := c.Param("id")
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
		si = "25"
	}
	q := c.QueryParam("q")

	size, err := strconv.Atoi(si)
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, fmt.Sprintf("invalid size: %s", ps))
	}
	if size < 1 {
		size = 1
	} else if size > MAX_LOG_PAGE_SIZE {
		size = MAX_LOG_PAGE_SIZE
	}
	_, err = s.ds.GetJobByID(c.Request().Context(), id)
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, err.Error())
	}
	l, err := s.ds.GetJobLogParts(c.Request().Context(), id, q, page, size)
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, err.Error())
	}
	return c.JSON(http.StatusOK, l)
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
	currentUser := c.Request().Context().Value(tork.USERNAME)
	var username string
	if currentUser != nil {
		cu, ok := currentUser.(string)
		if !ok {
			return errors.Errorf("error casting current user")
		}
		username = cu
	}
	res, err := s.ds.GetJobs(c.Request().Context(), username, q, page, size)
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

func (s *API) createScheduledJob(c echo.Context) error {
	contentType := c.Request().Header.Get("content-type")
	var ji input.ScheduledJob
	switch contentType {
	case "application/json":

		if err := bindInputJSON(&ji, c.Request().Body); err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}
	case "text/yaml":
		if err := bindInputYAML(&ji, c.Request().Body); err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}
	default:
		return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("unknown content type: %s", contentType))
	}
	if sj, err := s.submitScheduledJob(c.Request().Context(), &ji); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	} else {
		return c.JSON(http.StatusOK, tork.NewScheduledJobSummary(sj))
	}
}

func (s *API) submitScheduledJob(ctx context.Context, ji *input.ScheduledJob) (*tork.ScheduledJob, error) {
	if err := ji.Validate(s.ds); err != nil {
		return nil, err
	}
	sj := ji.ToScheduledJob()
	currentUser := ctx.Value(tork.USERNAME)
	if currentUser != nil {
		cu, ok := currentUser.(string)
		if !ok {
			return nil, errors.Errorf("error casting current user")
		}
		u, err := s.ds.GetUser(ctx, cu)
		if err != nil {
			return nil, err
		}
		sj.CreatedBy = u
	}
	if err := s.ds.CreateScheduledJob(ctx, sj); err != nil {
		return nil, err
	}
	if err := s.broker.PublishEvent(ctx, broker.TOPIC_SCHEDULED_JOB, sj); err != nil {
		return nil, err
	}
	return sj, nil
}

func (s *API) listScheduledJobs(c echo.Context) error {
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
	currentUser := c.Request().Context().Value(tork.USERNAME)
	var username string
	if currentUser != nil {
		cu, ok := currentUser.(string)
		if !ok {
			return errors.Errorf("error casting current user")
		}
		username = cu
	}
	res, err := s.ds.GetScheduledJobs(c.Request().Context(), username, page, size)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, datastore.Page[*tork.ScheduledJobSummary]{
		Number:     res.Number,
		Size:       res.Size,
		TotalPages: res.TotalPages,
		Items:      res.Items,
		TotalItems: res.TotalItems,
	})
}

func (s *API) pauseScheduledJob(c echo.Context) error {
	id := c.Param("id")
	j, err := s.ds.GetScheduledJobByID(c.Request().Context(), id)
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, err.Error())
	}
	if j.State != tork.ScheduledJobStateActive {
		return echo.NewHTTPError(http.StatusBadRequest, "scheduled job is not active")
	}
	j.State = tork.ScheduledJobStatePaused
	if err := s.ds.UpdateScheduledJob(c.Request().Context(), id, func(u *tork.ScheduledJob) error {
		u.State = tork.ScheduledJobStatePaused
		return nil
	}); err != nil {
		return err
	}
	if err := s.broker.PublishEvent(c.Request().Context(), broker.TOPIC_SCHEDULED_JOB, j); err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]string{"status": "OK"})
}

func (s *API) resumeScheduledJob(c echo.Context) error {
	id := c.Param("id")
	j, err := s.ds.GetScheduledJobByID(c.Request().Context(), id)
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, err.Error())
	}
	if j.State != tork.ScheduledJobStatePaused {
		return echo.NewHTTPError(http.StatusBadRequest, "scheduled job is not paused")
	}
	j.State = tork.ScheduledJobStateActive
	if err := s.ds.UpdateScheduledJob(c.Request().Context(), id, func(u *tork.ScheduledJob) error {
		u.State = tork.ScheduledJobStateActive
		return nil
	}); err != nil {
		return err
	}
	if err := s.broker.PublishEvent(c.Request().Context(), broker.TOPIC_SCHEDULED_JOB, j); err != nil {
		return err
	}
	return c.JSON(http.StatusOK, map[string]string{"status": "OK"})
}

func (s *API) deleteScheduledJob(c echo.Context) error {
	id := c.Param("id")
	j, err := s.ds.GetScheduledJobByID(c.Request().Context(), id)
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, err.Error())
	}
	if err := s.ds.DeleteScheduledJob(c.Request().Context(), id); err != nil {
		return err
	}
	// if the job is active, we need to publish
	// an event to remove it from the scheduler
	if j.State == tork.ScheduledJobStateActive {
		j.State = tork.ScheduledJobStatePaused
		if err := s.broker.PublishEvent(c.Request().Context(), broker.TOPIC_SCHEDULED_JOB, j); err != nil {
			return err
		}
	}
	return c.JSON(http.StatusOK, map[string]string{"status": "OK"})
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

// getTaskLog
// @Summary Get a task's log
// @Tags tasks
// @Produce application/json
// @Success 200 {object} []tork.TaskLogPart
// @Router /tasks/{id}/log [get]
// @Param id path string true "Task ID"
// @Param q query int false "string search"
// @Param page query int false "page number"
// @Param size query int false "page size"
func (s *API) getTaskLog(c echo.Context) error {
	id := c.Param("id")
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
		si = "25"
	}
	size, err := strconv.Atoi(si)
	q := c.QueryParam("q")
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, fmt.Sprintf("invalid size: %s", ps))
	}
	if size < 1 {
		size = 1
	} else if size > MAX_LOG_PAGE_SIZE {
		size = MAX_LOG_PAGE_SIZE
	}
	_, err = s.ds.GetTaskByID(c.Request().Context(), id)
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, err.Error())
	}
	l, err := s.ds.GetTaskLogParts(c.Request().Context(), id, q, page, size)
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, err.Error())
	}
	return c.JSON(http.StatusOK, l)
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
	log.Debug().Msgf("cancel job %s", id)
	j, err := s.ds.GetJobByID(c.Request().Context(), id)
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, err.Error())
	}
	if j.State != tork.JobStateRunning && j.State != tork.JobStateScheduled {
		return echo.NewHTTPError(http.StatusBadRequest, "job is not running")
	}
	j.State = tork.JobStateCancelled
	if err := s.broker.PublishJob(c.Request().Context(), j); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, map[string]string{"status": "OK"})
}

// createUser
// @Summary Create a new user
// @Tags users
// @Accept json
// @Produce json
// @Success 200 {object} tork.User
// @Router /users [post]
// @Param request body tork.User true "body"
func (s *API) createUser(c echo.Context) error {
	var u tork.User
	body, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(body, &u); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	u.Username = strings.TrimSpace(u.Username)
	if u.Username == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "must provide username")
	}
	u.Password = strings.TrimSpace(u.Password)
	if u.Password == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "must provide password")
	}
	passwordHash, err := hash.Password(u.Password)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid password")
	}
	u.PasswordHash = passwordHash
	u.Password = ""
	_, err = s.ds.GetUser(c.Request().Context(), u.Username)
	if err == nil {
		return echo.NewHTTPError(http.StatusBadRequest, "user already exists")
	} else if !errors.Is(err, datastore.ErrUserNotFound) {
		return err
	}
	if err := s.ds.CreateUser(c.Request().Context(), &u); err != nil {
		return err
	} else {
		return c.JSON(http.StatusOK, u)
	}
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
