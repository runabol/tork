package coordinator

import (
	"context"
	"encoding/json"
	"io"
	"strconv"

	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/job"
	"github.com/runabol/tork/mq"
	"github.com/runabol/tork/node"
	"gopkg.in/yaml.v3"
)

type api struct {
	server *http.Server
	broker mq.Broker
	ds     datastore.Datastore
}

func newAPI(cfg Config) *api {
	if cfg.Address == "" {
		cfg.Address = ":8000"
	}
	if !cfg.Debug {
		gin.SetMode(gin.ReleaseMode)
	}
	r := gin.Default()

	r.Use(errorHandler)
	s := &api{
		broker: cfg.Broker,
		server: &http.Server{
			Addr:    cfg.Address,
			Handler: r,
		},
		ds: cfg.DataStore,
	}
	r.GET("/status", s.status)
	r.GET("/tasks/:id", s.getTask)
	r.GET("/queues", s.listQueues)
	r.GET("/nodes", s.listActiveNodes)
	r.POST("/jobs", s.createJob)
	r.GET("/jobs/:id", s.getJob)
	r.GET("/jobs", s.listJobs)
	r.PUT("/jobs/:id/cancel", s.cancelJob)
	r.PUT("/jobs/:id/restart", s.restartJob)
	return s
}

func errorHandler(c *gin.Context) {
	c.Next()
	if len(c.Errors) > 0 {
		c.JSON(-1, c.Errors[0])
	}
}

func (s *api) status(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "OK"})
}

func (s *api) listQueues(c *gin.Context) {
	qs, err := s.broker.Queues(c)
	if err != nil {
		_ = c.AbortWithError(http.StatusBadRequest, err)
		return
	}
	c.JSON(http.StatusOK, qs)
}

func (s *api) listActiveNodes(c *gin.Context) {
	nodes, err := s.ds.GetActiveNodes(c, time.Now().UTC().Add(-node.LAST_HEARTBEAT_TIMEOUT))
	if err != nil {
		_ = c.AbortWithError(http.StatusBadRequest, err)
		return
	}
	c.JSON(http.StatusOK, nodes)
}

func (s *api) createJob(c *gin.Context) {
	var ji *jobInput
	var err error
	switch c.ContentType() {
	case "application/json":
		ji, err = bindJobInputJSON(c.Request.Body)
		if err != nil {
			_ = c.AbortWithError(http.StatusBadRequest, err)
			return
		}
	case "text/yaml":
		ji, err = bindJobInputYAML(c.Request.Body)
		if err != nil {
			_ = c.AbortWithError(http.StatusBadRequest, err)
			return
		}
	default:
		_ = c.AbortWithError(http.StatusBadRequest, errors.Errorf("unknown content type: %s", c.ContentType()))
		return
	}
	if err := ji.validate(); err != nil {
		_ = c.AbortWithError(http.StatusBadRequest, err)
		return
	}
	j := ji.toJob()
	if err := s.ds.CreateJob(c, j); err != nil {
		_ = c.AbortWithError(http.StatusInternalServerError, err)
		return
	}
	log.Info().Str("job-id", j.ID).Msg("created job")
	if err := s.broker.PublishJob(c, j); err != nil {
		_ = c.AbortWithError(http.StatusBadRequest, err)
		return
	}
	c.JSON(http.StatusOK, redactJob(j))
}

func bindJobInputJSON(r io.ReadCloser) (*jobInput, error) {
	ji := jobInput{}
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

func bindJobInputYAML(r io.ReadCloser) (*jobInput, error) {
	ji := jobInput{}
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

func (s *api) getJob(c *gin.Context) {
	id := c.Param("id")
	j, err := s.ds.GetJobByID(c, id)
	if err != nil {
		_ = c.AbortWithError(http.StatusNotFound, err)
		return
	}
	c.JSON(http.StatusOK, redactJob(j))
}

func (s *api) listJobs(c *gin.Context) {
	ps := c.DefaultQuery("page", "1")
	page, err := strconv.Atoi(ps)
	if err != nil {
		_ = c.AbortWithError(http.StatusBadRequest, errors.Wrapf(err, "invalid page number: %s", ps))
		return
	}
	if page < 1 {
		page = 1
	}
	si := c.DefaultQuery("size", "10")
	size, err := strconv.Atoi(si)
	if err != nil {
		_ = c.AbortWithError(http.StatusBadRequest, errors.Wrapf(err, "invalid size: %s", ps))
		return
	}
	if size < 1 {
		size = 1
	} else if size > 20 {
		size = 20
	}
	res, err := s.ds.GetJobs(c, page, size)
	if err != nil {
		_ = c.AbortWithError(http.StatusInternalServerError, err)
		return
	}
	c.JSON(http.StatusOK, datastore.Page[*job.Job]{
		Number:     res.Number,
		Size:       res.Size,
		TotalPages: res.TotalPages,
		Items:      redactJobs(res.Items),
		TotalItems: res.TotalItems,
	})
}

func (s *api) getTask(c *gin.Context) {
	id := c.Param("id")
	t, err := s.ds.GetTaskByID(c, id)
	if err != nil {
		_ = c.AbortWithError(http.StatusNotFound, err)
		return
	}
	c.JSON(http.StatusOK, redactTask(t))
}

func (s *api) restartJob(c *gin.Context) {
	id := c.Param("id")
	j, err := s.ds.GetJobByID(c, id)
	if err != nil {
		_ = c.AbortWithError(http.StatusInternalServerError, err)
		return
	}
	if j.State != job.Failed && j.State != job.Cancelled {
		_ = c.AbortWithError(http.StatusBadRequest, errors.Errorf("job is %s and can not be restarted", j.State))
		return
	}
	if j.Position > len(j.Tasks) {
		_ = c.AbortWithError(http.StatusBadRequest, errors.Errorf("job has no more tasks to run"))
		return
	}
	j.State = job.Restart
	if err := s.broker.PublishJob(c, j); err != nil {
		_ = c.AbortWithError(http.StatusInternalServerError, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "OK"})
}

func (s *api) cancelJob(c *gin.Context) {
	id := c.Param("id")
	j, err := s.ds.GetJobByID(c, id)
	if err != nil {
		_ = c.AbortWithError(http.StatusInternalServerError, err)
		return
	}
	if j.State != job.Running {
		_ = c.AbortWithError(http.StatusBadRequest, errors.New("job is not running"))
		return
	}
	j.State = job.Cancelled
	if err := s.broker.PublishJob(c, j); err != nil {
		_ = c.AbortWithError(http.StatusInternalServerError, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "OK"})
}

func (s *api) start() error {
	go func() {
		log.Info().Msgf("listening on %s", s.server.Addr)
		// service connections
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msgf("error starting up server")
		}
	}()
	return nil
}

func (s *api) shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}
