package coordinator

import (
	"context"

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
	"github.com/runabol/tork/task"
	"github.com/runabol/tork/uuid"
)

type api struct {
	server *http.Server
	broker mq.Broker
	ds     datastore.Datastore
}

func newAPI(cfg Config) *api {
	if cfg.Address == "" {
		cfg.Address = ":3000"
	}
	gin.SetMode(gin.ReleaseMode)
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
	r.GET("/task/:id", s.getTask)
	r.GET("/queue", s.listQueues)
	r.GET("/node", s.listActiveNodes)
	r.POST("/job", s.createJob)
	r.GET("/job/:id", s.getJob)
	r.PUT("/job/:id/cancel", s.cancelJob)
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

func sanitizeTask(t *task.Task) error {
	if len(t.Parallel) > 0 && t.Each != nil {
		return errors.New("parallel and each tasks are mutually exclusive")
	}
	if len(t.Parallel) > 0 && t.SubJob != nil {
		return errors.New("parallel and subjob tasks are mutually exclusive")
	}
	if t.Each != nil && t.SubJob != nil {
		return errors.New("each and subjob tasks are mutually exclusive")
	}
	if (len(t.Parallel) > 0 || t.Each != nil) && (len(t.Pre) > 0 || len(t.Post) > 0) {
		return errors.New("composite tasks do not support pre/post tasks")
	}
	if (len(t.Parallel) > 0 || t.Each != nil) && t.Image != "" {
		return errors.New("composite tasks do not support image")
	}
	if (len(t.Parallel) > 0 || t.Each != nil) && t.Queue != "" {
		return errors.New("composite tasks do not support queue assignment")
	}
	if t.SubJob != nil && (len(t.Pre) > 0 || len(t.Post) > 0) {
		return errors.New("subjob tasks do not support pre/post tasks")
	}
	if t.SubJob != nil && t.Image != "" {
		return errors.New("subjob tasks do not support image")
	}
	if t.SubJob != nil && t.Queue != "" {
		return errors.New("subjob tasks do not support queue assignment")
	}
	if t.Each != nil {
		if t.Each.List == "" {
			return errors.New("each must provide a list expression")
		}
		if t.Each.Task == nil {
			return errors.New("each must provide a task")
		}
		if err := sanitizeTask(t.Each.Task); err != nil {
			return err
		}
	}
	for _, pt := range t.Parallel {
		if err := sanitizeTask(pt); err != nil {
			return err
		}
	}
	if len(t.Parallel) > 0 || t.Each != nil || t.SubJob != nil {
		return nil
	}
	if strings.TrimSpace(t.Image) == "" {
		return errors.New("missing required field: image")
	}
	if mq.IsCoordinatorQueue(t.Queue) || strings.HasPrefix(t.Queue, mq.QUEUE_EXCLUSIVE_PREFIX) {
		return errors.Errorf("can't route to special queue: %s", t.Queue)
	}
	if t.Retry != nil {
		if t.Retry.Attempts != 0 {
			return errors.Errorf("can't specify retry.attempts")
		}
		if t.Retry.Limit > 10 {
			return errors.Errorf("can't specify retry.limit > 10")
		}
		if t.Retry.Limit < 0 {
			t.Retry.Limit = 0
		}
		if t.Retry.Attempts != 0 {
			t.Retry.Attempts = 0
		}
	}
	if t.Timeout != "" {
		timeout, err := time.ParseDuration(t.Timeout)
		if err != nil {
			return errors.Errorf("invalid timeout duration: %s", t.Timeout)
		}
		if timeout < 0 {
			return errors.Errorf("invalid timeout duration: %s", t.Timeout)
		}
	}
	return nil
}

func (s *api) createJob(c *gin.Context) {
	j := &job.Job{}
	switch c.ContentType() {
	case "application/json":
		if err := c.BindJSON(&j); err != nil {
			_ = c.AbortWithError(http.StatusBadRequest, err)
			return
		}
	case "text/yaml":
		if err := c.BindYAML(&j); err != nil {
			_ = c.AbortWithError(http.StatusBadRequest, err)
			return
		}
	default:
		_ = c.AbortWithError(http.StatusBadRequest, errors.Errorf("unknown content type: %s", c.ContentType()))
		return
	}
	if len(j.Tasks) == 0 {
		_ = c.AbortWithError(http.StatusBadRequest, errors.New("job has no tasks"))
		return
	}
	for ix, t := range j.Tasks {
		if err := sanitizeTask(t); err != nil {
			_ = c.AbortWithError(http.StatusBadRequest, errors.Wrapf(err, "tasks[%d]", ix))
			return
		}
	}
	n := time.Now()
	j.ID = uuid.NewUUID()
	j.State = job.Pending
	j.CreatedAt = n
	j.Context = job.Context{}
	j.Context.Inputs = j.Inputs
	if err := s.ds.CreateJob(c, j); err != nil {
		_ = c.AbortWithError(http.StatusInternalServerError, err)
		return
	}
	log.Info().Str("task-id", j.ID).Msg("created job")
	if err := s.broker.PublishJob(c, j); err != nil {
		_ = c.AbortWithError(http.StatusBadRequest, err)
		return
	}
	c.JSON(http.StatusOK, redactJob(j))
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

func (s *api) getTask(c *gin.Context) {
	id := c.Param("id")
	t, err := s.ds.GetTaskByID(c, id)
	if err != nil {
		_ = c.AbortWithError(http.StatusNotFound, err)
		return
	}
	c.JSON(http.StatusOK, redactTask(t))
}

func (s *api) cancelJob(c *gin.Context) {
	id := c.Param("id")
	err := s.ds.UpdateJob(c, id, func(u *job.Job) error {
		if u.State != job.Running {
			return errors.New("job in not running")
		}
		u.State = job.Cancelled
		return nil
	})
	if err != nil {
		_ = c.AbortWithError(http.StatusBadRequest, err)
		return
	}
	tasks, err := s.ds.GetActiveTasks(c, id)
	if err != nil {
		_ = c.AbortWithError(http.StatusInternalServerError, err)
		return
	}
	for _, t := range tasks {
		err := s.ds.UpdateTask(c, t.ID, func(u *task.Task) error {
			u.State = task.Cancelled
			// notify the node to cancel the task
			if u.Node != "" {
				node, err := s.ds.GetNodeByID(c, u.Node)
				if err != nil {
					return err
				}
				if err := s.broker.PublishTask(c, node.Queue, u); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			_ = c.AbortWithError(http.StatusBadRequest, err)
			return
		}
	}
	c.JSON(http.StatusOK, gin.H{"status": "OK"})
}

func (s *api) start() error {
	go func() {
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
