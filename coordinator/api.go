package coordinator

import (
	"context"

	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/tork/datastore"
	"github.com/tork/job"
	"github.com/tork/mq"
	"github.com/tork/node"
	"github.com/tork/task"
	"github.com/tork/uuid"
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
	r.PUT("/task/:id/cancel", s.cancelTask)
	r.GET("/task/:id", s.getTask)
	r.GET("/queue", s.listQueues)
	r.GET("/node", s.listActiveNodes)
	r.POST("/job", s.createJob)
	r.GET("/job/:id", s.getJob)
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
		c.AbortWithError(http.StatusBadRequest, err)
		return
	}
	c.JSON(http.StatusOK, qs)
}

func (s *api) listActiveNodes(c *gin.Context) {
	nodes, err := s.ds.GetActiveNodes(c, time.Now().Add(-node.LAST_HEARTBEAT_TIMEOUT))
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err)
		return
	}
	c.JSON(http.StatusOK, nodes)
}

func validateTask(t task.Task) error {
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
		if t.Retry.ScalingFactor > 5 {
			return errors.Errorf("can't specify a retry.scalingFactor > 5")
		}
		if t.Retry.ScalingFactor < 2 {
			t.Retry.ScalingFactor = 2
		}
		if t.Retry.Limit < 0 {
			t.Retry.Limit = 0
		}
		if t.Retry.InitialDelay == "" {
			t.Retry.InitialDelay = "1s"
		}
		delay, err := time.ParseDuration(t.Retry.InitialDelay)
		if err != nil {
			return errors.Errorf("invalid initial delay duration: %s", t.Retry.InitialDelay)
		}
		if delay > (time.Minute * 5) {
			return errors.Errorf("can't specify retry.initialDelay greater than 5 minutes")
		}
	}
	return nil
}

func (s *api) createJob(c *gin.Context) {
	j := job.Job{}
	switch c.ContentType() {
	case "application/json":
		if err := c.BindJSON(&j); err != nil {
			c.AbortWithError(http.StatusBadRequest, err)
			return
		}
	case "text/yaml":
		if err := c.BindYAML(&j); err != nil {
			c.AbortWithError(http.StatusBadRequest, err)
			return
		}
	default:
		c.AbortWithError(http.StatusBadRequest, errors.Errorf("unknown content type: %s", c.ContentType()))
		return
	}
	if len(j.Tasks) == 0 {
		c.AbortWithError(http.StatusBadRequest, errors.New("job has not tasks"))
		return
	}
	for ix, t := range j.Tasks {
		if err := validateTask(t); err != nil {
			c.AbortWithError(http.StatusBadRequest, errors.Wrapf(err, "tasks[%d]", ix))
			return
		}
	}
	n := time.Now()
	j.ID = uuid.NewUUID()
	j.State = job.Pending
	j.CreatedAt = n
	if err := s.ds.CreateJob(c, j); err != nil {
		c.AbortWithError(http.StatusInternalServerError, err)
		return
	}
	log.Info().Str("task-id", j.ID).Msg("created job")
	if err := s.broker.PublishJob(c, j); err != nil {
		c.AbortWithError(http.StatusBadRequest, err)
		return
	}
	c.JSON(http.StatusOK, redactJob(j))
}

func (s *api) getJob(c *gin.Context) {
	id := c.Param("id")
	j, err := s.ds.GetJobByID(c, id)
	if err != nil {
		c.AbortWithError(http.StatusNotFound, err)
		return
	}
	c.JSON(http.StatusOK, redactJob(j))
}

func (s *api) getTask(c *gin.Context) {
	id := c.Param("id")
	t, err := s.ds.GetTaskByID(c, id)
	if err != nil {
		c.AbortWithError(http.StatusNotFound, err)
		return
	}
	c.JSON(http.StatusOK, redactTask(t))
}

func (s *api) cancelTask(c *gin.Context) {
	id := c.Param("id")
	err := s.ds.UpdateTask(c, id, func(u *task.Task) error {
		if u.State != task.Running {
			return errors.New("task in not running")
		}
		u.State = task.Cancelled
		if u.Node != "" {
			node, err := s.ds.GetNodeByID(c, u.Node)
			if err != nil {
				return err
			}
			if err := s.broker.PublishTask(c, node.Queue, *u); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err)
		return
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
