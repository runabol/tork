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
	r.POST("/task", s.createTask)
	r.GET("/task/:id", s.getTask)
	r.GET("/queue", s.listQueues)
	r.GET("/node", s.listActiveNodes)
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

func (s *api) createTask(c *gin.Context) {
	t := &task.Task{}
	switch c.ContentType() {
	case "application/json":
		if err := c.BindJSON(&t); err != nil {
			c.AbortWithError(http.StatusBadRequest, err)
			return
		}
	case "text/yaml":
		if err := c.BindYAML(&t); err != nil {
			c.AbortWithError(http.StatusBadRequest, err)
			return
		}
	default:
		c.AbortWithError(http.StatusBadRequest, errors.Errorf("unknown content type: %s", c.ContentType()))
		return
	}
	if strings.TrimSpace(t.Image) == "" {
		c.AbortWithError(http.StatusBadRequest, errors.New("missing required field: image"))
		return
	}
	if mq.IsCoordinatorQueue(t.Queue) || strings.HasPrefix(t.Queue, mq.QUEUE_EXCLUSIVE_PREFIX) {
		c.AbortWithError(http.StatusBadRequest, errors.Errorf("can't route to special queue: %s", t.Queue))
		return
	}
	n := time.Now()
	t.ID = uuid.NewUUID()
	t.State = task.Pending
	t.CreatedAt = &n
	if err := s.ds.CreateTask(c, t); err != nil {
		c.AbortWithError(http.StatusInternalServerError, err)
		return
	}
	log.Info().Any("task", t).Msg("received task")
	if err := s.broker.PublishTask(c, mq.QUEUE_PENDING, t); err != nil {
		c.AbortWithError(http.StatusBadRequest, err)
		return
	}
	c.JSON(http.StatusOK, t)
}

func (s *api) getTask(c *gin.Context) {
	id := c.Param("id")
	t, err := s.ds.GetTaskByID(c, id)
	if err != nil {
		c.AbortWithError(http.StatusNotFound, err)
		return
	}
	c.JSON(http.StatusOK, t)
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
