package broker

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
	"github.com/runabol/tork/internal/syncx"
	"github.com/runabol/tork/internal/wildcard"
)

// NATSBroker is a broker implementation using NATS for message passing.
type NATSBroker struct {
	conn      *nats.Conn
	server    *server.Server // Optional embedded NATS server
	queues    *syncx.Map[string, *natsQueue]
	topics    *syncx.Map[string, *natsTopic]
	terminate *atomic.Bool
	subs      sync.Map // Store NATS subscriptions
}

// natsQueue represents a NATS queue subscription.
type natsQueue struct {
	name    string
	subs    []*nats.Subscription
	unacked int32
	mu      sync.Mutex
}

// natsTopic represents a NATS subject subscription.
type natsTopic struct {
	name string
	subs []*nats.Subscription
	mu   sync.RWMutex
}

// Config holds NATS broker configuration.
type NATSConfig struct {
	// NATSURL is the NATS server URL (e.g., "nats://localhost:4222").
	// If empty and Embedded is false, defaults to nats.DefaultURL.
	NATSURL string
	// Embedded indicates whether to run an embedded NATS server.
	Embedded bool
	// DataDir is the directory for embedded server persistence (optional).
	DataDir string
}

// NewNATSBroker creates a new NATS-based broker.
func NewNATSBroker(cfg NATSConfig) (*NATSBroker, error) {
	b := &NATSBroker{
		queues:    new(syncx.Map[string, *natsQueue]),
		topics:    new(syncx.Map[string, *natsTopic]),
		terminate: new(atomic.Bool),
	}

	// Start embedded NATS server if configured
	if cfg.Embedded {
		opts := &server.Options{
			Host:   "localhost",
			Port:   4222,
			NoLog:  false,
			NoSigs: true,
		}
		if cfg.DataDir != "" {
			opts.StoreDir = cfg.DataDir
		}
		srv, err := server.NewServer(opts)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create embedded NATS server")
		}
		srv.Start()
		if !srv.ReadyForConnections(10 * time.Second) {
			srv.Shutdown()
			return nil, errors.New("embedded NATS server failed to start")
		}
		b.server = srv
		cfg.NATSURL = srv.Addr().String()
	}

	// Connect to NATS server
	natsURL := cfg.NATSURL
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}
	conn, err := nats.Connect(natsURL, nats.Timeout(10*time.Second))
	if err != nil {
		if b.server != nil {
			b.server.Shutdown()
		}
		return nil, errors.Wrap(err, "failed to connect to NATS")
	}
	b.conn = conn

	return b, nil
}

// PublishTask publishes a task to a NATS queue.
func (b *NATSBroker) PublishTask(ctx context.Context, qname string, t *tork.Task) error {
	log.Debug().Msgf("publish task %s to %s queue", t.ID, qname)
	data, err := json.Marshal(t)
	if err != nil {
		return errors.Wrap(err, "failed to marshal task")
	}
	return b.conn.Publish(qname, data)
}

// SubscribeForTasks subscribes to a NATS queue for tasks.
func (b *NATSBroker) SubscribeForTasks(qname string, handler func(t *tork.Task) error) error {
	return b.subscribe(qname, func(m any) error {
		data, ok := m.(*nats.Msg)
		if !ok {
			return errors.Errorf("can't cast message to NATS message")
		}
		t := &tork.Task{}
		if err := json.Unmarshal(data.Data, t); err != nil {
			return errors.Wrap(err, "failed to unmarshal task")
		}
		return handler(t.Clone())
	})
}

// subscribe creates a NATS queue subscription.
func (b *NATSBroker) subscribe(qname string, handler func(m any) error) error {
	log.Debug().Msgf("subscribing for tasks on %s", qname)
	q, ok := b.queues.Get(qname)
	if !ok {
		q = &natsQueue{name: qname}
		b.queues.Set(qname, q)
	}

	// Create queue subscription
	sub, err := b.conn.QueueSubscribe(qname, qname, func(msg *nats.Msg) {
		atomic.AddInt32(&q.unacked, 1)
		if err := handler(msg); err != nil {
			log.Error().Err(err).Msg("unexpected error occurred while processing task")
		}
		atomic.AddInt32(&q.unacked, -1)
	})
	if err != nil {
		return errors.Wrap(err, "failed to subscribe to queue")
	}

	// Store subscription
	q.mu.Lock()
	q.subs = append(q.subs, sub)
	q.mu.Unlock()
	b.subs.Store(sub, true)

	return nil
}

// Queues returns information about active queues.
func (b *NATSBroker) Queues(ctx context.Context) ([]QueueInfo, error) {
	qi := make([]QueueInfo, 0)
	b.queues.Iterate(func(_ string, q *natsQueue) {
		qi = append(qi, QueueInfo{
			Name:        q.name,
			Size:        0, // NATS doesn't expose queue size directly
			Subscribers: len(q.subs),
			Unacked:     int(atomic.LoadInt32(&q.unacked)),
		})
	})
	return qi, nil
}

// PublishHeartbeat publishes a node heartbeat.
func (b *NATSBroker) PublishHeartbeat(_ context.Context, n *tork.Node) error {
	data, err := json.Marshal(n)
	if err != nil {
		return errors.Wrap(err, "failed to marshal node")
	}
	return b.conn.Publish(QUEUE_HEARTBEAT, data)
}

// SubscribeForHeartbeats subscribes to node heartbeats.
func (b *NATSBroker) SubscribeForHeartbeats(handler func(n *tork.Node) error) error {
	return b.subscribe(QUEUE_HEARTBEAT, func(m any) error {
		msg, ok := m.(*nats.Msg)
		if !ok {
			return errors.New("can't cast to NATS message")
		}
		n := &tork.Node{}
		if err := json.Unmarshal(msg.Data, n); err != nil {
			return errors.Wrap(err, "failed to unmarshal node")
		}
		return handler(n)
	})
}

// PublishJob publishes a job.
func (b *NATSBroker) PublishJob(ctx context.Context, j *tork.Job) error {
	data, err := json.Marshal(j)
	if err != nil {
		return errors.Wrap(err, "failed to marshal job")
	}
	return b.conn.Publish(QUEUE_JOBS, data)
}

// SubscribeForJobs subscribes to jobs.
func (b *NATSBroker) SubscribeForJobs(handler func(j *tork.Job) error) error {
	return b.subscribe(QUEUE_JOBS, func(m any) error {
		msg, ok := m.(*nats.Msg)
		if !ok {
			return errors.New("can't cast to NATS message")
		}
		j := &tork.Job{}
		if err := json.Unmarshal(msg.Data, j); err != nil {
			return errors.Wrap(err, "failed to unmarshal job")
		}
		return handler(j)
	})
}

// Shutdown gracefully shuts down the broker.
func (b *NATSBroker) Shutdown(ctx context.Context) error {
	if !b.terminate.CompareAndSwap(false, true) {
		return nil
	}

	done := make(chan struct{})
	go func() {
		// Unsubscribe all subscriptions
		b.subs.Range(func(key, _ interface{}) bool {
			if sub, ok := key.(*nats.Subscription); ok {
				if err := sub.Unsubscribe(); err != nil {
					log.Error().Err(err).Msg("failed to unsubscribe")
				}
			}
			return true
		})

		// Close NATS connection
		b.conn.Close()

		// Shutdown embedded server if present
		if b.server != nil {
			b.server.Shutdown()
		}

		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

// SubscribeForEvents subscribes to a NATS subject for events.
func (b *NATSBroker) SubscribeForEvents(ctx context.Context, topic string, handler func(event any)) error {
	log.Debug().Msgf("subscribing for events on %s", topic)
	t, ok := b.topics.Get(topic)
	if !ok {
		t = &natsTopic{name: topic}
		b.topics.Set(topic, t)
	}

	// Subscribe to NATS subject
	sub, err := b.conn.Subscribe(topic, func(msg *nats.Msg) {
		var event any
		if err := json.Unmarshal(msg.Data, &event); err != nil {
			log.Error().Err(err).Msg("failed to unmarshal event")
			return
		}
		handler(event)
	})
	if err != nil {
		return errors.Wrap(err, "failed to subscribe to topic")
	}

	// Store subscription
	t.mu.Lock()
	t.subs = append(t.subs, sub)
	t.mu.Unlock()
	b.subs.Store(sub, true)

	return nil
}

// PublishEvent publishes an event to matching NATS subjects.
func (b *NATSBroker) PublishEvent(ctx context.Context, topicName string, event any) error {
	data, err := json.Marshal(event)
	if err != nil {
		return errors.Wrap(err, "failed to marshal event")
	}
	b.topics.Iterate(func(name string, topic *natsTopic) {
		if wildcard.Match(name, topicName) {
			if err := b.conn.Publish(name, data); err != nil {
				log.Error().Err(err).Msgf("failed to publish to topic %s", name)
			}
		}
	})
	return nil
}

// HealthCheck checks the broker's health.
func (b *NATSBroker) HealthCheck(ctx context.Context) error {
	if b.terminate.Load() {
		return errors.New("broker is terminated")
	}
	if b.conn.IsClosed() {
		return errors.New("NATS connection is closed")
	}
	return nil
}

// PublishTaskLogPart publishes a task log part.
func (b *NATSBroker) PublishTaskLogPart(ctx context.Context, p *tork.TaskLogPart) error {
	data, err := json.Marshal(p)
	if err != nil {
		return errors.Wrap(err, "failed to marshal task log part")
	}
	return b.conn.Publish(QUEUE_LOGS, data)
}

// SubscribeForTaskLogPart subscribes to task log parts.
func (b *NATSBroker) SubscribeForTaskLogPart(handler func(p *tork.TaskLogPart)) error {
	return b.subscribe(QUEUE_LOGS, func(m any) error {
		msg, ok := m.(*nats.Msg)
		if !ok {
			return errors.New("can't cast to NATS message")
		}
		p := &tork.TaskLogPart{}
		if err := json.Unmarshal(msg.Data, p); err != nil {
			return errors.Wrap(err, "failed to unmarshal task log part")
		}
		handler(p)
		return nil
	})
}

// PublishTaskProgress publishes task progress.
func (b *NATSBroker) PublishTaskProgress(ctx context.Context, tp *tork.Task) error {
	data, err := json.Marshal(tp)
	if err != nil {
		return errors.Wrap(err, "failed to marshal task progress")
	}
	return b.conn.Publish(QUEUE_PROGRESS, data)
}

// SubscribeForTaskProgress subscribes to task progress.
func (b *NATSBroker) SubscribeForTaskProgress(handler func(tp *tork.Task) error) error {
	return b.subscribe(QUEUE_PROGRESS, func(m any) error {
		msg, ok := m.(*nats.Msg)
		if !ok {
			return errors.New("can't cast to NATS message")
		}
		p := &tork.Task{}
		if err := json.Unmarshal(msg.Data, p); err != nil {
			return errors.Wrap(err, "failed to unmarshal task progress")
		}
		return handler(p)
	})
}
