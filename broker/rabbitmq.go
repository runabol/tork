package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
	"github.com/runabol/tork"
	"github.com/runabol/tork/internal/fns"
	"github.com/runabol/tork/internal/syncx"
	"github.com/runabol/tork/internal/uuid"
)

const (
	RABBITMQ_DEFAULT_CONSUMER_TIMEOUT = time.Minute * 30
)

const (
	exchangeTopic       = "amq.topic"
	exchangeDefault     = ""
	keyDefault          = ""
	defaultHeartbeatTTL = 60000
	defaultPriority     = uint8(0)
	maxPriority         = uint8(9)
)

type RabbitMQBroker struct {
	connPool        []*amqp.Connection
	nextConn        int
	queues          *syncx.Map[string, string]
	topics          *syncx.Map[string, string]
	subscriptions   map[string]*subscription
	mu              sync.RWMutex
	url             string
	shuttingDown    bool
	heartbeatTTL    int
	consumerTimeout int
	managementURL   string
	durable         bool
}

type subscription struct {
	qname  string
	ch     *amqp.Channel
	name   string
	done   chan struct{}
	cancel chan struct{}
}

type rabbitq struct {
	Name      string         `json:"name"`
	Messages  int            `json:"messages"`
	Consumers int            `json:"consumers"`
	Unacked   int            `json:"messages_unacknowledged"`
	Arguments map[string]any `json:"arguments"`
}

type Option = func(b *RabbitMQBroker)

// WithHeartbeatTTL sets the TTL for a heartbeat pending in the queue.
// The value of the TTL argument or policy must be a non-negative integer (0 <= n),
// describing the TTL period in milliseconds.
func WithHeartbeatTTL(ttl int) Option {
	return func(b *RabbitMQBroker) {
		b.heartbeatTTL = ttl
	}
}

// WithConsumerTimeout sets the maximum amount of time in
// RabbitMQ will wait for a message ack before from a consumer
// before deeming the message undelivered and shuting down the
// connection. Default: 30 minutes
func WithConsumerTimeoutMS(consumerTimeout time.Duration) Option {
	return func(b *RabbitMQBroker) {
		b.consumerTimeout = int(consumerTimeout.Milliseconds())
	}
}

func WithManagementURL(url string) Option {
	return func(b *RabbitMQBroker) {
		b.managementURL = url
	}
}

// WithDurableQueues sets the durable flag upon queue creation.
// Durable queues can survive broker restarts.
func WithDurableQueues(val bool) Option {
	return func(b *RabbitMQBroker) {
		b.durable = val
	}
}

func NewRabbitMQBroker(url string, opts ...Option) (*RabbitMQBroker, error) {
	connPool := make([]*amqp.Connection, 3)
	for i := 0; i < len(connPool); i++ {
		conn, err := amqp.Dial(url)
		if err != nil {
			return nil, errors.Wrapf(err, "error dialing to RabbitMQ")
		}
		connPool[i] = conn
	}
	b := &RabbitMQBroker{
		queues:          new(syncx.Map[string, string]),
		topics:          new(syncx.Map[string, string]),
		url:             url,
		connPool:        connPool,
		subscriptions:   make(map[string]*subscription),
		heartbeatTTL:    defaultHeartbeatTTL,
		consumerTimeout: int(RABBITMQ_DEFAULT_CONSUMER_TIMEOUT.Milliseconds()),
	}
	for _, o := range opts {
		o(b)
	}
	return b, nil
}

func (b *RabbitMQBroker) Queues(ctx context.Context) ([]QueueInfo, error) {
	rqs, err := b.rabbitQueues(ctx)
	if err != nil {
		return nil, err
	}
	qis := make([]QueueInfo, len(rqs))
	for i, rq := range rqs {
		qis[i] = QueueInfo{
			Name:        rq.Name,
			Size:        rq.Messages,
			Subscribers: rq.Consumers,
			Unacked:     rq.Unacked,
		}
	}
	return qis, nil
}

func (b *RabbitMQBroker) rabbitQueues(ctx context.Context) ([]rabbitq, error) {
	u, err := url.Parse(b.url)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to parse url: %s", b.url)
	}
	var endpoint string
	if b.managementURL != "" {
		endpoint = fmt.Sprintf("%s/api/queues/", b.managementURL)
	} else {
		endpoint = fmt.Sprintf("http://%s:15672/api/queues/", u.Hostname())
	}
	client := &http.Client{}
	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to build get queues request")
	}
	pw, _ := u.User.Password()
	req.SetBasicAuth(u.User.Username(), pw)
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.Wrapf(err, "error getting rabbitmq queues from the API")
	}
	rqs := make([]rabbitq, 0)
	err = json.NewDecoder(resp.Body).Decode(&rqs)
	if err != nil {
		return nil, errors.Wrapf(err, "error unmarshalling API response")
	}
	return rqs, nil
}

func (b *RabbitMQBroker) PublishTask(ctx context.Context, qname string, t *tork.Task) error {
	// it's possible that the coordinator will try to publish to a queue for which there's
	// no active worker yet -- so it's possible that the queue was never created. This
	// ensures that the queue is created so the message don't get dropped.
	conn, err := b.getConnection()
	if err != nil {
		return errors.Wrapf(err, "error getting a connection")
	}
	ch, err := conn.Channel()
	if err != nil {
		return errors.Wrapf(err, "error creating channel")
	}
	defer fns.CloseIgnore(ch)
	return b.publish(ctx, exchangeDefault, qname, t)
}

func (b *RabbitMQBroker) SubscribeForTasks(qname string, handler func(t *tork.Task) error) error {
	return b.subscribe(context.Background(), exchangeDefault, keyDefault, qname, func(msg any) error {
		t, ok := msg.(*tork.Task)
		if !ok {
			return errors.Errorf("expecting a *tork.Task but got %T", t)
		}
		return handler(t)
	})
}

func (b *RabbitMQBroker) subscribe(ctx context.Context, exchange, key, qname string, handler func(msg any) error) error {
	conn, err := b.getConnection()
	if err != nil {
		return errors.Wrapf(err, "error getting a connection")
	}
	ch, err := conn.Channel()
	if err != nil {
		return errors.Wrapf(err, "error creating channel")
	}
	if err := b.declareQueue(exchange, key, qname, ch); err != nil {
		return errors.Wrapf(err, "error (re)declaring queue")
	}
	if err := ch.Qos(1, 0, false); err != nil {
		return errors.Wrapf(err, "error setting qos on channel")
	}
	cname := uuid.NewUUID()
	msgs, err := ch.Consume(
		qname, // queue
		cname, // consumer name
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return errors.Wrapf(err, "unable to subscribe on q: %s", qname)
	}
	log.Debug().Msgf("created channel %s for queue: %s", cname, qname)
	id := uuid.NewUUID()
	sub := &subscription{
		ch:     ch,
		qname:  qname,
		name:   cname,
		done:   make(chan struct{}),
		cancel: make(chan struct{}),
	}
	b.mu.Lock()
	b.subscriptions[id] = sub
	b.mu.Unlock()
	go func() {
		for {
			select {
			case <-sub.cancel:
				log.Debug().Msgf("subscription for queue %s canceled", qname)
				b.mu.Lock()
				delete(b.subscriptions, id)
				b.mu.Unlock()
				if err := ch.Cancel(cname, false); err != nil {
					log.Error().
						Err(err).
						Msgf("error closing channel for queue: %s", qname)
				}
				// close the channel and connection cleanly
				log.Debug().Msgf("closing channel %s for queue: %s", cname, qname)
				fns.CloseIgnore(ch)
				// signal that the subscription is done
				sub.done <- struct{}{}
				return
			case <-ctx.Done():
				log.Debug().Msgf("context canceled, stopping subscription for queue: %s", qname)
				b.mu.Lock()
				delete(b.subscriptions, id)
				b.mu.Unlock()
				if err := ch.Cancel(cname, false); err != nil {
					log.Error().
						Err(err).
						Msgf("error closing channel for queue: %s", qname)
				}
				// delete the queue if it's an exclusive queue
				if strings.HasPrefix(sub.qname, QUEUE_EXCLUSIVE_PREFIX) {
					if _, err := ch.QueueDelete(sub.qname, false, false, false); err != nil {
						log.Error().
							Err(err).
							Msgf("error deleting queue: %s", qname)
					}
				}
				// close the channel and connection cleanly
				log.Debug().Msgf("closing channel %s for queue: %s", cname, qname)
				fns.CloseIgnore(ch)
				// signal that the subscription is done
				sub.done <- struct{}{}
				return
			case d, ok := <-msgs:
				if !ok {
					log.Debug().Msgf("message channel closed for queue: %s", qname)
					// if the channel is closed, we need to re-subscribe
					maxAttempts := 20
					for attempt := 1; !b.shuttingDown && attempt <= maxAttempts; attempt++ {
						log.Info().Msgf("%s channel closed. reconnecting", qname)
						if err := b.subscribe(ctx, exchange, key, qname, handler); err != nil {
							log.Error().
								Err(err).
								Msgf("error reconnecting to %s (attempt %d/%d)", qname, attempt, maxAttempts)
							time.Sleep(time.Second * time.Duration(attempt))
						} else {
							return
						}
					}
					sub.done <- struct{}{}
					return
				}
				if msg, err := deserialize(d.Type, d.Body); err != nil {
					log.Error().
						Err(err).
						Str("queue", qname).
						Str("body", (string(d.Body))).
						Str("type", (string(d.Type))).
						Msg("failed to deserialize message")
				} else {
					if err := handler(msg); err != nil {
						log.Error().
							Err(err).
							Str("queue", qname).
							Str("body", (string(d.Body))).
							Msg("failed to handle message")
						if err := d.Reject(false); err != nil {
							log.Error().
								Err(err).
								Msg("failed to reject message")
						}
					} else {
						if err := d.Ack(false); err != nil {
							log.Error().
								Err(err).
								Msg("failed to ack message")
						}
					}
				}
			}
		}
	}()
	return nil
}

func serialize(msg any) ([]byte, error) {
	mtype := fmt.Sprintf("%T", msg)
	if mtype != "*tork.Task" && mtype != "*tork.Job" && mtype != "*tork.Node" && mtype != "*tork.TaskLogPart" && mtype != "*tork.TaskProgress" && mtype != "*tork.ScheduledJob" {
		return nil, errors.Errorf("unnknown type: %T", msg)
	}
	body, err := json.Marshal(msg)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to serialize the message")
	}
	return body, nil
}

func deserialize(tname string, body []byte) (any, error) {
	switch tname {
	case "*tork.Task":
		t := tork.Task{}
		if err := json.Unmarshal(body, &t); err != nil {
			return nil, err
		}
		return &t, nil
	case "*tork.Job":
		j := tork.Job{}
		if err := json.Unmarshal(body, &j); err != nil {
			return nil, err
		}
		return &j, nil
	case "*tork.Node":
		n := tork.Node{}
		if err := json.Unmarshal(body, &n); err != nil {
			return nil, err
		}
		return &n, nil
	case "*tork.TaskLogPart":
		p := tork.TaskLogPart{}
		if err := json.Unmarshal(body, &p); err != nil {
			return nil, err
		}
		return &p, nil
	case "*tork.ScheduledJob":
		p := tork.ScheduledJob{}
		if err := json.Unmarshal(body, &p); err != nil {
			return nil, err
		}
		return &p, nil
	}
	return nil, errors.Errorf("unknown message type: %s", tname)
}

func (b *RabbitMQBroker) declareQueue(exchange, key, qname string, ch *amqp.Channel) error {
	_, ok := b.queues.Get(qname)
	if ok {
		return nil
	}
	log.Debug().Msgf("declaring queue: %s", qname)
	args := amqp.Table{}
	if qname == QUEUE_HEARTBEAT {
		args["x-message-ttl"] = b.heartbeatTTL
	}
	if IsTaskQueue(qname) {
		args["x-max-priority"] = maxPriority
	}
	args["x-consumer-timeout"] = b.consumerTimeout
	_, err := ch.QueueDeclare(
		qname,
		b.durable,
		false, // delete when unused
		strings.HasPrefix(qname, QUEUE_EXCLUSIVE_PREFIX), // exclusive
		false, // no-wait
		args,  // arguments
	)
	if err != nil {
		return err
	}
	if exchange != exchangeDefault {
		if err := ch.QueueBind(qname, key, exchange, false, nil); err != nil {
			return err
		}
	}

	b.queues.Set(qname, qname)
	return nil
}

func (b *RabbitMQBroker) PublishHeartbeat(ctx context.Context, n *tork.Node) error {
	return b.publish(ctx, exchangeDefault, QUEUE_HEARTBEAT, n)
}

func (b *RabbitMQBroker) publish(ctx context.Context, exchange, key string, msg any) error {
	var priority = defaultPriority
	task, ok := msg.(*tork.Task)
	if ok {
		priority = uint8(task.Priority)
	}
	conn, err := b.getConnection()
	if err != nil {
		return errors.Wrapf(err, "error getting a connection")
	}
	ch, err := conn.Channel()
	if err != nil {
		return errors.Wrapf(err, "error creating channel")
	}
	defer fns.CloseIgnore(ch)
	body, err := serialize(msg)
	if err != nil {
		return err
	}
	err = ch.PublishWithContext(ctx,
		exchange, // exchange
		key,      // routing key
		false,    // mandatory
		false,    // immediate
		amqp.Publishing{
			Type:        fmt.Sprintf("%T", msg),
			ContentType: "application/json",
			Body:        []byte(body),
			Priority:    priority,
		})
	if err != nil {
		return errors.Wrapf(err, "unable to publish message")
	}
	return nil
}

func (b *RabbitMQBroker) SubscribeForHeartbeats(handler func(n *tork.Node) error) error {
	return b.subscribe(context.Background(), exchangeDefault, keyDefault, QUEUE_HEARTBEAT, func(msg any) error {
		n, ok := msg.(*tork.Node)
		if !ok {
			return errors.Errorf("expecting a *tork.Node but got %T", msg)
		}
		return handler(n)
	})
}

func (b *RabbitMQBroker) PublishJob(ctx context.Context, j *tork.Job) error {
	return b.publish(ctx, exchangeDefault, QUEUE_JOBS, j)
}

func (b *RabbitMQBroker) SubscribeForJobs(handler func(j *tork.Job) error) error {
	return b.subscribe(context.Background(), exchangeDefault, keyDefault, QUEUE_JOBS, func(msg any) error {
		j, ok := msg.(*tork.Job)
		if !ok {
			return errors.Errorf("expecting a *tork.Job but got %T", msg)
		}
		return handler(j)
	})
}

func (b *RabbitMQBroker) getConnection() (*amqp.Connection, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.connPool) == 0 {
		return nil, errors.Errorf("connection pool is empty")
	}
	var err error
	var conn *amqp.Connection
	conn = b.connPool[b.nextConn]
	if conn.IsClosed() {
		// clearing the known queues because rabbitmq
		// might have crashed and we would need to
		// re-declare them
		b.queues = new(syncx.Map[string, string])
		log.Warn().Msg("connection is closed. reconnecting to RabbitMQ")
		conn, err = amqp.Dial(b.url)
		if err != nil {
			return nil, errors.Wrapf(err, "error dialing to RabbitMQ")
		}
		b.connPool[b.nextConn] = conn
	}
	b.nextConn = b.nextConn + 1
	if b.nextConn >= len(b.connPool) {
		b.nextConn = 0
	}
	return conn, nil
}

func (b *RabbitMQBroker) isShuttingDown() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.shuttingDown
}

func (b *RabbitMQBroker) Shutdown(ctx context.Context) error {
	// when running in standalone mode both the coordinator
	// and the worker will attempt to shutdown the broker
	if b.isShuttingDown() {
		return nil
	}
	b.mu.Lock()
	b.shuttingDown = true
	b.mu.Unlock()
	// let's give the subscribers a grace
	// period to allow them to cleanly exit
	for _, sub := range b.subscriptions {
		// skip non-coordinator queue
		if !IsCoordinatorQueue(sub.qname) {
			continue
		}
		sub.cancel <- struct{}{}
		log.Debug().
			Msgf("waiting for subscription %s to terminate", sub.qname)
		select {
		case <-ctx.Done():
		case <-sub.done:
		}
	}
	// terminate connections cleanly
	for _, conn := range b.connPool {
		log.Debug().
			Msgf("shutting down connection to %s", conn.RemoteAddr())
		done := make(chan int)
		go func(c *amqp.Connection) {
			if err := c.Close(); err != nil {
				log.Error().
					Err(err).
					Msg("error closing rabbitmq connection")
			}
			done <- 1
		}(conn)
		select {
		case <-ctx.Done():
		case <-done:
		}
	}
	b.mu.Lock()
	b.connPool = []*amqp.Connection{}
	b.mu.Unlock()
	return nil
}

func (b *RabbitMQBroker) SubscribeForEvents(ctx context.Context, pattern string, handler func(event any)) error {
	key := strings.ReplaceAll(pattern, "*", "#")
	qname := fmt.Sprintf("%s%s", QUEUE_EXCLUSIVE_PREFIX, uuid.NewShortUUID())
	return b.subscribe(ctx, exchangeTopic, key, qname, func(msg any) error {
		handler(msg)
		return nil
	})
}

func (b *RabbitMQBroker) PublishEvent(ctx context.Context, topic string, event any) error {
	return b.publish(ctx, exchangeTopic, topic, event)
}

func (b *RabbitMQBroker) HealthCheck(ctx context.Context) error {
	conn, err := b.getConnection()
	if err != nil {
		return errors.Wrapf(err, "error getting a connection")
	}
	ch, err := conn.Channel()
	if err != nil {
		return errors.Wrapf(err, "error creating channel")
	}
	defer fns.CloseIgnore(ch)
	return nil
}

func (b *RabbitMQBroker) PublishTaskLogPart(ctx context.Context, p *tork.TaskLogPart) error {
	return b.publish(ctx, exchangeDefault, QUEUE_LOGS, p)
}

func (b *RabbitMQBroker) SubscribeForTaskLogPart(handler func(p *tork.TaskLogPart)) error {
	return b.subscribe(context.Background(), exchangeDefault, keyDefault, QUEUE_LOGS, func(msg any) error {
		p, ok := msg.(*tork.TaskLogPart)
		if !ok {
			return errors.Errorf("expecting a *tork.TaskLogPart but got %T", msg)
		}
		handler(p)
		return nil
	})
}

func (b *RabbitMQBroker) PublishTaskProgress(ctx context.Context, tp *tork.Task) error {
	return b.publish(ctx, exchangeDefault, QUEUE_PROGRESS, tp)
}

func (b *RabbitMQBroker) SubscribeForTaskProgress(handler func(t *tork.Task) error) error {
	return b.subscribe(context.Background(), exchangeDefault, keyDefault, QUEUE_PROGRESS, func(msg any) error {
		p, ok := msg.(*tork.Task)
		if !ok {
			return errors.Errorf("expecting a *tork.TaskProgress but got %T", msg)
		}
		return handler(p)
	})
}
