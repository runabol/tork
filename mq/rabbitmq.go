package mq

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
	"github.com/runabol/tork/internal/syncx"
	"github.com/runabol/tork/internal/uuid"
)

const (
	exchangeTopic       = "amq.topic"
	exchangeDefault     = ""
	keyDefault          = ""
	defaultHeartbeatTTL = 60000
)

type RabbitMQBroker struct {
	connPool      []*amqp.Connection
	nextConn      int
	queues        *syncx.Map[string, string]
	topics        *syncx.Map[string, string]
	subscriptions []*subscription
	mu            sync.RWMutex
	url           string
	shuttingDown  bool
	heartbeatTTL  int
}

type subscription struct {
	qname string
	ch    *amqp.Channel
	name  string
	done  chan int
}

type Option = func(b *RabbitMQBroker)

// WithHeartbeatTTL sets the TTL for a hearbeat pending in the queue.
// The value of the TTL argument or policy must be a non-negative integer (0 <= n),
// describing the TTL period in milliseconds.
func WithHeartbeatTTL(ttl int) Option {
	return func(b *RabbitMQBroker) {
		b.heartbeatTTL = ttl
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
		queues:        new(syncx.Map[string, string]),
		topics:        new(syncx.Map[string, string]),
		url:           url,
		connPool:      connPool,
		subscriptions: make([]*subscription, 0),
		heartbeatTTL:  defaultHeartbeatTTL,
	}
	for _, o := range opts {
		o(b)
	}
	return b, nil
}

func (b *RabbitMQBroker) Queues(ctx context.Context) ([]QueueInfo, error) {
	u, err := url.Parse(b.url)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to parse url: %s", b.url)
	}
	manager := fmt.Sprintf("http://%s:15672/api/queues/", u.Hostname())
	client := &http.Client{}
	req, err := http.NewRequestWithContext(ctx, "GET", manager, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to build get queues request")
	}
	pw, _ := u.User.Password()
	req.SetBasicAuth(u.User.Username(), pw)
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.Wrapf(err, "error getting rabbitmq queues from the API")
	}

	type rabbitq struct {
		Name      string `json:"name"`
		Messages  int    `json:"messages"`
		Consumers int    `json:"consumers"`
		Unacked   int    `json:"messages_unacknowledged"`
	}

	rqs := make([]rabbitq, 0)
	err = json.NewDecoder(resp.Body).Decode(&rqs)
	if err != nil {
		return nil, errors.Wrapf(err, "error unmarshalling API response")
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
	defer ch.Close()
	if err := b.declareQueue(exchangeDefault, keyDefault, qname, ch); err != nil {
		return errors.Wrapf(err, "error (re)declaring queue")
	}
	return b.publish(ctx, exchangeDefault, qname, t)
}

func (b *RabbitMQBroker) SubscribeForTasks(qname string, handler func(t *tork.Task) error) error {
	return b.subscribe(exchangeDefault, keyDefault, qname, func(msg any) error {
		t, ok := msg.(*tork.Task)
		if !ok {
			return errors.Errorf("expecting a *tork.Task but got %T", t)
		}
		return handler(t)
	})
}

func (b *RabbitMQBroker) subscribe(exchange, key, qname string, handler func(msg any) error) error {
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
	sub := &subscription{
		ch:    ch,
		qname: qname,
		name:  cname,
		done:  make(chan int),
	}
	b.mu.Lock()
	b.subscriptions = append(b.subscriptions, sub)
	b.mu.Unlock()
	go func() {
		for d := range msgs {
			if msg, err := deserialize(d.Type, d.Body); err != nil {
				log.Error().
					Err(err).
					Str("queue", qname).
					Str("body", (string(d.Body))).
					Str("type", (string(d.Type))).
					Msg("failed to deserialized message")
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
							Msg("failed to ack message")
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
		maxAttempts := 20
		for attempt := 1; !b.shuttingDown && attempt <= maxAttempts; attempt++ {
			log.Info().Msgf("%s channel closed. reconnecting", qname)
			if err := b.subscribe(exchange, key, qname, handler); err != nil {
				log.Error().
					Err(err).
					Msgf("error reconnecting to %s (attempt %d/%d)", qname, attempt, maxAttempts)
				time.Sleep(time.Second * time.Duration(attempt))
			} else {
				return
			}
		}
		sub.done <- 1
	}()
	return nil
}

func serialize(msg any) ([]byte, error) {
	mtype := fmt.Sprintf("%T", msg)
	if mtype != "*tork.Task" && mtype != "*tork.Job" && mtype != "*tork.Node" {
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
	}
	return nil, errors.Errorf("unknown message type: %s", tname)
}

func (b *RabbitMQBroker) declareQueue(exchange, key, qname string, ch *amqp.Channel) error {
	_, ok := b.queues.Get(qname)
	if ok {
		return nil
	}
	// get a list of existing queues
	qs, err := b.Queues(context.Background())
	if err != nil {
		return err
	}
	exists := false
	for _, q := range qs {
		if q.Name == qname {
			exists = true
		}
	}
	if !exists {
		log.Debug().Msgf("declaring queue: %s", qname)
		args := amqp.Table{}
		if qname == QUEUE_HEARBEAT {
			args["x-message-ttl"] = b.heartbeatTTL
		}
		_, err = ch.QueueDeclare(
			qname,
			false, // durable
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
	}
	b.queues.Set(qname, qname)
	return nil
}

func (b *RabbitMQBroker) PublishHeartbeat(ctx context.Context, n *tork.Node) error {
	return b.publish(ctx, exchangeDefault, QUEUE_HEARBEAT, n)
}

func (b *RabbitMQBroker) publish(ctx context.Context, exchange, key string, msg any) error {
	conn, err := b.getConnection()
	if err != nil {
		return errors.Wrapf(err, "error getting a connection")
	}
	ch, err := conn.Channel()
	if err != nil {
		return errors.Wrapf(err, "error creating channel")
	}
	defer ch.Close()
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
		})
	if err != nil {
		return errors.Wrapf(err, "unable to publish message")
	}
	return nil
}

func (b *RabbitMQBroker) SubscribeForHeartbeats(handler func(n *tork.Node) error) error {
	return b.subscribe(exchangeDefault, keyDefault, QUEUE_HEARBEAT, func(msg any) error {
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
	return b.subscribe(exchangeDefault, keyDefault, QUEUE_JOBS, func(msg any) error {
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
	// close channels cleanly
	for _, sub := range b.subscriptions {
		log.Debug().
			Msgf("shutting down subscription on %s", sub.qname)
		if err := sub.ch.Cancel(sub.name, false); err != nil {
			log.Error().
				Err(err).
				Msgf("error closing channel for %s", sub.qname)
		}
	}
	// let's give the subscribers a grace
	// period to allow them to cleanly exit
	for _, sub := range b.subscriptions {
		// skip non-coordinator queue
		if !IsCoordinatorQueue(sub.qname) {
			continue
		}
		log.Debug().
			Msgf("waiting for subscription %s to terminate", sub.qname)
		select {
		case <-ctx.Done():
		case <-sub.done:
		}
	}
	b.mu.Lock()
	b.subscriptions = []*subscription{}
	b.mu.Unlock()
	// terminate connections cleanly
	for _, conn := range b.connPool {
		log.Debug().
			Msgf("shutting down connection to %s", conn.RemoteAddr())
		var done chan int = make(chan int)
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
	qname := fmt.Sprintf("%s-%s", QUEUE_EXCLUSIVE_PREFIX, uuid.NewUUID())
	return b.subscribe(exchangeTopic, key, qname, func(msg any) error {
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
	defer ch.Close()
	return nil
}
