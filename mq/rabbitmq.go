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
	"github.com/runabol/tork/job"
	"github.com/runabol/tork/node"
	"github.com/runabol/tork/task"
	"github.com/runabol/tork/uuid"
)

type RabbitMQBroker struct {
	connPool      []*amqp.Connection
	nextConn      int
	queues        map[string]string
	subscriptions []*subscription
	mu            sync.RWMutex
	url           string
	shuttingDown  bool
}

type subscription struct {
	qname string
	ch    *amqp.Channel
	name  string
	done  chan int
}

func NewRabbitMQBroker(url string) (*RabbitMQBroker, error) {
	connPool := make([]*amqp.Connection, 3)
	for i := 0; i < len(connPool); i++ {
		conn, err := amqp.Dial(url)
		if err != nil {
			return nil, errors.Wrapf(err, "error dialing to RabbitMQ")
		}
		connPool[i] = conn
	}
	return &RabbitMQBroker{
		queues:        make(map[string]string),
		url:           url,
		connPool:      connPool,
		subscriptions: make([]*subscription, 0),
	}, nil
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

func (b *RabbitMQBroker) PublishTask(ctx context.Context, qname string, t *task.Task) error {
	return b.publish(ctx, qname, t)
}

func (b *RabbitMQBroker) isShuttingDown() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.shuttingDown
}

func (b *RabbitMQBroker) SubscribeForTasks(qname string, handler func(t *task.Task) error) error {
	return b.subscribe(qname, func(body []byte) error {
		t := &task.Task{}
		if err := json.Unmarshal(body, &t); err != nil {
			log.Error().
				Err(err).
				Str("body", string(body)).
				Msg("unable to deserialize task")
		}
		return handler(t)
	})
}

func (b *RabbitMQBroker) subscribe(qname string, handler func(body []byte) error) error {
	if b.isShuttingDown() {
		return errors.New("broker is shutting down")
	}
	conn, err := b.getConnection()
	if err != nil {
		return errors.Wrapf(err, "error getting a connection")
	}
	ch, err := conn.Channel()
	if err != nil {
		return errors.Wrapf(err, "error creating channel")
	}
	if err := b.declareQueue(qname, ch); err != nil {
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
			if err := handler(d.Body); err != nil {
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
		if !b.shuttingDown {
			maxAttempts := 20
			for attempt := 1; attempt <= maxAttempts; attempt++ {
				log.Info().Msgf("%s channel closed. reconnecting", qname)
				if err := b.subscribe(qname, handler); err != nil {
					log.Error().
						Err(err).
						Msgf("error reconnecting to %s (attempt %d/%d)", qname, attempt, maxAttempts)
					time.Sleep(time.Second * time.Duration(attempt))
				} else {
					return
				}
			}
			log.Error().Err(err).Msgf("error reconnecting to %s. giving up", qname)
		}
		sub.done <- 1
	}()
	return nil
}

func (b *RabbitMQBroker) declareQueue(qname string, ch *amqp.Channel) error {
	b.mu.RLock()
	_, ok := b.queues[qname]
	b.mu.RUnlock()
	if ok {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
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
		_, err := ch.QueueDeclare(
			qname,
			false, // durable
			false, // delete when unused
			strings.HasPrefix(qname, QUEUE_EXCLUSIVE_PREFIX), // exclusive
			false, // no-wait
			nil,   // arguments
		)
		if err != nil {
			return err
		}
	}
	b.queues[qname] = qname
	return nil
}

func (b *RabbitMQBroker) PublishHeartbeat(ctx context.Context, n node.Node) error {
	return b.publish(ctx, QUEUE_HEARBEAT, n)
}

func (b *RabbitMQBroker) publish(ctx context.Context, qname string, msg any) error {
	if b.isShuttingDown() {
		return errors.New("broker is shutting down")
	}
	conn, err := b.getConnection()
	if err != nil {
		return errors.Wrapf(err, "error getting a connection")
	}
	ch, err := conn.Channel()
	if err != nil {
		return errors.Wrapf(err, "error creating channel")
	}
	defer ch.Close()
	if err := b.declareQueue(qname, ch); err != nil {
		return errors.Wrapf(err, "error (re)declaring queue")
	}
	body, err := json.Marshal(msg)
	if err != nil {
		return errors.Wrapf(err, "unable to serialize the message")
	}
	err = ch.PublishWithContext(ctx,
		"",    // exchange
		qname, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(body),
		})
	if err != nil {
		return errors.Wrapf(err, "unable to publish message")
	}
	return nil
}

func (b *RabbitMQBroker) SubscribeForHeartbeats(handler func(n node.Node) error) error {
	return b.subscribe(QUEUE_HEARBEAT, func(body []byte) error {
		n := node.Node{}
		if err := json.Unmarshal(body, &n); err != nil {
			log.Error().
				Err(err).
				Str("body", string(body)).
				Msg("unable to deserialize node")
		}
		return handler(n)
	})
}

func (b *RabbitMQBroker) PublishJob(ctx context.Context, j *job.Job) error {
	return b.publish(ctx, QUEUE_JOBS, j)
}
func (b *RabbitMQBroker) SubscribeForJobs(handler func(j *job.Job) error) error {
	return b.subscribe(QUEUE_JOBS, func(body []byte) error {
		j := &job.Job{}
		if err := json.Unmarshal(body, &j); err != nil {
			log.Error().
				Err(err).
				Str("body", string(body)).
				Msg("unable to deserialize job")
		}
		return handler(j)
	})
}

func (b *RabbitMQBroker) getConnection() (*amqp.Connection, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var err error
	var conn *amqp.Connection
	conn = b.connPool[b.nextConn]
	if conn.IsClosed() {
		// clearing the known queues because rabbitmq
		// might have crashed and we would need to
		// re-declare them
		b.queues = make(map[string]string)
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

func (b *RabbitMQBroker) Shutdown(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.shuttingDown = true
	// close channels cleanly
	for _, sub := range b.subscriptions {
		log.Info().
			Msgf("shutting down subscription %s for %s", sub.name, sub.qname)
		if err := sub.ch.Cancel(sub.name, false); err != nil {
			log.Error().
				Err(err).
				Msgf("error closing channel for %s", sub.qname)
		}
		select {
		case <-ctx.Done():
		case <-sub.done:
		// let's give up to 5 seconds of grace
		// period for the subscriber to release
		// the channel cleanly
		case <-time.After(5 * time.Second):
		}
	}
	b.subscriptions = []*subscription{}
	// terminate connections cleanly
	for _, conn := range b.connPool {
		log.Info().
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
		case <-time.After(5 * time.Second):
		}
	}
	b.connPool = []*amqp.Connection{}
	return nil
}
