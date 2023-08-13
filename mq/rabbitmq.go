package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
	"github.com/tork/job"
	"github.com/tork/node"
	"github.com/tork/task"
)

type RabbitMQBroker struct {
	pconn  *amqp.Connection
	sconn  *amqp.Connection
	queues map[string]string
	qmu    sync.RWMutex
	url    string
}

func NewRabbitMQBroker(url string) (*RabbitMQBroker, error) {
	pconn, err := amqp.Dial(url)
	if err != nil {
		return nil, errors.Wrapf(err, "error dialing to RabbitMQ")
	}
	sconn, err := amqp.Dial(url)
	if err != nil {
		return nil, errors.Wrapf(err, "error dialing to RabbitMQ")
	}
	return &RabbitMQBroker{
		pconn:  pconn,
		sconn:  sconn,
		queues: make(map[string]string),
		url:    url,
	}, nil
}

func (b *RabbitMQBroker) Queues(ctx context.Context) ([]QueueInfo, error) {
	u, err := url.Parse(b.url)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to parse url: %s", b.url)
	}
	manager := fmt.Sprintf("http://%s:15672/api/queues/", u.Hostname())
	client := &http.Client{}
	req, err := http.NewRequest("GET", manager, nil)
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
		Name     string `json:"name"`
		Messages int    `json:"messages"`
	}

	rqs := make([]rabbitq, 0)
	err = json.NewDecoder(resp.Body).Decode(&rqs)
	if err != nil {
		return nil, errors.Wrapf(err, "error unmarshalling API response")
	}

	qis := make([]QueueInfo, len(rqs))
	for i, rq := range rqs {
		qis[i] = QueueInfo{
			Name: rq.Name,
			Size: rq.Messages,
		}
	}

	return qis, nil
}

func (b *RabbitMQBroker) PublishTask(ctx context.Context, qname string, t task.Task) error {
	return b.publish(ctx, qname, t)
}

func (b *RabbitMQBroker) SubscribeForTasks(qname string, handler func(t task.Task) error) error {
	return b.subscribe(qname, func(body []byte) error {
		t := task.Task{}
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
	log.Debug().Msgf("Subscribing for queue: %s", qname)
	ch, err := b.sconn.Channel()
	if err != nil {
		return errors.Wrapf(err, "error creating channel")
	}
	if err := b.declareQueue(qname, ch); err != nil {
		return errors.Wrapf(err, "error (re)declaring queue")
	}
	if err := ch.Qos(1, 0, false); err != nil {
		return errors.Wrapf(err, "error setting qos on channel")
	}
	msgs, err := ch.Consume(
		qname, // queue
		"",    // consumer name
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return errors.Wrapf(err, "unable to subscribe on q: %s", qname)
	}
	go func() {
		for d := range msgs {
			if err := handler(d.Body); err != nil {
				log.Error().
					Err(err).
					Str("queue", qname).
					Msg("failed to handle task")
				if err := d.Reject(false); err != nil {
					log.Error().
						Err(err).
						Msg("failed to reject task")
				}
			} else {
				if err := d.Ack(false); err != nil {
					log.Error().
						Err(err).
						Msg("failed to ack task")
				}
			}
		}
	}()
	return nil
}

func (b *RabbitMQBroker) declareQueue(qname string, ch *amqp.Channel) error {
	b.qmu.RLock()
	_, ok := b.queues[qname]
	b.qmu.RUnlock()
	if ok {
		return nil
	}
	b.qmu.Lock()
	defer b.qmu.Unlock()
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
	b.queues[qname] = qname
	return nil
}

func (b *RabbitMQBroker) PublishHeartbeat(ctx context.Context, n node.Node) error {
	return b.publish(ctx, QUEUE_HEARBEAT, n)
}

func (b *RabbitMQBroker) publish(ctx context.Context, qname string, msg any) error {
	ch, err := b.pconn.Channel()
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

func (b *RabbitMQBroker) PublishJob(ctx context.Context, j job.Job) error {
	return b.publish(ctx, QUEUE_JOBS, j)
}
func (b *RabbitMQBroker) SubscribeForJobs(handler func(j job.Job) error) error {
	return b.subscribe(QUEUE_JOBS, func(body []byte) error {
		j := job.Job{}
		if err := json.Unmarshal(body, &j); err != nil {
			log.Error().
				Err(err).
				Str("body", string(body)).
				Msg("unable to deserialize job")
		}
		return handler(j)
	})
}
