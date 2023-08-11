package mq

import (
	"context"
	"encoding/json"
	"strings"
	"sync"

	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
	"github.com/tork/node"
	"github.com/tork/task"
)

type RabbitMQBroker struct {
	pconn  *amqp.Connection
	sconn  *amqp.Connection
	queues map[string]string
	qmu    sync.RWMutex
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
	}, nil
}

func (b *RabbitMQBroker) Queues(ctx context.Context) ([]QueueInfo, error) {
	return make([]QueueInfo, 0), nil
}
func (b *RabbitMQBroker) PublishTask(ctx context.Context, qname string, t task.Task) error {
	return b.publish(ctx, qname, t)
}

func (b *RabbitMQBroker) SubscribeForTasks(qname string, handler func(ctx context.Context, t task.Task) error) error {
	return b.subscribe(qname, func(ctx context.Context, body []byte) error {
		t := task.Task{}
		if err := json.Unmarshal(body, &t); err != nil {
			log.Error().
				Err(err).
				Str("body", string(body)).
				Msg("unable to deserialize task")
		}
		return handler(ctx, t)
	})
}

func (b *RabbitMQBroker) subscribe(qname string, handler func(ctx context.Context, body []byte) error) error {
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
			ctx := context.TODO()
			if err := handler(ctx, d.Body); err != nil {
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

func (b *RabbitMQBroker) SubscribeForHeartbeats(handler func(ctx context.Context, n node.Node) error) error {
	return b.subscribe(QUEUE_HEARBEAT, func(ctx context.Context, body []byte) error {
		n := node.Node{}
		if err := json.Unmarshal(body, &n); err != nil {
			log.Error().
				Err(err).
				Str("body", string(body)).
				Msg("unable to deserialize node")
		}
		return handler(ctx, n)
	})
}
