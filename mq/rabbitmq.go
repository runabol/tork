package mq

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
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
func (b *RabbitMQBroker) Publish(ctx context.Context, qname string, t *task.Task) error {
	log.Debug().Msgf("publish task %s to %s queue", t.ID, qname)
	ch, err := b.pconn.Channel()
	if err != nil {
		return errors.Wrapf(err, "error creating channel")
	}
	defer ch.Close()
	if err := b.declareQueue(qname, ch); err != nil {
		return errors.Wrapf(err, "error (re)declaring queue")
	}
	body, err := json.Marshal(t)
	if err != nil {
		return errors.Wrapf(err, "unable to serialize the task")
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
		return errors.Wrapf(err, "unable to publish task")
	}
	return nil
}

func (b *RabbitMQBroker) Subscribe(qname string, handler func(ctx context.Context, t *task.Task) error) error {
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
			t := task.Task{}
			if err := json.Unmarshal(d.Body, &t); err != nil {
				log.Error().
					Err(err).
					Str("body", string(d.Body)).
					Msg("unable to deserialize task")
			}
			ctx := context.TODO()
			if err := handler(ctx, &t); err != nil {
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
	log.Debug().Msgf("attempting to (re)delcare queue: %s", qname)
	_, err := ch.QueueDeclare(
		qname,
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return err
	}
	b.queues[qname] = qname
	return nil
}
