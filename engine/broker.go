package engine

import (
	"github.com/pkg/errors"
	"github.com/runabol/tork/conf"
	"github.com/runabol/tork/mq"
)

func (e *Engine) initBroker() error {
	var b mq.Broker
	bt := conf.StringDefault("broker.type", mq.BROKER_INMEMORY)

	b, err := mq.NewFromProvider(bt)
	if err != nil && !errors.Is(err, mq.ErrProviderNotFound) {
		return err
	}
	if b != nil {
		e.broker = b
		return nil
	}
	switch bt {
	case "inmemory":
		b = mq.NewInMemoryBroker()
	case "rabbitmq":
		rb, err := mq.NewRabbitMQBroker(conf.StringDefault("broker.rabbitmq.url", "amqp://guest:guest@localhost:5672/"))
		if err != nil {
			return errors.Wrapf(err, "unable to connect to RabbitMQ")
		}
		b = rb
	default:
		return errors.Errorf("invalid broker type: %s", bt)
	}
	e.broker = b
	return nil
}
