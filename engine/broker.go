package engine

import (
	"github.com/pkg/errors"
	"github.com/runabol/tork/conf"
	"github.com/runabol/tork/mq"
)

func (e *Engine) initBroker() error {
	bt := conf.StringDefault("broker.type", mq.BROKER_INMEMORY)
	broker, err := e.createBroker(bt)
	if err != nil {
		return err
	}
	e.broker = broker
	return nil
}

func (e *Engine) createBroker(btype string) (mq.Broker, error) {
	p, ok := e.mqProviders[btype]
	if ok {
		return p()
	}
	switch btype {
	case "inmemory":
		return mq.NewInMemoryBroker(), nil
	case "rabbitmq":
		rb, err := mq.NewRabbitMQBroker(conf.StringDefault("broker.rabbitmq.url", "amqp://guest:guest@localhost:5672/"))
		if err != nil {
			return nil, errors.Wrapf(err, "unable to connect to RabbitMQ")
		}
		return rb, nil
	default:
		return nil, errors.Errorf("invalid broker type: %s", btype)
	}
}
