package engine

import (
	"context"

	"github.com/pkg/errors"
	"github.com/runabol/tork"
	"github.com/runabol/tork/broker"
	"github.com/runabol/tork/conf"
)

type brokerProxy struct {
	broker broker.Broker
}

func (b *brokerProxy) PublishTask(ctx context.Context, qname string, t *tork.Task) error {
	if err := b.checkInit(); err != nil {
		return err
	}
	return b.broker.PublishTask(ctx, qname, t)
}

func (b *brokerProxy) SubscribeForTasks(qname string, handler func(t *tork.Task) error) error {
	if err := b.checkInit(); err != nil {
		return err
	}
	return b.broker.SubscribeForTasks(qname, handler)
}

func (b *brokerProxy) PublishTaskProgress(ctx context.Context, t *tork.Task) error {
	if err := b.checkInit(); err != nil {
		return err
	}
	return b.broker.PublishTaskProgress(ctx, t)
}

func (b *brokerProxy) SubscribeForTaskProgress(handler func(t *tork.Task) error) error {
	if err := b.checkInit(); err != nil {
		return err
	}
	return b.broker.SubscribeForTaskProgress(handler)
}

func (b *brokerProxy) PublishHeartbeat(ctx context.Context, n *tork.Node) error {
	if err := b.checkInit(); err != nil {
		return err
	}
	return b.broker.PublishHeartbeat(ctx, n)
}

func (b *brokerProxy) SubscribeForHeartbeats(handler func(n *tork.Node) error) error {
	if err := b.checkInit(); err != nil {
		return err
	}
	return b.broker.SubscribeForHeartbeats(handler)
}

func (b *brokerProxy) PublishJob(ctx context.Context, j *tork.Job) error {
	if err := b.checkInit(); err != nil {
		return err
	}
	return b.broker.PublishJob(ctx, j)
}

func (b *brokerProxy) SubscribeForJobs(handler func(j *tork.Job) error) error {
	if err := b.checkInit(); err != nil {
		return err
	}
	return b.broker.SubscribeForJobs(handler)
}

func (b *brokerProxy) PublishEvent(ctx context.Context, topic string, event interface{}) error {
	if err := b.checkInit(); err != nil {
		return err
	}
	return b.broker.PublishEvent(ctx, topic, event)
}

func (b *brokerProxy) SubscribeForEvents(ctx context.Context, pattern string, handler func(event interface{})) error {
	if err := b.checkInit(); err != nil {
		return err
	}
	return b.broker.SubscribeForEvents(ctx, pattern, handler)
}

func (b *brokerProxy) PublishTaskLogPart(ctx context.Context, p *tork.TaskLogPart) error {
	if err := b.checkInit(); err != nil {
		return err
	}
	return b.broker.PublishTaskLogPart(ctx, p)
}

func (b *brokerProxy) SubscribeForTaskLogPart(handler func(p *tork.TaskLogPart)) error {
	if err := b.checkInit(); err != nil {
		return err
	}
	return b.broker.SubscribeForTaskLogPart(handler)
}

func (b *brokerProxy) Queues(ctx context.Context) ([]broker.QueueInfo, error) {
	if err := b.checkInit(); err != nil {
		return nil, err
	}
	return b.broker.Queues(ctx)
}

func (b *brokerProxy) HealthCheck(ctx context.Context) error {
	if err := b.checkInit(); err != nil {
		return err
	}
	return b.broker.HealthCheck(ctx)
}

func (b *brokerProxy) Shutdown(ctx context.Context) error {
	if err := b.checkInit(); err != nil {
		return err
	}
	return b.broker.Shutdown(ctx)
}

func (b *brokerProxy) checkInit() error {
	if b.broker == nil {
		return errors.New("Broker not initialized. You must call engine.Start() first")
	}
	return nil
}

func (e *Engine) initBroker() error {
	bt := conf.StringDefault("broker.type", broker.BROKER_INMEMORY)
	broker, err := e.createBroker(bt)
	if err != nil {
		return err
	}
	e.brokerRef.broker = broker
	return nil
}

func (e *Engine) createBroker(btype string) (broker.Broker, error) {
	p, ok := e.mqProviders[btype]
	if ok {
		return p()
	}
	switch btype {
	case "inmemory":
		return broker.NewInMemoryBroker(), nil
	case "rabbitmq":
		rb, err := broker.NewRabbitMQBroker(
			conf.StringDefault("broker.rabbitmq.url", "amqp://guest:guest@localhost:5672/"),
			broker.WithConsumerTimeoutMS(conf.DurationDefault("broker.rabbitmq.consumer.timeout", broker.RABBITMQ_DEFAULT_CONSUMER_TIMEOUT)),
			broker.WithManagementURL(conf.String("broker.rabbitmq.management.url")),
			broker.WithDurableQueues(conf.Bool("broker.rabbitmq.durable.queues")),
		)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to connect to RabbitMQ")
		}
		return rb, nil
	default:
		return nil, errors.Errorf("invalid broker type: %s", btype)
	}
}
