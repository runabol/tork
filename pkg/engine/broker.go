package engine

import (
	"github.com/pkg/errors"
	"github.com/runabol/tork/datastore"
	"github.com/runabol/tork/mq"
	"github.com/runabol/tork/pkg/conf"
)

func createDatastore() (datastore.Datastore, error) {
	dstype := conf.StringDefault("datastore.type", datastore.DATASTORE_INMEMORY)
	var ds datastore.Datastore
	ds, err := datastore.NewFromProvider(dstype)
	if err != nil && !errors.Is(err, datastore.ErrProviderNotFound) {
		return nil, err
	}
	if ds != nil {
		return ds, nil
	}
	switch dstype {
	case datastore.DATASTORE_INMEMORY:
		ds = datastore.NewInMemoryDatastore()
	case datastore.DATASTORE_POSTGRES:
		dsn := conf.StringDefault(
			"datastore.postgres.dsn",
			"host=localhost user=tork password=tork dbname=tork port=5432 sslmode=disable",
		)
		pg, err := datastore.NewPostgresDataStore(dsn)
		if err != nil {
			return nil, err
		}
		ds = pg
	default:
		return nil, errors.Errorf("unknown datastore type: %s", dstype)
	}
	return ds, nil
}

func createBroker() (mq.Broker, error) {
	var b mq.Broker
	bt := conf.StringDefault("broker.type", mq.BROKER_INMEMORY)

	b, err := mq.NewFromProvider(bt)
	if err != nil && !errors.Is(err, mq.ErrProviderNotFound) {
		return nil, err
	}
	if b != nil {
		return b, nil
	}
	switch bt {
	case "inmemory":
		b = mq.NewInMemoryBroker()
	case "rabbitmq":
		rb, err := mq.NewRabbitMQBroker(conf.StringDefault("broker.rabbitmq.url", "amqp://guest:guest@localhost:5672/"))
		if err != nil {
			return nil, errors.Wrapf(err, "unable to connect to RabbitMQ")
		}
		b = rb
	default:
		return nil, errors.Errorf("invalid broker type: %s", bt)
	}
	return b, nil
}
