package mq

type Provider func() (Broker, error)
