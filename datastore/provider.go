package datastore

type Provider func() (Datastore, error)
