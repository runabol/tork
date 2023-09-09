package datastore

import (
	"sync"

	"github.com/pkg/errors"
)

type Provider func() (Datastore, error)

var (
	providers           = map[string]Provider{}
	providersMu         = sync.RWMutex{}
	ErrProviderNotFound = errors.Errorf("datastore provider not found")
)

func NewFromProvider(name string) (Datastore, error) {
	providersMu.RLock()
	defer providersMu.RUnlock()
	if p, ok := providers[name]; ok {
		return p()
	}
	return nil, ErrProviderNotFound
}

func RegisterProvider(name string, provider Provider) {
	providersMu.Lock()
	defer providersMu.Unlock()
	if _, ok := providers[name]; ok {
		panic("datastore: Register called twice for driver " + name)
	}
	providers[name] = provider
}
