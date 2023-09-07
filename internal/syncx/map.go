package syncx

import "sync"

type Map[K comparable, V any] struct {
	m sync.Map
}

func (m *Map[K, V]) Delete(key K) {
	m.m.Delete(key)
}

func (m *Map[K, V]) Get(key K) (value V, ok bool) {
	v, ok := m.m.Load(key)
	if !ok {
		return value, ok
	}
	return v.(V), ok
}

func (m *Map[K, V]) Set(key K, value V) {
	m.m.Store(key, value)
}

func (m *Map[K, V]) Iterate(f func(key K, value V)) {
	m.m.Range(func(key, value any) bool {
		f(key.(K), value.(V))
		return true
	})
}
