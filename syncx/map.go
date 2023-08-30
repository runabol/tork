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

func (m *Map[K, V]) Values() []V {
	vals := make([]V, 0)
	m.m.Range(func(key, value any) bool {
		vals = append(vals, value.(V))
		return true
	})
	return vals
}
