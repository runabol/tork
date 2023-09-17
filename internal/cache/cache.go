// Modified version of https://github.com/patrickmn/go-cache

package cache

import (
	"sync"
	"time"

	"github.com/pkg/errors"
)

type Item[V any] struct {
	Object     V
	Expiration int64
	mu         sync.Mutex
}

const (
	// For use with functions that take an expiration time.
	NoExpiration time.Duration = -1
	// For use with functions that take an expiration time. Equivalent to
	// passing in the same expiration duration as was given to New() or
	// NewFrom() when the cache was created (e.g. 5 minutes.)
	DefaultExpiration time.Duration = 0
)

type Cache[V any] struct {
	defaultExpiration time.Duration
	items             map[string]*Item[V]
	mu                sync.RWMutex
	onEvicted         func(string, V)
	janitor           *janitor[V]
}

// Add an item to the cache, replacing any existing item. If the duration is 0
// (DefaultExpiration), the cache's default expiration time is used. If it is -1
// (NoExpiration), the item never expires.
func (c *Cache[V]) SetWithExpiration(k string, x V, d time.Duration) {
	// "Inlining" of set
	var e int64
	if d == DefaultExpiration {
		d = c.defaultExpiration
	}
	if d > 0 {
		e = time.Now().Add(d).UnixNano()
	}
	c.mu.Lock()
	c.items[k] = &Item[V]{
		Object:     x,
		Expiration: e,
	}
	// TODO: Calls to mu.Unlock are currently not deferred because defer
	// adds ~200 ns (as of go1.)
	c.mu.Unlock()
}

func (c *Cache[V]) SetExpiration(k string, d time.Duration) error {
	var e int64
	if d == DefaultExpiration {
		d = c.defaultExpiration
	}
	if d > 0 {
		e = time.Now().Add(d).UnixNano()
	}
	c.mu.RLock()
	item, found := c.items[k]
	if !found {
		c.mu.RUnlock()
		return errors.Errorf("unknown key: %s", k)
	}
	c.mu.RUnlock()
	item.mu.Lock()
	item.Expiration = e
	// TODO: Calls to mu.Unlock are currently not deferred because defer
	// adds ~200 ns (as of go1.)
	item.mu.Unlock()
	return nil

}

func (c *Cache[V]) set(k string, x V, d time.Duration) {
	var e int64
	if d == DefaultExpiration {
		d = c.defaultExpiration
	}
	if d > 0 {
		e = time.Now().Add(d).UnixNano()
	}
	c.mu.Lock()
	c.items[k] = &Item[V]{
		Object:     x,
		Expiration: e,
	}
	c.mu.Unlock()
}

// Add an item to the cache, replacing any existing item, using the default
// expiration.
func (c *Cache[V]) Set(k string, x V) {
	c.set(k, x, DefaultExpiration)
}

func (c *Cache[V]) Modify(k string, m func(x V) (V, error)) error {
	c.mu.RLock()
	// "Inlining" of get and Expired
	item, found := c.items[k]
	if !found {
		c.mu.RUnlock()
		return errors.Errorf("unknown key: %s", k)
	}
	c.mu.RUnlock()
	item.mu.Lock()
	v := item.Object
	v, err := m(v)
	if err != nil {
		return err
	}
	item.Object = v
	item.mu.Unlock()
	return nil
}

// Get an item from the cache. Returns the item or nil, and a bool indicating
// whether the key was found.
func (c *Cache[V]) Get(k string) (val V, ok bool) {
	c.mu.RLock()
	// "Inlining" of get and Expired
	item, found := c.items[k]
	if !found {
		c.mu.RUnlock()
		return val, false
	}
	c.mu.RUnlock()
	item.mu.Lock()
	defer item.mu.Unlock()
	if item.Expiration > 0 {
		if time.Now().UnixNano() > item.Expiration {
			return val, false
		}
	}
	return item.Object, true
}

// Delete an item from the cache. Does nothing if the key is not in the cache.
func (c *Cache[V]) Delete(k string) {
	c.mu.Lock()
	v, evicted := c.delete(k)
	c.mu.Unlock()
	if evicted {
		c.onEvicted(k, v)
	}
}

func (c *Cache[V]) delete(k string) (val V, ok bool) {
	if c.onEvicted != nil {
		if v, found := c.items[k]; found {
			delete(c.items, k)
			return v.Object, true
		}
	}
	delete(c.items, k)
	return val, false
}

type keyAndValue[V any] struct {
	key   string
	value V
}

// Delete all expired items from the cache.
func (c *Cache[V]) deleteExpired() {
	var evictedItems []keyAndValue[V]
	now := time.Now().UnixNano()
	c.mu.Lock()
	for k, v := range c.items {
		// "Inlining" of expired
		v.mu.Lock()
		if v.Expiration > 0 && now > v.Expiration {
			ov, evicted := c.delete(k)
			if evicted {
				evictedItems = append(evictedItems, keyAndValue[V]{k, ov})
			}
		}
		v.mu.Unlock()
	}
	c.mu.Unlock()
	for _, v := range evictedItems {
		c.onEvicted(v.key, v.value)
	}
}

// Sets an (optional) function that is called with the key and value when an
// item is evicted from the cache. (Including when it is deleted manually, but
// not when it is overwritten.) Set to nil to disable.
func (c *Cache[V]) OnEvicted(f func(string, V)) {
	c.mu.Lock()
	c.onEvicted = f
	c.mu.Unlock()
}

// Copies all unexpired items in the cache into a new map and returns it.
func (c *Cache[V]) allItems() map[string]*Item[V] {
	c.mu.RLock()
	defer c.mu.RUnlock()
	m := make(map[string]*Item[V], len(c.items))
	now := time.Now().UnixNano()
	for k, v := range c.items {
		// "Inlining" of Expired
		if v.Expiration > 0 {
			if now > v.Expiration {
				continue
			}
		}
		m[k] = v
	}
	return m
}

func (c *Cache[V]) Iterate(it func(key string, v V)) {
	items := c.allItems()
	for k, v := range items {
		v.mu.Lock()
		it(k, v.Object)
		v.mu.Unlock()
	}
}

// Returns the number of items in the cache. This may include items that have
// expired, but have not yet been cleaned up.
func (c *Cache[V]) ItemCount() int {
	c.mu.RLock()
	n := len(c.items)
	c.mu.RUnlock()
	return n
}

// Delete all items from the cache.
func (c *Cache[V]) Flush() {
	c.mu.Lock()
	c.items = map[string]*Item[V]{}
	c.mu.Unlock()
}

type janitor[V any] struct {
	Interval time.Duration
	stop     chan bool
}

func (j *janitor[V]) Run(c *Cache[V]) {
	ticker := time.NewTicker(j.Interval)
	for {
		select {
		case <-ticker.C:
			c.deleteExpired()
		case <-j.stop:
			ticker.Stop()
			return
		}
	}
}

func (c *Cache[V]) Close() {
	stopJanitor(c)
}

func stopJanitor[V any](c *Cache[V]) {
	c.janitor.stop <- true
}

func runJanitor[V any](c *Cache[V], ci time.Duration) {
	j := &janitor[V]{
		Interval: ci,
		stop:     make(chan bool),
	}
	c.janitor = j
	go j.Run(c)
}

func newCache[V any](de time.Duration, m map[string]*Item[V]) *Cache[V] {
	if de == 0 {
		de = -1
	}
	c := &Cache[V]{
		defaultExpiration: de,
		items:             m,
	}
	return c
}

func newCacheWithJanitor[V any](de time.Duration, ci time.Duration, m map[string]*Item[V]) *Cache[V] {
	c := newCache[V](de, m)
	if ci > 0 {
		runJanitor(c, ci)
	}
	return c
}

// Return a new cache with a given default expiration duration and cleanup
// interval. If the expiration duration is less than one (or NoExpiration),
// the items in the cache never expire (by default), and must be deleted
// manually. If the cleanup interval is less than one, expired items are not
// deleted from the cache before calling c.DeleteExpired().
func New[V any](defaultExpiration, cleanupInterval time.Duration) *Cache[V] {
	items := make(map[string]*Item[V])
	return newCacheWithJanitor[V](defaultExpiration, cleanupInterval, items)
}
