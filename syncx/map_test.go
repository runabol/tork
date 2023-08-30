package syncx_test

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/runabol/tork/syncx"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/slices"
)

func TestGetNonExistent(t *testing.T) {
	m := syncx.Map[string, int]{}
	v, ok := m.Get("nothing")
	assert.False(t, ok)
	assert.Equal(t, 0, v)
}

func TestSetAndGet(t *testing.T) {
	m := syncx.Map[string, int]{}
	m.Set("somekey", 100)
	v, ok := m.Get("somekey")
	assert.True(t, ok)
	assert.Equal(t, 100, v)
}

func TestSetAndDelete(t *testing.T) {
	m := syncx.Map[string, int]{}
	m.Set("somekey", 100)
	v, ok := m.Get("somekey")
	assert.True(t, ok)
	assert.Equal(t, 100, v)
	m.Delete("somekey")
	v, ok = m.Get("somekey")
	assert.False(t, ok)
	assert.Equal(t, 0, v)
}

func TestConcurrentSetAndGet(t *testing.T) {
	m := syncx.Map[string, int]{}
	wg := sync.WaitGroup{}
	wg.Add(1000)
	for i := 1; i <= 1000; i++ {
		go func(ix int) {
			defer wg.Done()
			// introduce some arbitrary latency
			time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)+1))
			m.Set("somekey", ix)
			v, ok := m.Get("somekey")
			assert.True(t, ok)
			assert.Greater(t, v, 0)
		}(i)
	}
	wg.Wait()
}

func TestIterate(t *testing.T) {
	m := syncx.Map[string, int]{}
	m.Set("k1", 100)
	m.Set("k2", 200)
	vals := make([]int, 0)
	keys := make([]string, 0)
	m.Iterate(func(k string, v int) {
		vals = append(vals, v)
		keys = append(keys, k)
	})
	slices.Sort(vals)
	slices.Sort(keys)
	assert.Equal(t, []int{100, 200}, vals)
	assert.Equal(t, []string{"k1", "k2"}, keys)
}

func BenchmarkSetAndGet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		m := syncx.Map[string, int]{}
		m.Set("somekey", 100)
		v, ok := m.Get("somekey")
		assert.True(b, ok)
		assert.Equal(b, 100, v)
	}
}
