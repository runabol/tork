package cache

import (
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/runabol/tork"
	"github.com/stretchr/testify/assert"
)

type TestStruct struct {
	Num      int
	Children []*TestStruct
}

func TestCache(t *testing.T) {
	tc := New[any](DefaultExpiration, 0)

	a, found := tc.Get("a")
	if found || a != nil {
		t.Error("Getting A found value that shouldn't exist:", a)
	}

	b, found := tc.Get("b")
	if found || b != nil {
		t.Error("Getting B found value that shouldn't exist:", b)
	}

	c, found := tc.Get("c")
	if found || c != nil {
		t.Error("Getting C found value that shouldn't exist:", c)
	}

	tc.Set("a", 1)
	tc.Set("b", "b")
	tc.Set("c", 3.5)

	x, found := tc.Get("a")
	if !found {
		t.Error("a was not found while getting a2")
	}
	if x == nil {
		t.Error("x for a is nil")
	} else if a2 := x.(int); a2+2 != 3 {
		t.Error("a2 (which should be 1) plus 2 does not equal 3; value:", a2)
	}

	x, found = tc.Get("b")
	if !found {
		t.Error("b was not found while getting b2")
	}
	if x == nil {
		t.Error("x for b is nil")
	} else if b2 := x.(string); b2+"B" != "bB" {
		t.Error("b2 (which should be b) plus B does not equal bB; value:", b2)
	}

	x, found = tc.Get("c")
	if !found {
		t.Error("c was not found while getting c2")
	}
	if x == nil {
		t.Error("x for c is nil")
	} else if c2 := x.(float64); c2+1.2 != 4.7 {
		t.Error("c2 (which should be 3.5) plus 1.2 does not equal 4.7; value:", c2)
	}
}

func TestCacheTimes(t *testing.T) {
	var found bool

	tc := New[any](50*time.Millisecond, 1*time.Millisecond)
	tc.SetWithExpiration("a", 1, DefaultExpiration)
	tc.SetWithExpiration("b", 2, NoExpiration)
	tc.SetWithExpiration("c", 3, 20*time.Millisecond)
	tc.SetWithExpiration("d", 4, 70*time.Millisecond)

	<-time.After(25 * time.Millisecond)
	_, found = tc.Get("c")
	if found {
		t.Error("Found c when it should have been automatically deleted")
	}

	<-time.After(30 * time.Millisecond)
	_, found = tc.Get("a")
	if found {
		t.Error("Found a when it should have been automatically deleted")
	}

	_, found = tc.Get("b")
	if !found {
		t.Error("Did not find b even though it was set to never expire")
	}

	_, found = tc.Get("d")
	if !found {
		t.Error("Did not find d even though it was set to expire later than the default")
	}

	<-time.After(20 * time.Millisecond)
	_, found = tc.Get("d")
	if found {
		t.Error("Found d when it should have been automatically deleted (later than the default)")
	}
}

func TestStorePointerToStruct(t *testing.T) {
	tc := New[any](DefaultExpiration, 0)
	tc.SetWithExpiration("foo", &TestStruct{Num: 1}, DefaultExpiration)
	x, found := tc.Get("foo")
	if !found {
		t.Fatal("*TestStruct was not found for foo")
	}
	foo := x.(*TestStruct)
	foo.Num++

	y, found := tc.Get("foo")
	if !found {
		t.Fatal("*TestStruct was not found for foo (second time)")
	}
	bar := y.(*TestStruct)
	if bar.Num != 2 {
		t.Fatal("TestStruct.Num is not 2")
	}
}

func TestDelete(t *testing.T) {
	tc := New[any](DefaultExpiration, 0)
	tc.SetWithExpiration("foo", "bar", DefaultExpiration)
	tc.Delete("foo")
	x, found := tc.Get("foo")
	if found {
		t.Error("foo was found, but it should have been deleted")
	}
	if x != nil {
		t.Error("x is not nil:", x)
	}
}

func TestItemCount(t *testing.T) {
	tc := New[any](DefaultExpiration, 0)
	tc.SetWithExpiration("foo", "1", DefaultExpiration)
	tc.SetWithExpiration("bar", "2", DefaultExpiration)
	tc.SetWithExpiration("baz", "3", DefaultExpiration)
	if n := tc.ItemCount(); n != 3 {
		t.Errorf("Item count is not 3: %d", n)
	}
}

func TestIterate(t *testing.T) {
	tc := New[*tork.Task](DefaultExpiration, 0)

	for i := 0; i < 1000; i++ {
		tc.SetWithExpiration(fmt.Sprintf("foo%d", i), &tork.Task{Name: "some task"}, DefaultExpiration)
	}

	r := sync.WaitGroup{}
	r.Add(100)

	for i := 0; i < 100; i++ {
		go func() {
			defer r.Done()
			tc.Iterate(func(key string, v *tork.Task) {
				_ = v.Name
			})
		}()
	}

	w := sync.WaitGroup{}
	w.Add(100)
	for i := 0; i < 100; i++ {
		go func() {
			defer w.Done()
			for i := 0; i < 1000; i++ {
				err := tc.Modify(fmt.Sprintf("foo%d", i), func(x *tork.Task) (*tork.Task, error) {
					x.Clone()
					x.Env = map[string]string{
						"SOME_VAR": "someval",
					}
					return x, nil
				})
				assert.NoError(t, err)
			}
		}()
	}

	r.Wait()
	w.Wait()
}

func TestFlush(t *testing.T) {
	tc := New[any](DefaultExpiration, 0)
	tc.SetWithExpiration("foo", "bar", DefaultExpiration)
	tc.SetWithExpiration("baz", "yes", DefaultExpiration)
	tc.Flush()
	x, found := tc.Get("foo")
	if found {
		t.Error("foo was found, but it should have been deleted")
	}
	if x != nil {
		t.Error("x is not nil:", x)
	}
	x, found = tc.Get("baz")
	if found {
		t.Error("baz was found, but it should have been deleted")
	}
	if x != nil {
		t.Error("x is not nil:", x)
	}
}

func TestOnEvicted(t *testing.T) {
	tc := New[any](DefaultExpiration, 0)
	tc.SetWithExpiration("foo", 3, DefaultExpiration)
	if tc.onEvicted != nil {
		t.Fatal("tc.onEvicted is not nil")
	}
	works := false
	tc.OnEvicted(func(k string, v interface{}) {
		if k == "foo" && v.(int) == 3 {
			works = true
		}
		tc.SetWithExpiration("bar", 4, DefaultExpiration)
	})
	tc.Delete("foo")
	x, _ := tc.Get("bar")
	if !works {
		t.Error("works bool not true")
	}
	if x.(int) != 4 {
		t.Error("bar was not 4")
	}
}

func TestModify(t *testing.T) {
	tc := New[int](DefaultExpiration, 0)
	tc.Set("number", 0)
	for i := 0; i < 10; i++ {
		err := tc.Modify("number", func(x int) (int, error) {
			return x + 1, nil
		})
		assert.NoError(t, err)
	}
	v, ok := tc.Get("number")
	assert.True(t, ok)
	assert.Equal(t, 10, v)

	err := tc.Modify("number", func(x int) (int, error) {
		return 0, errors.New("something bad happened")
	})
	assert.Error(t, err)
	err = tc.Modify("number", func(x int) (int, error) {
		return 17, nil
	})
	assert.NoError(t, err)
}

func TestModifyObjectConcurrently(t *testing.T) {
	tc := New[*tork.Job](DefaultExpiration, 0)
	tc.Set("job", &tork.Job{
		Context: tork.JobContext{
			Tasks: map[string]string{},
		},
	})

	w := sync.WaitGroup{}
	w.Add(1000)
	for i := 0; i < 1000; i++ {
		go func() {
			defer w.Done()
			err := tc.Modify("job", func(x *tork.Job) (*tork.Job, error) {
				x = x.Clone()
				time.Sleep(time.Duration(rand.Intn(1000)) * time.Microsecond)
				x.TaskCount = x.TaskCount + 1
				x.Context.Tasks[fmt.Sprintf("someVar-%d", rand.Intn(100000))] = "some value"
				return x, nil
			})

			assert.NoError(t, err)
		}()
	}

	r := sync.WaitGroup{}
	r.Add(1000)
	for i := 0; i < 1000; i++ {
		go func() {
			defer r.Done()
			time.Sleep(time.Duration(rand.Intn(1000)) * time.Microsecond)
			j2, ok := tc.Get("job")
			assert.True(t, ok)
			_ = j2.Clone()
		}()
	}

	r.Wait()
	w.Wait()

	j, ok := tc.Get("job")
	assert.True(t, ok)

	assert.Equal(t, 1000, j.TaskCount)

}

func TestModifyExpiration(t *testing.T) {
	tc := New[int](DefaultExpiration, 0)
	tc.Set("number", 0)

	wg := sync.WaitGroup{}
	wg.Add(10)

	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()
			err := tc.SetExpiration("number", NoExpiration)
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	v, ok := tc.Get("number")
	assert.True(t, ok)
	assert.Equal(t, 0, v)

	assert.Equal(t, int64(0), tc.items["number"].Expiration)
}

func TestModifyConcurrently(t *testing.T) {
	tc := New[int](DefaultExpiration, 0)
	tc.Set("number", 0)
	wg := sync.WaitGroup{}
	wg.Add(1000)
	for i := 0; i < 1000; i++ {
		go func() {
			defer wg.Done()
			err := tc.Modify("number", func(x int) (int, error) {
				time.Sleep(time.Duration(rand.Intn(1000)) * time.Microsecond)
				return x + 1, nil
			})
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
	v, ok := tc.Get("number")
	assert.True(t, ok)
	assert.Equal(t, 1000, v)
}

func BenchmarkCacheGetExpiring(b *testing.B) {
	benchmarkCacheGet(b, 5*time.Minute)
}

func BenchmarkCacheGetNotExpiring(b *testing.B) {
	benchmarkCacheGet(b, NoExpiration)
}

func benchmarkCacheGet(b *testing.B, exp time.Duration) {
	b.StopTimer()
	tc := New[any](exp, 0)
	tc.SetWithExpiration("foo", "bar", DefaultExpiration)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		tc.Get("foo")
	}
}

func BenchmarkRWMutexMapGet(b *testing.B) {
	b.StopTimer()
	m := map[string]string{
		"foo": "bar",
	}
	mu := sync.RWMutex{}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		mu.RLock()
		_ = m["foo"]
		mu.RUnlock()
	}
}

func BenchmarkRWMutexInterfaceMapGetStruct(b *testing.B) {
	b.StopTimer()
	s := struct{ name string }{name: "foo"}
	m := map[interface{}]string{
		s: "bar",
	}
	mu := sync.RWMutex{}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		mu.RLock()
		_ = m[s]
		mu.RUnlock()
	}
}

func BenchmarkRWMutexInterfaceMapGetString(b *testing.B) {
	b.StopTimer()
	m := map[interface{}]string{
		"foo": "bar",
	}
	mu := sync.RWMutex{}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		mu.RLock()
		_ = m["foo"]
		mu.RUnlock()
	}
}

func BenchmarkCacheGetConcurrentExpiring(b *testing.B) {
	benchmarkCacheGetConcurrent(b, 5*time.Minute)
}

func BenchmarkCacheGetConcurrentNotExpiring(b *testing.B) {
	benchmarkCacheGetConcurrent(b, NoExpiration)
}

func benchmarkCacheGetConcurrent(b *testing.B, exp time.Duration) {
	b.StopTimer()
	tc := New[any](exp, 0)
	tc.SetWithExpiration("foo", "bar", DefaultExpiration)
	wg := new(sync.WaitGroup)
	workers := runtime.NumCPU()
	each := b.N / workers
	wg.Add(workers)
	b.StartTimer()
	for i := 0; i < workers; i++ {
		go func() {
			for j := 0; j < each; j++ {
				tc.Get("foo")
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func BenchmarkRWMutexMapGetConcurrent(b *testing.B) {
	b.StopTimer()
	m := map[string]string{
		"foo": "bar",
	}
	mu := sync.RWMutex{}
	wg := new(sync.WaitGroup)
	workers := runtime.NumCPU()
	each := b.N / workers
	wg.Add(workers)
	b.StartTimer()
	for i := 0; i < workers; i++ {
		go func() {
			for j := 0; j < each; j++ {
				mu.RLock()
				_ = m["foo"]
				mu.RUnlock()
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func BenchmarkCacheGetManyConcurrentExpiring(b *testing.B) {
	benchmarkCacheGetManyConcurrent(b, 5*time.Minute)
}

func BenchmarkCacheGetManyConcurrentNotExpiring(b *testing.B) {
	benchmarkCacheGetManyConcurrent(b, NoExpiration)
}

func benchmarkCacheGetManyConcurrent(b *testing.B, exp time.Duration) {
	// This is the same as BenchmarkCacheGetConcurrent, but its result
	// can be compared against BenchmarkShardedCacheGetManyConcurrent
	// in sharded_test.go.
	b.StopTimer()
	n := 10000
	tc := New[any](exp, 0)
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		k := "foo" + strconv.Itoa(i)
		keys[i] = k
		tc.SetWithExpiration(k, "bar", DefaultExpiration)
	}
	each := b.N / n
	wg := new(sync.WaitGroup)
	wg.Add(n)
	for _, v := range keys {
		go func(k string) {
			for j := 0; j < each; j++ {
				tc.Get(k)
			}
			wg.Done()
		}(v)
	}
	b.StartTimer()
	wg.Wait()
}

func BenchmarkCacheSetExpiring(b *testing.B) {
	benchmarkCacheSet(b, 5*time.Minute)
}

func BenchmarkCacheSetNotExpiring(b *testing.B) {
	benchmarkCacheSet(b, NoExpiration)
}

func benchmarkCacheSet(b *testing.B, exp time.Duration) {
	b.StopTimer()
	tc := New[any](exp, 0)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		tc.SetWithExpiration("foo", "bar", DefaultExpiration)
	}
}

func BenchmarkRWMutexMapSet(b *testing.B) {
	b.StopTimer()
	m := map[string]string{}
	mu := sync.RWMutex{}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		mu.Lock()
		m["foo"] = "bar"
		mu.Unlock()
	}
}

func BenchmarkCacheSetDelete(b *testing.B) {
	b.StopTimer()
	tc := New[any](DefaultExpiration, 0)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		tc.SetWithExpiration("foo", "bar", DefaultExpiration)
		tc.Delete("foo")
	}
}

func BenchmarkRWMutexMapSetDelete(b *testing.B) {
	b.StopTimer()
	m := map[string]string{}
	mu := sync.RWMutex{}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		mu.Lock()
		m["foo"] = "bar"
		mu.Unlock()
		mu.Lock()
		delete(m, "foo")
		mu.Unlock()
	}
}

func BenchmarkCacheSetDeleteSingleLock(b *testing.B) {
	b.StopTimer()
	tc := New[any](DefaultExpiration, 0)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		tc.mu.Lock()
		tc.set("foo", "bar", DefaultExpiration)
		tc.delete("foo")
		tc.mu.Unlock()
	}
}

func BenchmarkRWMutexMapSetDeleteSingleLock(b *testing.B) {
	b.StopTimer()
	m := map[string]string{}
	mu := sync.RWMutex{}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		mu.Lock()
		m["foo"] = "bar"
		delete(m, "foo")
		mu.Unlock()
	}
}

func BenchmarkDeleteExpiredLoop(b *testing.B) {
	b.StopTimer()
	tc := New[any](5*time.Minute, 0)
	tc.mu.Lock()
	for i := 0; i < 100000; i++ {
		tc.set(strconv.Itoa(i), "bar", DefaultExpiration)
	}
	tc.mu.Unlock()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		tc.deleteExpired()
	}
}
