package cache

import (
	"context"
	"github.com/gotomicro/ekit/list"
	"sync"
	"time"
)

type MaxMemoryCache struct {
	Cache
	max         int64
	used        int64
	mutex       *sync.Mutex
	orderedKeys *list.LinkedList[string]
}

func NewMaxMemoryCache(max int64, cache Cache) *MaxMemoryCache {
	result := &MaxMemoryCache{
		max:         max,
		Cache:       cache,
		mutex:       &sync.Mutex{},
		orderedKeys: list.NewLinkedList[string](),
	}
	result.Cache.OnEvicted(result.evictedKeyOrder)
	return result
}

func (m *MaxMemoryCache) Set(ctx context.Context, key string, val []byte,
	expiration time.Duration) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	_, _ = m.Cache.LoadAndDelete(ctx, key)
	for m.used+int64(len(val)) > m.max {
		k, err := m.orderedKeys.Get(0)
		if err != nil {
			return err
		}
		_ = m.Cache.Delete(ctx, k)
	}
	err := m.Cache.Set(ctx, key, val, expiration)
	if err == nil {
		m.used = m.used + int64(len(val))
		_ = m.orderedKeys.Append(key)
	}

	return nil
}

func (m *MaxMemoryCache) removeKeyOrder(key string) {
	for i := 0; i < m.orderedKeys.Len(); i++ {
		keyInner, _ := m.orderedKeys.Get(i)
		if keyInner == key {
			_, _ = m.orderedKeys.Delete(i)
			return
		}
	}
}

func (m *MaxMemoryCache) Get(ctx context.Context, key string) (any, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	val, err := m.Cache.Get(ctx, key)

	if err == nil {
		m.removeKeyOrder(key)
		_ = m.orderedKeys.Append(key)
	}

	return val, err
}

func (m *MaxMemoryCache) Delete(ctx context.Context, key string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.Cache.Delete(ctx, key)
}

func (m *MaxMemoryCache) LoadAndDelete(ctx context.Context, key string) (any, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.Cache.LoadAndDelete(ctx, key)
}

func (m *MaxMemoryCache) OnEvicted(fn func(key string, val []byte)) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.Cache.OnEvicted(func(key string, val []byte) {
		m.evictedKeyOrder(key, val)
		fn(key, val)
	})
}

func (m *MaxMemoryCache) evictedKeyOrder(key string, val []byte) {
	m.used = m.used - int64(len(val))
	m.removeKeyOrder(key)
}
