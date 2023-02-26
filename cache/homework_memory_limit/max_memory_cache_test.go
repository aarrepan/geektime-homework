package cache

import (
	"context"
	"errors"
	"github.com/gotomicro/ekit/list"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var errNotFound = errors.New("no errors found")

func TestMaxMemoryCache_Get(t *testing.T) {
	testCases := []struct {
		name  string
		cache func() *MaxMemoryCache

		key string

		wantKeys []string
		wantErr  error
	}{
		{
			name: "exist [key1, key2]",
			cache: func() *MaxMemoryCache {
				res := NewMaxMemoryCache(100, &mockCache{
					data: map[string][]byte{
						"key1": []byte("OK"),
					},
				})
				res.orderedKeys = list.NewLinkedListOf[string]([]string{"key1", "key2"})
				return res
			},
			key:      "key1",
			wantKeys: []string{"key2", "key1"},
		},
		{
			name: "not exist [key1]",
			cache: func() *MaxMemoryCache {
				res := NewMaxMemoryCache(100, &mockCache{})
				return res
			},
			key:      "key1",
			wantKeys: []string{},
			wantErr:  errNotFound,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cache := tc.cache()
			_, err := cache.Get(context.Background(), tc.key)
			assert.Equal(t, tc.wantErr, err)
			assert.Equal(t, tc.wantKeys, cache.orderedKeys.AsSlice())
		})
	}
}

func TestMaxMemoryCache_Set(t *testing.T) {
	testCases := []struct {
		name  string
		cache func() *MaxMemoryCache

		key string
		val []byte

		wantKeys []string
		wantErr  error
		wantUsed int64
	}{
		{
			name: "not exist [key1]",
			cache: func() *MaxMemoryCache {
				res := NewMaxMemoryCache(100, &mockCache{data: map[string][]byte{}})
				return res
			},
			key:      "key1",
			val:      []byte("hello"),
			wantKeys: []string{"key1"},
			wantUsed: 5,
		},
		{
			name: "override inc order [key1]",
			cache: func() *MaxMemoryCache {
				res := NewMaxMemoryCache(100, &mockCache{
					data: map[string][]byte{
						"key1": []byte("hello"),
					},
				})
				res.orderedKeys = list.NewLinkedListOf[string]([]string{"key1"})
				res.used = 5
				return res
			},
			key:      "key1",
			val:      []byte("hello,world"),
			wantKeys: []string{"key1"},
			wantUsed: 11,
		},
		{
			name: "override desc order [key1]",
			cache: func() *MaxMemoryCache {
				res := NewMaxMemoryCache(100, &mockCache{
					data: map[string][]byte{
						"key1": []byte("hello"),
					},
				})
				res.orderedKeys = list.NewLinkedListOf[string]([]string{"key1"})
				res.used = 5
				return res
			},
			key:      "key1",
			val:      []byte("he"),
			wantKeys: []string{"key1"},
			wantUsed: 2,
		},
		{
			name: "delete [key4]",
			cache: func() *MaxMemoryCache {
				res := NewMaxMemoryCache(40, &mockCache{
					data: map[string][]byte{
						"key1": []byte("hello, key1"),
						"key2": []byte("hello, key2"),
						"key3": []byte("hello, key3"),
					},
				})
				res.orderedKeys = list.NewLinkedListOf[string]([]string{"key1", "key2", "key3"})
				res.used = 33
				return res
			},
			key:      "key4",
			val:      []byte("hello, key4"),
			wantKeys: []string{"key2", "key3", "key4"},
			wantUsed: 33,
		},
		{
			name: "delete multi keys",
			cache: func() *MaxMemoryCache {
				res := NewMaxMemoryCache(40, &mockCache{
					data: map[string][]byte{
						"key1": []byte("hello, key1"),
						"key2": []byte("hello, key2"),
						"key3": []byte("hello, key3"),
					},
				})
				res.orderedKeys = list.NewLinkedListOf[string]([]string{"key1", "key2", "key3"})
				res.used = 33
				return res
			},
			key:      "key4",
			val:      []byte("hello, key4,hello, key4"),
			wantKeys: []string{"key3", "key4"},
			wantUsed: 34,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cache := tc.cache()
			err := cache.Set(context.Background(), tc.key, tc.val, time.Minute)
			assert.Equal(t, tc.wantKeys, cache.orderedKeys.AsSlice())
			assert.Equal(t, tc.wantErr, err)
			assert.Equal(t, tc.wantUsed, cache.used)
		})
	}
}

type mockCache struct {
	Cache
	fn   func(key string, val []byte)
	data map[string][]byte
}

func (m *mockCache) Set(ctx context.Context, key string, val []byte, expiration time.Duration) error {
	m.data[key] = val
	return nil
}

func (m *mockCache) Get(ctx context.Context, key string) ([]byte, error) {
	val, ok := m.data[key]
	if ok {
		return val, nil
	}
	return nil, errNotFound
}

func (m *mockCache) Delete(ctx context.Context, key string) error {
	val, ok := m.data[key]
	if ok {
		m.fn(key, val)
	}
	return nil
}

func (m *mockCache) LoadAndDelete(ctx context.Context, key string) ([]byte, error) {
	val, ok := m.data[key]
	if ok {
		m.fn(key, val)
		return val, nil
	}
	return nil, errNotFound
}

func (m *mockCache) OnEvicted(fn func(key string, val []byte)) {
	m.fn = fn
}
