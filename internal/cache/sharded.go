package cache

import (
	"errors"
	"hash/maphash"
	"sync"
)

type shard[K comparable, V any] struct {
	sync.RWMutex
	data map[K]V
}

type ShardedCache[K comparable, V any] struct {
	shards []*shard[K, V]
	hasher func(K) uint64
}

func NewSharded[K comparable, V any](shardCount int) (*ShardedCache[K, V], error) {
	if shardCount <= 0 {
		return nil, errors.New("shard count must be positive")
	}

	shards := make([]*shard[K, V], shardCount)
	for i := range shards {
		shards[i] = &shard[K, V]{
			data: make(map[K]V),
		}
	}

	seed := maphash.MakeSeed()
	hasher := func(key K) uint64 {
		return maphash.Comparable(seed, key)
	}

	return &ShardedCache[K, V]{
		shards: shards,
		hasher: hasher,
	}, nil
}

func (c *ShardedCache[K, V]) Get(key K) (V, bool) {
	hash := c.hasher(key)

	return c.shards[hash%uint64(len(c.shards))].get(key)
}

func (c *ShardedCache[K, V]) Put(key K, val V) {
	hash := c.hasher(key)

	c.shards[hash%uint64(len(c.shards))].put(key, val)
}

func (c *ShardedCache[K, V]) Delete(key K) {
	hash := c.hasher(key)

	c.shards[hash%uint64(len(c.shards))].delete(key)
}

func (s *shard[K, V]) get(key K) (V, bool) {
	s.RLock()
	defer s.RUnlock()
	val, ok := s.data[key]
	return val, ok
}

func (s *shard[K, V]) put(key K, val V) {
	s.Lock()
	defer s.Unlock()
	s.data[key] = val
}

func (s *shard[K, V]) delete(key K) {
	s.Lock()
	defer s.Unlock()
	delete(s.data, key)
}
