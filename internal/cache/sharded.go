package cache

import (
	"errors"
	"sync"
)

type Cache[K comparable, V any] interface {
	Get(key K) (V, bool)
	Put(key K, value V)
	Delete(key K)
}

type shard[K comparable, V any] struct {
	sync.RWMutex
	data map[K]V
}

type ShardedCache[K comparable, V any] struct {
	shards []shard[K, V]
	hash   func(K) uint64
}

func NewSharded[K comparable, V any](shardCount int) (*ShardedCache[K, V], error) {
	if shardCount <= 0 {
		return nil, errors.New("shard count must be positive")
	}

	shards := make([]shard[K, V], shardCount)
	for i := range shards {
		shards[i].data = make(map[K]V)
	}

	return &ShardedCache[K, V]{
		shards: shards,
		hash:   nil,
	}, nil
}

func (c *ShardedCache[K, V]) Get(key K) (V, bool) {
	var (
		zeroV    V
		zeroBool bool
	)
	return zeroV, zeroBool
}

func (c *ShardedCache[K, V]) Put(key K, val V) {
}

func (c *ShardedCache[K, V]) Delete(key K) {
}
