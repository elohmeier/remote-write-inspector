package inspector

import (
	"sync"
	"time"
)

const defaultCacheShards = 64

type stateCache[K comparable, V any] struct {
	shards []stateCacheShard[K, V]
	shard  func(K) uint64
}

type stateCacheShard[K comparable, V any] struct {
	mtx   sync.Mutex
	cache *circularCache[K, V]
}

type circularCache[K comparable, V any] struct {
	maxEntries int
	ttl        time.Duration
	clock      func() time.Time
	items      map[K]int
	slots      []cacheSlot[K, V]
	free       []int
	usedSlots  int
	nextEvict  int
	len        int
}

type cacheSlot[K comparable, V any] struct {
	key       K
	value     V
	expiresAt time.Time
	used      bool
}

func newStateCache[K comparable, V any](maxEntries int, ttl time.Duration, clock func() time.Time, shard func(K) uint64) *stateCache[K, V] {
	if maxEntries <= 0 {
		maxEntries = 1
	}
	shardCount := defaultCacheShards
	if maxEntries < shardCount {
		shardCount = maxEntries
	}
	c := &stateCache[K, V]{
		shards: make([]stateCacheShard[K, V], shardCount),
		shard:  shard,
	}
	baseSize := maxEntries / shardCount
	extra := maxEntries % shardCount
	for idx := range c.shards {
		size := baseSize
		if idx < extra {
			size++
		}
		c.shards[idx].cache = newCircularCache[K, V](size, ttl, clock)
	}
	return c
}

func (c *stateCache[K, V]) GetOrPut(key K, value V) (V, bool, int) {
	shard := c.shardFor(key)
	shard.mtx.Lock()
	defer shard.mtx.Unlock()

	if existing, ok := shard.cache.Get(key); ok {
		return existing, true, 0
	}
	evicted := shard.cache.Put(key, value)
	var zero V
	return zero, false, evicted
}

func (c *stateCache[K, V]) Len() int {
	total := 0
	for idx := range c.shards {
		shard := &c.shards[idx]
		shard.mtx.Lock()
		total += shard.cache.Len()
		shard.mtx.Unlock()
	}
	return total
}

func (c *stateCache[K, V]) shardFor(key K) *stateCacheShard[K, V] {
	return &c.shards[int(c.shard(key)%uint64(len(c.shards)))]
}

func newCircularCache[K comparable, V any](maxEntries int, ttl time.Duration, clock func() time.Time) *circularCache[K, V] {
	if maxEntries <= 0 {
		maxEntries = 1
	}
	return &circularCache[K, V]{
		maxEntries: maxEntries,
		ttl:        ttl,
		clock:      clock,
		items:      make(map[K]int, maxEntries),
		slots:      make([]cacheSlot[K, V], maxEntries),
	}
}

func (c *circularCache[K, V]) Get(key K) (V, bool) {
	var zero V
	idx, ok := c.items[key]
	if !ok {
		return zero, false
	}
	slot := &c.slots[idx]
	if !slot.used {
		delete(c.items, key)
		return zero, false
	}
	if c.clock().After(slot.expiresAt) {
		c.removeSlot(idx)
		return zero, false
	}
	return slot.value, true
}

func (c *circularCache[K, V]) Put(key K, value V) int {
	now := c.clock()
	if idx, ok := c.items[key]; ok {
		slot := &c.slots[idx]
		slot.value = value
		slot.expiresAt = now.Add(c.ttl)
		return 0
	}

	idx, evicted := c.nextSlot()
	c.slots[idx] = cacheSlot[K, V]{
		key:       key,
		value:     value,
		expiresAt: now.Add(c.ttl),
		used:      true,
	}
	c.items[key] = idx
	c.len++
	return evicted
}

func (c *circularCache[K, V]) Len() int {
	return c.len
}

func (c *circularCache[K, V]) nextSlot() (int, int) {
	if n := len(c.free); n > 0 {
		idx := c.free[n-1]
		c.free = c.free[:n-1]
		return idx, 0
	}
	if c.usedSlots < c.maxEntries {
		idx := c.usedSlots
		c.usedSlots++
		return idx, 0
	}

	idx := c.nextEvict
	c.nextEvict = (c.nextEvict + 1) % c.maxEntries
	old := c.slots[idx]
	if old.used {
		delete(c.items, old.key)
		c.len--
		return idx, 1
	}
	return idx, 0
}

func (c *circularCache[K, V]) removeSlot(idx int) {
	slot := &c.slots[idx]
	if !slot.used {
		return
	}
	delete(c.items, slot.key)
	var zeroK K
	var zeroV V
	*slot = cacheSlot[K, V]{key: zeroK, value: zeroV}
	c.free = append(c.free, idx)
	c.len--
}
