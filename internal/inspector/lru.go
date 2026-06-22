package inspector

import (
	"container/list"
	"time"
)

type lruCache[K comparable, V any] struct {
	maxEntries int
	ttl        time.Duration
	clock      func() time.Time
	items      map[K]*list.Element
	order      *list.List
}

type lruEntry[K comparable, V any] struct {
	key       K
	value     V
	expiresAt time.Time
}

func newLRUCache[K comparable, V any](maxEntries int, ttl time.Duration, clock func() time.Time) *lruCache[K, V] {
	return &lruCache[K, V]{
		maxEntries: maxEntries,
		ttl:        ttl,
		clock:      clock,
		items:      make(map[K]*list.Element),
		order:      list.New(),
	}
}

func (c *lruCache[K, V]) Get(key K) (V, bool) {
	var zero V
	el, ok := c.items[key]
	if !ok {
		return zero, false
	}
	entry := el.Value.(*lruEntry[K, V])
	if c.clock().After(entry.expiresAt) {
		c.removeElement(el)
		return zero, false
	}
	c.order.MoveToFront(el)
	return entry.value, true
}

func (c *lruCache[K, V]) Put(key K, value V) int {
	now := c.clock()
	if el, ok := c.items[key]; ok {
		entry := el.Value.(*lruEntry[K, V])
		entry.value = value
		entry.expiresAt = now.Add(c.ttl)
		c.order.MoveToFront(el)
		return 0
	}

	el := c.order.PushFront(&lruEntry[K, V]{
		key:       key,
		value:     value,
		expiresAt: now.Add(c.ttl),
	})
	c.items[key] = el

	evicted := 0
	for len(c.items) > c.maxEntries {
		if c.removeOldest() {
			evicted++
		}
	}

	return evicted
}

func (c *lruCache[K, V]) Len() int {
	return len(c.items)
}

func (c *lruCache[K, V]) removeOldest() bool {
	el := c.order.Back()
	if el == nil {
		return false
	}
	c.removeElement(el)
	return true
}

func (c *lruCache[K, V]) removeElement(el *list.Element) {
	c.order.Remove(el)
	entry := el.Value.(*lruEntry[K, V])
	delete(c.items, entry.key)
}
