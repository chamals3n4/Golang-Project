package kv

import (
	"container/list"
	"sync"

	bolt "go.etcd.io/bbolt"
)

/* ──────────────── public commands ──────────────── */
type SetCmd struct{ Key, Value string }
type DelCmd struct{ Key string }
type GetCmd struct{ Key string }

/* ──────────────── Store struct ─────────────────── */
type Store struct {
	db  *bolt.DB
	lru *lruCache
	mu  sync.RWMutex
}

func New(db *bolt.DB, maxEntries int) *Store {
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("kv"))
		return err
	})
	if err != nil {
		panic(err)
	}
	return &Store{db: db, lru: newLRU(maxEntries)}
}

func (s *Store) Close() error {
	return s.db.Close()
}

/* ─────────────────── API ───────────────────────── */

func (s *Store) Get(key string) string {
	// Try cache first
	if v, ok := s.lru.get(key); ok {
		return v
	}

	// Cache miss, check database
	var value string
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("kv"))
		if b == nil {
			return nil
		}
		v := b.Get([]byte(key))
		if v != nil {
			value = string(v)
		}
		return nil
	})
	if err != nil {
		return ""
	}

	// Update cache if found in database
	if value != "" {
		s.lru.add(key, value)
	}
	return value
}

func (s *Store) Apply(cmd any) any {
	switch c := cmd.(type) {
	case SetCmd:
		err := s.db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("kv"))
			if b == nil {
				return nil
			}
			return b.Put([]byte(c.Key), []byte(c.Value))
		})
		if err != nil {
			return err
		}
		s.lru.add(c.Key, c.Value)

	case DelCmd:
		err := s.db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("kv"))
			if b == nil {
				return nil
			}
			return b.Delete([]byte(c.Key))
		})
		if err != nil {
			return err
		}
		s.lru.remove(c.Key)

	case GetCmd:
		return s.Get(c.Key)
	}
	return nil
}

/* ───────────── LRU cache ──────────────── */
type entry struct{ key, value string }

type lruCache struct {
	mu  sync.Mutex
	ll  *list.List
	tab map[string]*list.Element
	cap int
}

func newLRU(cap int) *lruCache {
	return &lruCache{ll: list.New(), tab: make(map[string]*list.Element), cap: cap}
}

func (c *lruCache) get(k string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.tab[k]
	if !ok {
		return "", false
	}
	c.ll.MoveToFront(e)
	return e.Value.(*entry).value, true
}

func (c *lruCache) add(k, v string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if e, ok := c.tab[k]; ok {
		c.ll.MoveToFront(e)
		e.Value.(*entry).value = v
		return
	}
	e := c.ll.PushFront(&entry{k, v})
	c.tab[k] = e
	if c.ll.Len() > c.cap {
		tail := c.ll.Back()
		c.ll.Remove(tail)
		delete(c.tab, tail.Value.(*entry).key)
	}
}

func (c *lruCache) remove(k string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.tab[k]; ok {
		c.ll.Remove(e)
		delete(c.tab, k)
	}
}
