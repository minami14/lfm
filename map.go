package lfm

import (
	"hash/fnv"
	"sync/atomic"
)

type Map struct {
	pairs  []*atomic.Value
	length int64
}

func New(size int) *Map {
	pairs := make([]*atomic.Value, size)
	for i := range pairs {
		pairs[i] = new(atomic.Value)
	}
	return &Map{
		pairs: pairs,
	}
}

type keyValuePair struct {
	key      string
	value    interface{}
	nextPair *atomic.Value
}

func (m *Map) index(key string) int {
	h := fnv.New64()
	_, _ = h.Write([]byte(key))
	return int(h.Sum64()&((1<<63)-1)) % (len(m.pairs) - 1)
}

func (m *Map) Store(key string, value interface{}) {
	pair := &keyValuePair{
		key:      key,
		value:    value,
		nextPair: new(atomic.Value),
	}
	i := m.index(key)
	current := m.pairs[i]
	for {
		if current.CompareAndSwap(nil, pair) {
			atomic.AddInt64(&m.length, 1)
			return
		}
		p := current.Load().(*keyValuePair)
		if p.key == key {
			p.value = value
			return
		}
		current = p.nextPair
	}
}

func (m *Map) Load(key string) (interface{}, bool) {
	i := m.index(key)
	current := m.pairs[i]
	for {
		v := current.Load()
		if v == nil {
			return nil, false
		}
		p := v.(*keyValuePair)
		if p.key == key {
			return p.value, true
		}
		current = p.nextPair
	}
}

func (m *Map) LoadOrStore(key string, value interface{}) (interface{}, bool) {
	pair := &keyValuePair{
		key:      key,
		value:    value,
		nextPair: new(atomic.Value),
	}
	i := m.index(key)
	current := m.pairs[i]
	for {
		if current.CompareAndSwap(nil, pair) {
			atomic.AddInt64(&m.length, 1)
			return value, true
		}
		p := current.Load().(*keyValuePair)
		if p.key == key {
			return p.value, false
		}
		current = p.nextPair
	}
}

func (m *Map) Delete(key string) bool {
	i := m.index(key)
	v := m.pairs[i].Load()
	if v == nil {
		return false
	}
	p := v.(*keyValuePair)
	if p.key == key {
		if m.pairs[i].CompareAndSwap(p, p.nextPair.Load()) {
			atomic.AddInt64(&m.length, -1)
			return true
		}
		return false
	}

	pre := m.pairs[i]
	current := p.nextPair
	for {
		v := current.Load()
		if v == nil {
			return false
		}
		p := v.(*keyValuePair)
		if p.key == key {
			if pre.CompareAndSwap(current, p.nextPair) {
				atomic.AddInt64(&m.length, -1)
				return true
			}
			return false
		}
		pre = current
		current = p.nextPair
	}
}

func (m *Map) Range(f func(key string, value interface{}) bool) {
	for _, pair := range m.pairs {
		current := pair.Load()
		if current == nil {
			continue
		}
		for {
			p := current.(*keyValuePair)
			if !f(p.key, p.value) {
				return
			}
			current = p.nextPair.Load()
			if current == nil {
				break
			}
		}
	}
}

func (m *Map) Length() int {
	return int(m.length)
}
