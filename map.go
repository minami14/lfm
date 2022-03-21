package lfm

import (
	"hash/fnv"
	"sync/atomic"
	"unsafe"
)

type Map[T any] struct {
	pairs  []*atomic.Value
	length int64
}

func New[T any](size int) *Map[T] {
	pairs := make([]*atomic.Value, size)
	for i := range pairs {
		pairs[i] = new(atomic.Value)
	}
	return &Map[T]{
		pairs: pairs,
	}
}

type keyValuePair[T any] struct {
	key      string
	value    T
	nextPair *atomic.Value
}

func (m *Map) index(key string) int {
	if len(m.pairs) == 1 {
		return 0
	}
	h := fnv.New64()
	_, _ = h.Write([]byte(key))
	return int(h.Sum64()&((1<<63)-1)) % (len(m.pairs) - 1)
}

func (m *Map[T]) Store(key string, value T) {
	pair := &keyValuePair[T]{
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
		p := current.Load().(*keyValuePair[T])
		if p.key == key {
			p.value = value
			return
		}
		current = p.nextPair
	}
}

func (m *Map[T]) Load(key string) (T, bool) {
	i := m.index(key)
	current := m.pairs[i]
	for {
		v := current.Load()
		if v == nil {
			return nil, false
		}
		p := v.(*keyValuePair[T])
		if p.key == key {
			return p.value, true
		}
		current = p.nextPair
	}
}

func (m *Map[T]) LoadOrStore(key string, value T) (T, bool) {
	pair := &keyValuePair[T]{
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
		p := current.Load().(*keyValuePair[T])
		if p.key == key {
			return p.value, false
		}
		current = p.nextPair
	}
}

func (m *Map[T]) LoadAndStore(key string, value T) T {
	pair := &keyValuePair[T]{
		key:      key,
		value:    value,
		nextPair: new(atomic.Value),
	}
	i := m.index(key)
	current := m.pairs[i]
	for {
		if current.CompareAndSwap(nil, pair) {
			atomic.AddInt64(&m.length, 1)
			return nil
		}
		p := current.Load().(*keyValuePair[T])
		if p.key == key {
			old := p.value
			p.value = value
			return old
		}
		current = p.nextPair
	}
}

func (m *Map[T]) StoreNotExists(key string, value T) bool {
	pair := &keyValuePair[T]{
		key:      key,
		value:    value,
		nextPair: new(atomic.Value),
	}
	i := m.index(key)
	current := m.pairs[i]
	for {
		if current.CompareAndSwap(nil, pair) {
			atomic.AddInt64(&m.length, 1)
			return true
		}
		p := current.Load().(*keyValuePair[T])
		if p.key == key {
			return false
		}
		current = p.nextPair
	}
}

func (m *Map) Delete(key string) bool {
	i := m.index(key)
	ov := *(*unsafe.Pointer)(unsafe.Pointer(&m.pairs[i]))
	pv := m.pairs[i]
	v := pv.Load()
	if v == nil {
		return false
	}
	p := v.(*keyValuePair)
	if p.key == key {
		if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&m.pairs[i])), ov, unsafe.Pointer(p.nextPair)) {
			atomic.AddInt64(&m.length, -1)
			return true
		}
		return false
	}

	pp := p
	for {
		ov := *(*unsafe.Pointer)(unsafe.Pointer(&pp.nextPair))
		nv := pp.nextPair.Load()
		var np *keyValuePair
		var npp unsafe.Pointer
		if nv != nil {
			np = nv.(*keyValuePair)
			npp = unsafe.Pointer(np.nextPair)
		}
		if np.key == key {
			if atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&pp.nextPair)), ov, npp) {
				atomic.AddInt64(&m.length, -1)
				return true
			}
			return false
		}
		pv := pp.nextPair.Load()
		if pv == nil {
			return false
		}
		pp = pv.(*keyValuePair)
	}
}

func (m *Map[T]) Range(f func(key string, value T) bool) {
	for _, pair := range m.pairs {
		current := pair.Load()
		if current == nil {
			continue
		}
		for {
			p := current.(*keyValuePair[T])
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
