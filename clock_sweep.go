package clocksweep

import (
	"errors"
	"sync"
)

// The clock-sweep algorithm is a scheduling algorithm used to assign tasks to resources in a distributed system. It works by assigning tasks to resources in a circular fashion, starting at the beginning of the list of resources and moving clockwise until all tasks have been assigned. The algorithm is designed to ensure that all resources are used efficiently and that tasks are evenly distributed among them.
//
// To implement the clock-sweep algorithm, the first step is to create a list of resources and tasks. The resources should be listed in a circular fashion, with the first resource at the top of the list and the last resource at the bottom. The tasks should be listed in the same order, with the first task at the top and the last task at the bottom.
//
// Next, the algorithm should be initialized. This involves setting a pointer to the first resource in the list and assigning the first task to that resource. The pointer should then be moved clockwise to the next resource in the list and the next task should be assigned to that resource. This process should be repeated until all tasks have been assigned to resources.
//
// Finally, the algorithm should be monitored to ensure that all resources are being used efficiently and that tasks are being evenly distributed among them. If any resource is not being used or if any task is not being assigned to a resource, the algorithm should be adjusted accordingly.
//
// The clock-sweep algorithm is a simple yet effective scheduling algorithm that can be used to assign tasks to resources in a distributed system. It ensures that all resources are used efficiently and that tasks are evenly distributed among them.

type Frame[K comparable, V any] struct {
	mux    sync.Mutex
	cnt    int
	refCnt int
	key    K
	value  V
}

type ClockSweep[K comparable, V any] struct {
	frames []*Frame[K, V]
}

func NewClockSweep[K comparable, V any](capacity int) *ClockSweep[K, V] {
	return &ClockSweep[K, V]{make([]*Frame[K, V], capacity)}
}

var (
	ErrCapacityExceeded = errors.New("capacity exceeded")
	ErrKeyNotFound      = errors.New("key not found")
)

// Get returns the value associated with the given key.
func (c *ClockSweep[K, V]) Acquire(key K) (val *V, release func(), err error) {
	var frame *Frame[K, V]
	for i := 0; i < cap(c.frames); i++ {
		if c.frames[i] == nil {
			continue
		}

		f := c.frames[i]
		if f.key == key {
			f.mux.Lock()
			f.cnt += 1
			f.refCnt += 1
			frame = f
			f.mux.Unlock()
		} else {
			f.mux.Lock()
			f.cnt -= 1
			f.mux.Unlock()
		}
	}

	if frame == nil {
		return nil, nil, ErrKeyNotFound
	}

	return &frame.value, func() {
		frame.mux.Lock()
		frame.refCnt -= 1
		frame.mux.Unlock()
	}, nil
}

// Set sets the value associated with the given key.
func (c *ClockSweep[K, V]) Set(key K, value V) error {
	freeIdx, ok := c.evict()
	if !ok {
		return ErrCapacityExceeded
	}

	c.frames[freeIdx] = &Frame[K, V]{
		cnt:    1,
		refCnt: 0,
		key:    key,
		value:  value,
	}

	return nil
}

func (c *ClockSweep[K, V]) evict() (index int, ok bool) {
	for i := 0; i < cap(c.frames); i++ {
		if c.frames[i] == nil {
			return i, true
		}

		c.frames[i].mux.Lock()
		shouldEvict := c.frames[i].cnt == 0 && c.frames[i].refCnt == 0
		c.frames[i].mux.Unlock()

		if shouldEvict {
			return i, true
		}
	}

	return 0, false
}
