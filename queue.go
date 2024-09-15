// Copyright 2024 Blues Inc.  All rights reserved.
// Use of this source code is governed by licenses granted by the
// copyright holder including that found in the LICENSE file.
//
// One of the amazing benefits of golang's "chan" structure is that it is a lock-free and highly-efficient
// for multiple goroutines to implement a queue.  One of the downsides, however, is that these queues
// must be allocated as fixed-length data structures, and the enqueuer will block if/when enqueueing if
// the channel fills up.  This typically results in developers just doing a wild-ass guess as to
// how many will be queueud, ignoring the overall system performance implications of what happens when
// the channel fills up and the enqueuer blocks.
//
// In a situation where there are an extremely large number of concurrent queues, the developer is
// thus encouraged to minimize the channel size so as to be conservative about use of heap.  On the other
// hand, we may need to deal with very high bursts of enqueueing on a statistically small number of channels,
// and so the reduced size of those channels will cause bad system behavior.
//
// This package implements a super-efficient "infinite length channel".  It preserves and stands on
// the shoulders of 'chan' semantics for timeout handling and for wait/signal, and yet (critically) it
// implements the queue using the careful head/tail manipulation mechanisms of Michael & Scott's classic
// concurrent lock-free queue algorithm, however it deviates from that algorithm (which would 'spin')
// by using a simple 1-entry golang chan for timeout and blocking.
// https://www.cs.rochester.edu/~scott/papers/1996_PODC_queues.pdf
//
// Because of the recurring challenges in creating and 'winding down' queues, this code also supports
// the concept of synchronously deleting the queue by internally using the 'nil' object as a signal
// that the queue has been deleted and will no longer be used.

package safequeue

import (
	"fmt"
	"sync/atomic"
	"time"
	"unsafe"
)

// Node represents a single item in the lock-free queue.
type Node struct {
	value interface{}
	next  unsafe.Pointer // Atomic pointer to next node
}

// SafeQueue represents a lock-free, thread-safe queue.
type SafeQueue struct {
	head       unsafe.Pointer // Atomic pointer to the head node
	tail       unsafe.Pointer // Atomic pointer to the tail node
	signalChan chan struct{}  // Channel used to signal availability of new items
	deleted    bool           // Refuses new entries to be enqueued after deletion
}

// NewSafeQueue creates a new safe queue with a dummy node and a signaling channel.
func NewSafeQueue() *SafeQueue {
	dummy := &Node{} // Dummy node for initialization
	return &SafeQueue{
		head:       unsafe.Pointer(dummy),
		tail:       unsafe.Pointer(dummy),
		signalChan: make(chan struct{}, 1), // 1-entry channel for signaling
	}
}

// DeleteQueue marks the queue for deletion and prevents anything further from being enqueued
func (q *SafeQueue) DeleteQueue() {
	q.Enqueue(nil)
	q.deleted = true
}

// Enqueue adds an item to the queue in a lock-free manner, returning an error if it has been deleted
func (q *SafeQueue) Enqueue(value interface{}) error {

	if q.deleted {
		return fmt.Errorf("enqueue: queue has been deleted")
	}

	newNode := &Node{value: value}

	// Retry loop for handling atomic concurrency
	for {

		// Load the tail pointer
		tail := atomic.LoadPointer(&q.tail)
		next := atomic.LoadPointer(&((*Node)(tail)).next)

		// Check that the tail hasn't moved
		if tail == atomic.LoadPointer(&q.tail) {
			if next == nil {

				// Tail is pointing to the last node, try to append new node
				if atomic.CompareAndSwapPointer(&((*Node)(tail)).next, next, unsafe.Pointer(newNode)) {

					// Successfully appended the new node, now move the tail pointer forward
					atomic.CompareAndSwapPointer(&q.tail, tail, unsafe.Pointer(newNode))

					// Signal that a new item is available
					select {
					case q.signalChan <- struct{}{}:
					default:
					}

					// Done - enqueued
					return nil
				}

			} else {

				// Tail is lagging, move it forward
				atomic.CompareAndSwapPointer(&q.tail, tail, next)

			}
		}

		// If CAS fails, retry from the beginning of the loop with updated state
	}

}

// Dequeue removes an item from the queue in a lock-free manner, with an optional timeout.  (If no
// timeout is needed, specify 0 for timeout.)  Also returns a flag if the queue has been deleted.
// Once dequeue returns the deleted flag, the caller should stop dequeueing and should no longer
// use the queue.
func (q *SafeQueue) Dequeue(timeout time.Duration) (value interface{}, timeoutOccurred bool, queueWasDeleted bool) {
	timeoutChan := make(chan struct{})

	if timeout > 0 {
		go func() {
			time.Sleep(timeout)
			close(timeoutChan)
		}()
	}

	// Retry loop for handling atomic concurrency
	for {
		head := atomic.LoadPointer(&q.head)
		tail := atomic.LoadPointer(&q.tail)
		next := atomic.LoadPointer(&((*Node)(head)).next)

		if head == atomic.LoadPointer(&q.head) {
			if head == tail {
				if next == nil {
					select {
					case <-q.signalChan:
						continue
					case <-timeoutChan:
						return nil, true, false
					}
				}
				atomic.CompareAndSwapPointer(&q.tail, tail, next)

			} else {

				value := (*Node)(next).value
				if atomic.CompareAndSwapPointer(&q.head, head, next) {
					if value == nil {
						return nil, false, true
					}
					return value, false, false
				}

			}
		}

		// If CAS fails, retry from the beginning of the loop with updated state
	}

}
