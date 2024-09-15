// Copyright 2024 Blues Inc.  All rights reserved.
// Use of this source code is governed by licenses granted by the
// copyright holder including that found in the LICENSE file.
//
// cheat sheet: in order to just run these tests, do:
// go test -run ^TestSafeQueue

package safequeue

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestSafeQueue_EnqueueDequeue(t *testing.T) {
	q := NewSafeQueue()

	// Enqueue items
	if err := q.Enqueue(1); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if err := q.Enqueue(2); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Dequeue items
	val, timedOut, deleted := q.Dequeue(0)
	if deleted || timedOut || val != 1 {
		t.Fatalf("expected 1, got %v", val)
	}

	val, timedOut, deleted = q.Dequeue(0)
	if deleted || timedOut || val != 2 {
		t.Fatalf("expected 2, got %v", val)
	}
}

func TestSafeQueue_Deletion(t *testing.T) {
	q := NewSafeQueue()

	// Enqueue some items
	if err := q.Enqueue(1); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Delete the queue
	q.DeleteQueue()

	// First dequeue should get the item
	_, timedOut, deleted := q.Dequeue(0)
	if deleted {
		t.Fatalf("expected non-deleted queue entry, got deleted")
	}
	if timedOut {
		t.Fatalf("expected non-deleted queue entry, got timeout")
	}

	// Dequeue should indicate that the queue is deleted (via nil value)
	_, timedOut, deleted = q.Dequeue(0)
	if !deleted || timedOut {
		t.Fatalf("expected deleted queue, got non-deleted")
	}

}

// TestSafeQueue_BlockEnqueueAfterDeletion ensures that no enqueues are allowed
// after the queue has been deleted.
func TestSafeQueue_BlockEnqueueAfterDeletion(t *testing.T) {
	q := NewSafeQueue()

	// Enqueue a few items
	if err := q.Enqueue(1); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if err := q.Enqueue(2); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Delete the queue
	q.DeleteQueue()

	// Attempt to dequeue all existing items
	val, _, deleted := q.Dequeue(0)
	if deleted {
		t.Fatalf("expected to dequeue 1, but queue was deleted prematurely")
	}
	if val != 1 {
		t.Fatalf("expected 1, got %v", val)
	}

	val, _, deleted = q.Dequeue(0)
	if deleted {
		t.Fatalf("expected to dequeue 2, but queue was deleted prematurely")
	}
	if val != 2 {
		t.Fatalf("expected 2, got %v", val)
	}

	// The queue should now be deleted, attempt to dequeue should return "deleted"
	_, _, deleted = q.Dequeue(0)
	if !deleted {
		t.Fatalf("expected the queue to be deleted, but it was not")
	}

	// Attempt to enqueue after deletion should fail
	err := q.Enqueue(3)
	if err == nil {
		t.Fatalf("expected an error on enqueue after deletion, but got no error")
	}
}

func TestSafeQueue_Timeout(t *testing.T) {
	q := NewSafeQueue()

	// Create a channel to capture errors
	errCh := make(chan error, 1)

	// Start a goroutine to dequeue with a timeout
	go func() {
		val, timedOut, deleted := q.Dequeue(100 * time.Millisecond)
		if !timedOut || deleted || val != nil {
			errCh <- fmt.Errorf("expected timeout, got %v, %v, %v", val, timedOut, deleted)
		}
		close(errCh)
	}()

	// Wait for the goroutine to complete and check for errors
	if err := <-errCh; err != nil {
		t.Fatal(err) // This safely calls Fatal in the main test goroutine
	}
}

func TestSafeQueue_EnqueueAfterTimeout(t *testing.T) {
	q := NewSafeQueue()

	// Create a channel to capture errors
	errCh := make(chan error, 1)

	// Start a goroutine to dequeue with a long timeout
	go func() {
		val, timedOut, deleted := q.Dequeue(500 * time.Millisecond)
		if timedOut || deleted || val != 1 {
			errCh <- fmt.Errorf("expected 1, got %v, %v, %v", val, timedOut, deleted)
		}
		close(errCh)
	}()

	time.Sleep(100 * time.Millisecond) // Wait a bit to ensure the goroutine is blocking
	if err := q.Enqueue(1); err != nil {
		t.Fatalf("expected no error on enqueue, got %v", err)
	}

	// Wait for the goroutine to complete and check for errors
	if err := <-errCh; err != nil {
		t.Fatal(err) // Safely call Fatal in the main test goroutine
	}
}

// TestSafeQueue_HighConcurrencyStressTest runs a high concurrency stress test where multiple
// goroutines enqueue and dequeue items concurrently for a configurable duration.
func TestSafeQueue_HighConcurrencyStressTest(t *testing.T) {
	const numGoroutines = 1000            // Number of goroutines for enqueuing and dequeuing
	const testDuration = 10 * time.Second // How long the test should run
	const maxValue = 10000                // Max value to enqueue and dequeue

	q := NewSafeQueue() // Create the queue

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2) // Enqueue and Dequeue goroutines

	// This map will track the counts of items enqueued and dequeued
	enqueueCount := make(map[int]int)
	dequeueCount := make(map[int]int)
	var mapMutex sync.Mutex // To protect access to the map

	// Set the end time of the test
	endTime := time.Now().Add(testDuration)

	// Start enqueue goroutines
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for {
				// Check if the test duration is over
				if time.Now().After(endTime) {
					break
				}

				// Generate a random value to enqueue
				value := rand.Intn(maxValue)

				// Enqueue the value
				if err := q.Enqueue(value); err != nil {
					t.Errorf("failed to enqueue: %v", err)
					return
				}

				// Safely update the enqueue count
				mapMutex.Lock()
				enqueueCount[value]++
				mapMutex.Unlock()

				// Sleep for a tiny amount of time to simulate realistic workloads
				time.Sleep(time.Microsecond * time.Duration(rand.Intn(10)))
			}
		}()
	}

	// Start dequeue goroutines
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for {
				// Check if the test duration is over
				if time.Now().After(endTime) {
					break
				}

				// Try to dequeue a value
				val, _, _ := q.Dequeue(10 * time.Millisecond)

				// If we successfully dequeue a value, update the count
				if val != nil {
					value := val.(int)

					// Safely update the dequeue count
					mapMutex.Lock()
					dequeueCount[value]++
					mapMutex.Unlock()
				}
			}
		}()
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Verify that enqueue and dequeue counts match
	for i := 0; i < maxValue; i++ {
		if enqueueCount[i] != dequeueCount[i] {
			t.Errorf("mismatch between enqueues and dequeues for value %d: enqueued %d times, dequeued %d times",
				i, enqueueCount[i], dequeueCount[i])
		}
	}
}
