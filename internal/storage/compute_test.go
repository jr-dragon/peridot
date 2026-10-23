package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func TestRawComputePreservesPhysicalEntriesAndCancellation(t *testing.T) {
	m := NewXsync()
	original := Entry{Value: []int{1, 2}, ExpiresAt: 1}
	m.Put("key\x00\xff", original)
	calls := 0
	m.Compute("key\x00\xff", func(e Entry, exists bool) (Entry, ComputeOp) {
		calls++
		if !exists || e.ExpiresAt != 1 || e.Value.([]int)[1] != 2 {
			t.Fatal("raw compute hid an expired entry", e, exists)
		}
		return Entry{Value: "ignored"}, ComputeCancel
	})
	if e, exists := m.Load("key\x00\xff"); calls != 1 || !exists || e.ExpiresAt != 1 || e.Value.([]int)[1] != 2 {
		t.Fatal(calls, e, exists)
	}
	m.Compute("key\x00\xff", func(Entry, bool) (Entry, ComputeOp) { return Entry{}, ComputeDelete })
	if _, exists := m.Load("key\x00\xff"); exists {
		t.Fatal("raw delete retained the entry")
	}
	m.Compute("missing", func(e Entry, exists bool) (Entry, ComputeOp) {
		if exists {
			t.Fatal("missing key exists")
		}
		return e, ComputeCancel
	})
	if m.Len() != 0 {
		t.Fatal("cancellation inserted an entry")
	}
}

func TestRawComputeAtomicAndExactlyOnceDuringGrowth(t *testing.T) {
	m := NewXsync()
	const workers, iterations = 8, 512
	var calls atomic.Int64
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Go(func() {
			for i := range iterations {
				for _, key := range []string{"counter", fmt.Sprintf("%d:%d", worker, i)} {
					m.Compute(key, func(e Entry, exists bool) (Entry, ComputeOp) {
						calls.Add(1)
						n := 0
						if exists {
							n = e.Value.(int)
						}
						return Entry{Value: n + 1}, ComputeUpdate
					})
				}
			}
		})
	}
	wg.Wait()
	entry, exists := m.Load("counter")
	if calls.Load() != workers*iterations*2 || !exists || entry.Value != workers*iterations || m.Len() != workers*iterations+1 {
		t.Fatal(calls.Load(), entry, exists, m.Len())
	}
}
