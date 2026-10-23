package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
)

func TestReadDoesNotWaitForUnpublishedUpdate(t *testing.T) {
	m := NewXsync()
	m.Put("key", Entry{Value: "before"})
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	updated := make(chan struct{})
	go func() {
		defer close(updated)
		m.entries.Compute("key", func(Entry, bool) (Entry, xsync.ComputeOp) {
			close(entered)
			<-release
			return Entry{Value: "after"}, xsync.UpdateOp
		})
	}()
	<-entered
	read := make(chan error, 1)
	go func() {
		read <- m.Read(context.Background(), "key", func(e Entry, exists bool, _ int64) {
			if !exists || e.Value != "before" {
				t.Error("read observed an unpublished value", e, exists)
			}
		})
	}()
	select {
	case err := <-read:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("read waited for a bucket lock")
	}
	unblock()
	<-updated
}

func TestReadRechecksExpiredReplacement(t *testing.T) {
	m := NewXsync()
	m.Put("key", Entry{Value: "expired", ExpiresAt: 999})
	loaded, resume := make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	m.now = func() time.Time {
		if calls.Add(1) == 1 {
			close(loaded)
			<-resume
		}
		return time.UnixMilli(1000)
	}
	var once sync.Once
	unblock := func() { once.Do(func() { close(resume) }) }
	t.Cleanup(unblock)
	read := make(chan error, 1)
	go func() {
		read <- m.Read(context.Background(), "key", func(e Entry, exists bool, now int64) {
			if !exists || e.Value != "replacement" || now != 1000 {
				t.Error("replacement lost", e, exists, now)
			}
		})
	}()
	<-loaded
	updated := make(chan error, 1)
	go func() {
		updated <- m.Update(context.Background(), "key", func(Entry, bool, int64) (Entry, bool, error) { return Entry{Value: "replacement"}, true, nil })
	}()
	select {
	case err := <-updated:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("expired observation held the bucket lock")
	}
	unblock()
	if err := <-read; err != nil {
		t.Fatal(err)
	}
	if e, ok := m.Load("key"); !ok || e.Value != "replacement" {
		t.Fatal(e, ok)
	}
}

func TestReadExpirationBoundaryAndCancellation(t *testing.T) {
	m := NewXsync()
	var now atomic.Int64
	now.Store(1000)
	m.now = func() time.Time { return time.UnixMilli(now.Load()) }
	m.Put("key", Entry{Value: "value", ExpiresAt: 1000})
	for _, deadline := range []int64{1000, 1001} {
		now.Store(deadline)
		calls := 0
		if err := m.Read(context.Background(), "key", func(e Entry, exists bool, snapshot int64) {
			calls++
			if snapshot != deadline || exists != (deadline == 1000) || exists && e.Value != "value" {
				t.Fatal(e, exists, snapshot)
			}
		}); err != nil || calls != 1 {
			t.Fatal(err, calls)
		}
	}
	if _, exists := m.Load("key"); exists {
		t.Fatal("expired entry retained")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := m.Read(ctx, "key", func(Entry, bool, int64) { t.Fatal("canceled callback") }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestDeleteKeysCountsDistinctLiveEntries(t *testing.T) {
	m := NewXsync()
	m.now = func() time.Time { return time.UnixMilli(1000) }
	m.Put("\x00\xff", Entry{Value: "binary"})
	m.Put("deadline", Entry{Value: "live", ExpiresAt: 1000})
	m.Put("expired", Entry{Value: "old", ExpiresAt: 999})
	keys := []string{"\x00\xff", "\x00\xff", "missing", "expired", "expired", "deadline"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if n, err := m.DeleteKeys(ctx, keys); !errors.Is(err, context.Canceled) || n != 0 || m.Len() != 3 {
		t.Fatal(n, err, m.Len())
	}
	if n, err := m.DeleteKeys(context.Background(), keys); err != nil || n != 2 || m.Len() != 0 {
		t.Fatal(n, err, m.Len())
	}
}

func TestSnapshotAfterGrowthAndDeletion(t *testing.T) {
	m := NewXsync()
	for i := range 4096 {
		m.Put(fmt.Sprintf("key\x00%d\xff", i), Entry{Value: fmt.Sprint(i), ExpiresAt: 1})
	}
	for i := range 4000 {
		m.Delete(fmt.Sprintf("key\x00%d\xff", i))
	}
	snapshot := m.Snapshot()
	if len(snapshot) != 96 {
		t.Fatalf("snapshot length = %d", len(snapshot))
	}
	for i := 4000; i < 4096; i++ {
		key := fmt.Sprintf("key\x00%d\xff", i)
		if snapshot[key] != (Entry{Value: fmt.Sprint(i), ExpiresAt: 1}) {
			t.Fatal("missing physical entry", key)
		}
		delete(snapshot, key)
	}
	if m.Len() != 96 {
		t.Fatal("snapshot map shared backend state")
	}
}
