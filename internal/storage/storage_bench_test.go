package storage

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
)

// BenchmarkStorage measures the Store API, including its synchronization and
// time snapshots, without RESP parsing, network I/O, expiration, or command
// logging. SET means an unconditional Update that overwrites an existing key.
// Both backends use the same prepopulated keys and immutable 128-byte payload.
// Uniform keys cover a bounded working set; HotKey measures same-key contention.
//
// Each case starts exactly workers goroutines, independent of GOMAXPROCS. The
// operation budget is split between workers without a shared counter in the hot
// loop. Each worker has its own deterministic key generator and mix schedule.
// Fixture creation and worker startup are excluded from the measurements.
//
// Run, for example:
//
//	go test ./internal/storage -run '^$' -bench '^BenchmarkStorage$' -benchmem -benchtime=200ms -count=3
//
// Use -cpu to control GOMAXPROCS independently of the worker count. Results are
// aggregate throughput: ns/op is elapsed time / total operations across workers,
// not the latency of an individual request. DLHT measures the current adapter
// with its per-shard read/write locks, rather than the upstream map in isolation.
func BenchmarkStorage(b *testing.B) {
	b.Logf("GOMAXPROCS=%d; payload=128B; SET=overwrite; no expiration", runtime.GOMAXPROCS(0))
	backends := []struct {
		name string
		new  func() Store
	}{
		{"Xsync", func() Store { return NewXsync() }},
		{"DLHT", func() Store { return NewDLHT() }},
	}
	workloads := []struct {
		name        string
		readPercent int
	}{
		{"GET", 100},
		{"SET", 0},
		{"Mixed95R5W", 95},
		{"Mixed50R50W", 50},
	}
	keySets := []struct {
		name  string
		count int
	}{
		{"Uniform", 1 << 16},
		{"HotKey", 1},
	}
	// Run matching backend cases next to each other to reduce order-dependent
	// thermal and system-load drift across the comparison.
	for _, workload := range workloads {
		for _, keySet := range keySets {
			for _, workers := range []int{1, 2, 4, 8, 16, 32} {
				for _, backend := range backends {
					name := fmt.Sprintf("%s/%s/%s/workers=%d", backend.name, workload.name, keySet.name, workers)
					b.Run(name, func(b *testing.B) {
						benchmarkStore(b, backend.new(), keySet.count, workers, workload.readPercent)
					})
				}
			}
		}
	}
}

func benchmarkStore(b *testing.B, store Store, keyCount, workers, readPercent int) {
	b.Helper()
	b.StopTimer()
	ctx := context.Background()
	keys := make([]string, keyCount)
	payload := strings.Repeat("x", 128)
	entry := Entry{Value: payload}
	overwrite := func(Entry, bool, int64) (Entry, bool, error) {
		return entry, true, nil
	}
	for i := range keys {
		keys[i] = fmt.Sprintf("key:%016x", i)
		if err := store.Update(ctx, keys[i], overwrite); err != nil {
			b.Fatal(err)
		}
	}
	type result struct {
		err  error
		last Entry
		read bool
	}
	results := make([]result, workers)
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	ready.Add(workers)
	done.Add(workers)
	for worker := range workers {
		iterations := b.N / workers
		if worker < b.N%workers {
			iterations++
		}
		go func() {
			defer done.Done()
			var res result
			defer func() { results[worker] = res }()
			// Nonzero, independent xorshift64 streams; no shared RNG or atomic
			// counter can become the bottleneck instead of the Store itself.
			state := uint64(worker+1) * 0x9e3779b97f4a7c15
			phase := worker * 37 % 100
			ready.Done()
			<-start
			for range iterations {
				state ^= state << 13
				state ^= state >> 7
				state ^= state << 17
				key := keys[state&uint64(keyCount-1)]
				if phase < readPercent {
					var found bool
					res.last, found, res.err = store.Get(ctx, key)
					if res.err != nil {
						return
					}
					if !found {
						res.err = errors.New("prepopulated key missing")
						return
					}
					res.read = true
				} else if res.err = store.Update(ctx, key, overwrite); res.err != nil {
					return
				}
				phase++
				if phase == 100 {
					phase = 0
				}
			}
		}()
	}
	ready.Wait()
	b.ReportAllocs()
	b.ResetTimer()
	b.StartTimer()
	close(start)
	done.Wait()
	b.StopTimer()
	for _, res := range results {
		if res.err != nil {
			b.Fatal(res.err)
		}
		if res.read && res.last.Value != payload {
			b.Fatal("unexpected stored value")
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/s")
}
