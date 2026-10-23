package database

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jr-dragon/peridot/internal/storage"
)

// BenchmarkDatabaseUpdate measures the direct memory path and coordinated
// active-expiration paths, including optional TTL-index maintenance. The same
// harness can run unchanged against the striped-lock baseline.
func BenchmarkDatabaseUpdate(b *testing.B) {
	for _, mode := range []string{"direct", "coordinated", "ttl"} {
		for _, hot := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/hot=%t", mode, hot), func(b *testing.B) {
				store := storage.New()
				keys := make([]string, 1<<16)
				entry := storage.Entry{Value: strings.Repeat("x", 128)}
				if mode == "ttl" {
					entry.ExpiresAt = 4102444800000
				}
				for i := range keys {
					keys[i] = fmt.Sprint(i)
					store.Put(keys[i], entry)
				}
				var options []Option
				if mode != "direct" {
					options = append(options, WithActiveExpiration(10, false))
				}
				db := New(store, nil, options...)
				b.Cleanup(func() {
					if err := db.Close(); err != nil {
						b.Error(err)
					}
				})
				ctx := context.Background()
				update := func(storage.Entry, bool, int64) (storage.Entry, bool, error) { return entry, true, nil }
				b.ReportAllocs()
				var workers atomic.Uint64
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					state := workers.Add(1) * 0x9e3779b97f4a7c15
					for pb.Next() {
						state ^= state << 13
						state ^= state >> 7
						state ^= state << 17
						key := keys[state&uint64(len(keys)-1)]
						if hot {
							key = keys[0]
						}
						if err := db.Update(ctx, key, update); err != nil {
							b.Error(err)
						}
					}
				})
			})
		}
	}
}
