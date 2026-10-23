package database

import (
	"context"
	"fmt"
	"testing"

	"github.com/jr-dragon/peridot/internal/storage"
)

// BenchmarkDatabaseRead includes database coordination and the active expiration
// worker, with live non-TTL values and no persistence or protocol/network I/O.
func BenchmarkDatabaseRead(b *testing.B) {
	for _, operation := range []string{"Get", "Read"} {
		for _, hot := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/hot=%t", operation, hot), func(b *testing.B) {
				store := storage.New()
				keys := make([]string, 1<<16)
				for i := range keys {
					keys[i] = fmt.Sprint(i)
					store.Put(keys[i], storage.Entry{Value: "value"})
				}
				db := New(store, nil, WithActiveExpiration(10, false))
				b.Cleanup(func() {
					if err := db.Close(); err != nil {
						b.Error(err)
					}
				})
				ctx := context.Background()
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					state := uint64(12345)
					for pb.Next() {
						state ^= state << 13
						state ^= state >> 7
						state ^= state << 17
						key := keys[state&uint64(len(keys)-1)]
						if hot {
							key = keys[0]
						}
						if operation == "Get" {
							entry, exists, err := db.Get(ctx, key)
							if err != nil || !exists || entry.Value != "value" {
								b.Error(entry, exists, err)
							}
						} else if err := db.Read(ctx, key, func(e storage.Entry, exists bool, _ int64) {
							if !exists || e.Value != "value" {
								b.Error("invalid read")
							}
						}); err != nil {
							b.Error(err)
						}
					}
				})
			})
		}
	}
}
