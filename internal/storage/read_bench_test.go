package storage

import (
	"context"
	"fmt"
	"testing"
)

// BenchmarkXsyncRead measures read-only callbacks on live immutable values.
func BenchmarkXsyncRead(b *testing.B) {
	for _, hot := range []bool{false, true} {
		b.Run(fmt.Sprintf("hot=%t", hot), func(b *testing.B) {
			m := NewXsync()
			keys := make([]string, 1<<16)
			for i := range keys {
				keys[i] = fmt.Sprint(i)
				m.Put(keys[i], Entry{Value: "value"})
			}
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
					if err := m.Read(ctx, key, func(e Entry, exists bool, _ int64) {
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

func BenchmarkXsyncSnapshot(b *testing.B) {
	m := NewXsync()
	for i := range 1 << 16 {
		m.Put(fmt.Sprint(i), Entry{Value: "value"})
	}
	var snapshot map[string]Entry
	b.ReportAllocs()
	for b.Loop() {
		snapshot = m.Snapshot()
	}
	if len(snapshot) != 1<<16 {
		b.Fatal("incomplete snapshot")
	}
}

// BenchmarkXsyncDeleteKeys includes refilling deleted keys in each cycle so
// every iteration performs the same number of successful deletions.
func BenchmarkXsyncDeleteKeys(b *testing.B) {
	for _, count := range []int{1, 32, 1024} {
		b.Run(fmt.Sprintf("keys=%d", count), func(b *testing.B) {
			m := NewXsync()
			keys := make([]string, 1<<16)
			for i := range keys {
				keys[i] = fmt.Sprint(i)
				m.Put(keys[i], Entry{Value: "value"})
			}
			ctx := context.Background()
			b.ReportAllocs()
			for b.Loop() {
				if n, err := m.DeleteKeys(ctx, keys[:count]); err != nil || n != int64(count) {
					b.Fatal(n, err)
				}
				for _, key := range keys[:count] {
					m.Put(key, Entry{Value: "value"})
				}
			}
		})
	}
}
