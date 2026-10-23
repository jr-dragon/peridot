package database

import (
	"errors"
	"log/slog"
	"math"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/jr-dragon/peridot/internal/liberr"
	"github.com/jr-dragon/peridot/internal/storage"
)

const (
	expirationSampleSize = 20
	expirationBudget     = 25 * time.Millisecond
)

// mu protects the index during concurrent key operations. The database gate
// excludes those operations while an expiration cycle samples and deletes keys.
// The dense key slice supports uniform bounded sampling without scanning live
// or persistent keys. Each index update takes constant time.
type activeExpiration struct {
	mu         sync.Mutex
	baseline   float64
	dynamic    bool
	keys       []string
	positions  map[string]int
	stop, done chan struct{}
}

// WithActiveExpiration enables a worker owned by DB.Close. Without this option,
// embedded databases retain their passive-only expiration behavior. A zero hz
// uses the default baseline; configuration parsing already clamps explicit zero.
func WithActiveExpiration(hz int, dynamic bool) Option {
	if hz == 0 {
		hz = 10
	}
	hz = max(1, min(500, hz))
	return func(d *DB) {
		d.expiration = &activeExpiration{
			baseline: float64(hz), dynamic: dynamic,
			positions: make(map[string]int), stop: make(chan struct{}), done: make(chan struct{}),
		}
	}
}

func (s *DB) append(key string, entry storage.Entry, keep bool) error {
	if s.persistence != nil {
		return s.persistence.Append(key, entry, keep)
	}
	return nil
}

func (s *DB) delete(key string) {
	s.store.Delete(key)
	s.trackExpiration(key, storage.Entry{})
}

// Expiration is a read-side effect, not a user write. AOF failures continue to
// block mutations through Persistence.Err, but must not keep expired keys live
// or turn reads into connection errors. Their persisted deadlines still prevent
// resurrection on recovery, even when the DEL cannot be appended.
func (s *DB) expireKey(key string) error {
	if err := s.append(key, storage.Entry{}, false); err != nil && !errors.Is(err, ErrAOFFailed) {
		return err
	}
	s.delete(key)
	s.dirty.Add(1)
	return nil
}

// expireComputed prepares expiration inside a raw Compute callback. The caller
// returns ComputeDelete to publish removal without reentering the bucket lock.
// The database gate prevents an expiration sample or snapshot from observing
// the index change before the physical deletion is published.
func (s *DB) expireComputed(key string) error {
	if err := s.append(key, storage.Entry{}, false); err != nil && !errors.Is(err, ErrAOFFailed) {
		return err
	}
	s.trackExpiration(key, storage.Entry{})
	s.dirty.Add(1)
	return nil
}

func (s *DB) trackExpiration(key string, entry storage.Entry) {
	e := s.expiration
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	position, exists := e.positions[key]
	if entry.ExpiresAt != 0 {
		if !exists {
			e.positions[key] = len(e.keys)
			e.keys = append(e.keys, key)
		}
		return
	}
	if exists {
		last := len(e.keys) - 1
		e.keys[position] = e.keys[last]
		e.positions[e.keys[position]] = position
		e.keys[last] = ""
		e.keys = e.keys[:last]
		delete(e.positions, key)
	}
}

// sample performs a partial Fisher-Yates shuffle. Samples contain no duplicate
// keys; copying the small prefix lets deletion swap entries in the dense index.
func (e *activeExpiration) sample() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := min(expirationSampleSize, len(e.keys))
	for i := range n {
		j := i + rand.IntN(len(e.keys)-i)
		e.keys[i], e.keys[j] = e.keys[j], e.keys[i]
		e.positions[e.keys[i]], e.positions[e.keys[j]] = i, j
	}
	return slices.Clone(e.keys[:n])
}

// expirationHZ increases with TTL population and decreases with absolute median
// deadline distance. 1,000 keys and one second are the half-saturation scales.
// Floating point Hz preserves a 0.5 Hz lower bound even at a 1 Hz baseline.
func expirationHZ(baseline float64, count int, distanceMillis float64) float64 {
	population := float64(count) / (float64(count) + 1000)
	proximity := 1000 / (1000 + math.Abs(distanceMillis))
	return max(baseline/2, min(baseline*2, baseline*(0.5+1.5*population*proximity)))
}

func medianDistance(deadlines []int64, now int64) float64 {
	slices.Sort(deadlines)
	i := len(deadlines) / 2
	median := float64(deadlines[i])
	if len(deadlines)%2 == 0 {
		median = float64(deadlines[i-1])/2 + median/2
	}
	// Convert before subtracting to avoid overflowing valid int64 timestamps.
	return math.Abs(median - float64(now))
}

type expirationSample struct {
	checked, deleted, remaining int
	distance                    float64
	stopped                     bool
}

// expireSample never waits for the database gate: a busy writer or checkpoint
// must not consume an unbounded part of the cleanup budget. Once acquired, the
// gate orders the current expiration check, durable DEL, and index removal.
func (s *DB) expireSample(deadline time.Time) (result expirationSample, err error) {
	if !s.mu.TryLock() {
		result.stopped = true
		return
	}
	defer s.mu.Unlock()
	defer func() { result.remaining = len(s.expiration.keys) }()
	if s.closed || !time.Now().Before(deadline) {
		result.stopped = true
		return
	}
	keys := s.expiration.sample()
	var deadlines [expirationSampleSize]int64
	for _, key := range keys {
		select {
		case <-s.expiration.stop:
			result.stopped = true
			return
		default:
		}
		if !time.Now().Before(deadline) {
			result.stopped = true
			return
		}
		entry, exists := s.store.Load(key)
		if !exists || entry.ExpiresAt == 0 {
			// Defensive repair; all supported DB mutations maintain the index.
			s.trackExpiration(key, storage.Entry{})
			continue
		}
		deadlines[result.checked] = entry.ExpiresAt
		result.checked++
		if expired(entry, s.now().UnixMilli()) {
			if !time.Now().Before(deadline) {
				result.stopped = true
				return
			}
			// Append can include synchronous disk I/O and cannot be preempted.
			if err = s.expireKey(key); err != nil {
				return
			}
			result.deleted++
		}
	}
	if result.checked > 0 {
		result.distance = medianDistance(deadlines[:result.checked], s.now().UnixMilli())
	}
	return
}

func (s *DB) expireCycle(hz float64) (float64, error) {
	deadline := time.Now().Add(expirationBudget)
	for {
		result, err := s.expireSample(deadline)
		if err != nil || result.stopped {
			return hz, err
		}
		if s.expiration.dynamic {
			hz = expirationHZ(s.expiration.baseline, result.remaining, result.distance)
		}
		// Exactly 25% stops too: only a strictly greater ratio repeats.
		if result.checked == 0 || result.deleted*4 <= result.checked || result.remaining == 0 {
			return hz, nil
		}
	}
}

func (s *DB) expirationLoop() {
	e := s.expiration
	defer close(e.done)
	hz := e.baseline
	timer := time.NewTimer(time.Duration(float64(time.Second) / hz))
	defer timer.Stop()
	failed := false
	for {
		select {
		case <-e.stop:
			return
		case <-timer.C:
			started := time.Now()
			var err error
			hz, err = s.expireCycle(hz)
			if err != nil && !failed {
				slog.Error("active expiration failed", slog.String("component", "database"), liberr.Attr(err))
			}
			failed = err != nil
			// Account for work time instead of adding it to every interval. Slow
			// cycles never overlap or queue catch-up runs; yield at least 1ms.
			interval := time.Duration(float64(time.Second) / hz)
			timer.Reset(max(time.Millisecond, interval-time.Since(started)))
		}
	}
}
