package persistence

import (
	"errors"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/jr-dragon/peridot/internal/persistence/aof"
	"github.com/jr-dragon/peridot/internal/storage"
	"github.com/puzpuzpuz/xsync/v4"
)

// Each caller waits for completion before publishing its memory mutation. The
// queue therefore holds at most one request per in-flight persistence call.
type appendRequest struct {
	key         string
	next        storage.Entry
	keep, multi bool
	keys        []string
	done        sync.WaitGroup
	err         error
}

// Producers may hold backend bucket locks; the consumer only encodes and writes
// resolved mutations and never accesses the backend.
type appendQueue struct {
	work  *xsync.UMPSCQueue[*appendRequest]
	count atomic.Int64
	pool  sync.Pool
	done  chan struct{}
}

func (s *AOF) submit(key string, next storage.Entry, keep bool, keys []string, multi bool) error {
	s.submitMu.RLock()
	s.mu.Lock()
	if s.closed || s.closing {
		s.mu.Unlock()
		s.submitMu.RUnlock()
		return ErrAOFClosed
	}
	if multi && len(keys) == 0 {
		s.mu.Unlock()
		s.submitMu.RUnlock()
		return nil
	}
	if s.failure != nil {
		err := s.failure
		s.mu.Unlock()
		s.submitMu.RUnlock()
		return classifyAOFFailure(err)
	}
	if keep {
		if _, ok := next.Value.(string); !ok {
			s.mu.Unlock()
			s.submitMu.RUnlock()
			return errors.New("aof: unsupported value type")
		}
	}
	if s.queue == nil {
		s.queue = &appendQueue{work: xsync.NewUMPSCQueue[*appendRequest](), done: make(chan struct{})}
		s.appended = sync.NewCond(&s.mu)
		go s.consume(s.queue)
	}
	q := s.queue
	s.sequence++
	s.mu.Unlock()
	request, ok := q.pool.Get().(*appendRequest)
	if !ok {
		request = &appendRequest{}
	}
	request.key, request.next, request.keep, request.multi = key, next, keep, multi
	if multi {
		request.keys = slices.Clone(keys)
	}
	request.done.Add(1)
	// Count reservations before publication. The consumer may block on a
	// reserved slot, but it cannot miss a request when deciding to flush.
	q.count.Add(1)
	q.work.Enqueue(request)
	s.submitMu.RUnlock()
	request.done.Wait()
	err := request.err
	request.err = nil
	q.pool.Put(request)
	return classifyAOFFailure(err)
}

func (s *AOF) consume(q *appendQueue) {
	defer close(q.done)
	var batch [256]*appendRequest
	var record []byte
	for {
		first := q.work.Dequeue()
		q.count.Add(-1)
		if first == nil {
			return
		}
		s.mu.Lock()
		s.writing = true
		failure := s.failure
		s.mu.Unlock()
		n, stopping := 0, false
		request := first
		for {
			batch[n] = request
			n++
			if failure == nil {
				if request.multi {
					args := append([]string{"DEL"}, request.keys...)
					record = aof.AppendCommand(record, args...)
				} else if request.keep {
					record = aof.AppendReplacement(record, request.key, request.next.Value.(string), request.next.ExpiresAt)
				} else {
					record = aof.AppendCommand(record, "DEL", request.key)
				}
			}
			if n == len(batch) || len(record) >= 256<<10 || q.count.Load() == 0 {
				break
			}
			request = q.work.Dequeue()
			q.count.Add(-1)
			if request == nil {
				stopping = true
				break
			}
		}
		var err error
		if failure == nil {
			err = s.writeBatch(record)
		}
		s.mu.Lock()
		if err != nil {
			s.failure = errors.Join(s.failure, err)
		} else if failure == nil {
			s.committed += uint64(n)
			if s.policy != "always" {
				s.dirty = true
			}
		}
		result := s.failure
		if cap(record) <= 64<<10 && result == nil {
			record = record[:0]
		} else {
			record = nil
		}
		s.pending = record
		s.writing = false
		s.appended.Broadcast()
		s.mu.Unlock()
		for i := range n {
			request := batch[i]
			batch[i] = nil
			// UMPSC segments retain request pointers until reuse. Clear payloads
			// before signaling so neither the queue nor the pool pins large values.
			request.key, request.next, request.keys = "", storage.Entry{}, nil
			request.err = result
			request.done.Done()
		}
		if stopping {
			return
		}
	}
}
