package server

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"

	"github.com/jr-dragon/peridot/internal/liberr"
	"github.com/jr-dragon/peridot/pkg/resp"
)

var (
	ErrServerClosed         = errors.New("resp: server closed")
	ErrServerAlreadyServing = errors.New("resp: server already serving")
)

type RESP struct {
	Addr string

	handler Handler

	inShutdown atomic.Bool

	lnmu sync.Mutex
	ln   net.Listener

	connsmu   sync.Mutex
	connswg   sync.WaitGroup
	conns     map[net.Conn]struct{}
	connsDone chan struct{}

	l *slog.Logger
}

func NewRESP(addr string, handler Handler) *RESP {
	return &RESP{
		Addr:    addr,
		handler: handler,

		l: slog.Default().With(slog.String("component", "resp")),
	}
}

// ListenAndServe listens for TCP connections until Shutdown or Close is called.
// ctx is used only while creating the listener; it does not control established
// connections. ListenAndServe returns ErrServerClosed after Shutdown or Close;
// callers must wait for Shutdown separately to allow active connections to drain.
func (s *RESP) ListenAndServe(ctx context.Context) error {
	if s.inShutdown.Load() {
		return ErrServerClosed
	}

	var lncfg net.ListenConfig
	ln, err := lncfg.Listen(ctx, "tcp", s.Addr)
	if err != nil {
		return err
	}
	defer func() {
		if err := ln.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			s.l.ErrorContext(ctx, "failed to close listener", liberr.Attr(err), slog.String("addr", ln.Addr().String()))
		}
	}()

	if !s.trackListener(ln) {
		if s.inShutdown.Load() {
			return ErrServerClosed
		}
		return ErrServerAlreadyServing
	}
	defer s.untrackListener(ln)
	s.l.InfoContext(ctx, "listening", slog.String("addr", ln.Addr().String()))
	defer s.l.InfoContext(ctx, "stopped accepting connections", slog.String("addr", ln.Addr().String()))

	for {
		conn, err := ln.Accept()
		if err != nil {
			if s.inShutdown.Load() {
				return ErrServerClosed
			}

			return err
		}

		if !s.trackConn(conn) {
			conn.Close()
			return ErrServerClosed
		}
	}
}

// Shutdown gracefully shuts down the server without interrupting active
// connections. It returns when all connections have closed or ctx expires.
// Context expiration only stops waiting; it does not close active connections.
// Once Shutdown has been called, the server cannot be reused.
func (s *RESP) Shutdown(ctx context.Context) error {
	s.inShutdown.Store(true)
	s.l.DebugContext(ctx, "waiting for connections to drain")

	lnerr := s.closeListener()

	done := s.waitConns()
	select {
	case <-done:
		s.l.DebugContext(ctx, "connection handlers stopped")
		return lnerr
	case <-ctx.Done():
		// Prefer a completed shutdown if both cases are ready.
		select {
		case <-done:
			s.l.DebugContext(ctx, "connection handlers stopped")
			return lnerr
		default:
			s.l.DebugContext(ctx, "connection drain interrupted", liberr.Attr(ctx.Err()))
			return ctx.Err()
		}
	}
}

// Close immediately closes the listener and all active connections. Once
// Close has been called, the Server cannot be reused.
func (s *RESP) Close() error {
	s.inShutdown.Store(true)
	s.l.Debug("closing active connections")

	lnerr := s.closeListener()

	type closeError struct {
		conn net.Conn
		err  error
	}
	var closeErrors []closeError
	defer func() {
		for _, failure := range closeErrors {
			s.l.Error("failed to close connection", liberr.Attr(failure.err),
				slog.String("local_address", failure.conn.LocalAddr().String()),
				slog.String("remote_address", failure.conn.RemoteAddr().String()),
			)
		}
	}()
	s.connsmu.Lock()
	defer s.connsmu.Unlock()
	for conn := range s.conns {
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			closeErrors = append(closeErrors, closeError{conn: conn, err: err})
		}
	}

	return lnerr
}

// waitConns is called only after inShutdown is set. Synchronizing with
// trackConn ensures no WaitGroup additions can occur after waiting starts.
func (s *RESP) waitConns() <-chan struct{} {
	s.connsmu.Lock()
	defer s.connsmu.Unlock()

	if s.connsDone == nil {
		s.connsDone = make(chan struct{})
		if len(s.conns) == 0 {
			// Handlers have finished connection cleanup; synchronize their final
			// WaitGroup bookkeeping without scheduling another goroutine.
			s.connswg.Wait()
			close(s.connsDone)
		} else {
			go func() {
				s.connswg.Wait()
				close(s.connsDone)
			}()
		}
	}
	return s.connsDone
}

func (s *RESP) trackListener(ln net.Listener) bool {
	s.lnmu.Lock()
	defer s.lnmu.Unlock()

	if s.inShutdown.Load() || s.ln != nil {
		return false
	}

	s.ln = ln
	return true
}

func (s *RESP) untrackListener(ln net.Listener) {
	s.lnmu.Lock()
	defer s.lnmu.Unlock()

	if s.ln == ln {
		s.ln = nil
	}
}

func (s *RESP) trackConn(conn net.Conn) bool {
	s.connsmu.Lock()
	defer s.connsmu.Unlock()

	if s.inShutdown.Load() {
		return false
	}

	if s.conns == nil {
		s.conns = make(map[net.Conn]struct{})
	}
	s.conns[conn] = struct{}{}
	s.connswg.Go(func() { s.serve(conn) })
	return true
}

func (s *RESP) serve(conn net.Conn) {
	l := s.l.With(
		slog.String("local_address", conn.LocalAddr().String()),
		slog.String("remote_address", conn.RemoteAddr().String()),
	)
	l.Debug("connection opened")
	defer func() {
		s.connsmu.Lock()
		defer s.connsmu.Unlock()
		delete(s.conns, conn)
	}()
	defer l.Debug("connection closed")
	defer func() {
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			l.Error("failed to close connection", liberr.Attr(err))
		}
	}()

	writer := bufio.NewWriter(conn)
	rw := bufio.NewReadWriter(bufio.NewReader(&replyFlushingReader{reader: conn, writer: writer}), writer)
	flush := func() bool {
		if err := rw.Writer.Flush(); err != nil {
			if !errors.Is(err, net.ErrClosed) {
				l.Error("failed to flush connection", liberr.Attr(err))
			}
			return false
		}
		return true
	}

	cmd := resp.NewCommand(rw.Reader)
	for {
		ret, err := s.handler.ServeRESP(context.Background(), cmd)
		if err == io.EOF {
			flush()
			return
		}
		closeConn := false
		if err != nil {
			switch {
			case errors.Is(err, ErrClientFatal):
				closeConn = true
				if ret == nil {
					ret = resp.UnknownError
				}
			case errors.Is(err, ErrClient):
				if ret == nil {
					ret = resp.UnknownError
				}
			default:
				l.Error("failed to serve", liberr.Attr(err))
				closeConn = true
				if ret == nil || !errors.Is(err, ErrServer) {
					ret = resp.InternalError
				}
			}
		} else if ret == nil {
			ret = resp.NullBulkStringValue
		}

		if _, err := rw.Writer.Write(ret.Marshal()); err != nil {
			if !errors.Is(err, net.ErrClosed) {
				l.Error("failed to write to connection", liberr.Attr(err))
			}
			return
		}

		if (closeConn || rw.Reader.Buffered() == 0) && !flush() {
			return
		}
		if closeConn {
			return
		}
	}
}

// replyFlushingReader flushes completed replies before a read can block on the
// connection. The outer bufio.Reader still allows fully buffered commands to
// share a reply batch, even when the last buffered command is incomplete.
type replyFlushingReader struct {
	reader io.Reader
	writer *bufio.Writer
}

func (r *replyFlushingReader) Read(p []byte) (int, error) {
	if err := r.writer.Flush(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

func (s *RESP) closeListener() error {
	s.lnmu.Lock()
	defer s.lnmu.Unlock()

	if s.ln == nil {
		return nil
	}

	ln := s.ln
	s.ln = nil
	return ln.Close()
}
