package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jr-dragon/peridot/internal/service/cmd"
	"github.com/jr-dragon/peridot/internal/storage"
	"github.com/jr-dragon/peridot/pkg/resp"
)

func newTestHandler() *HandlerMock {
	return &HandlerMock{
		ServeRESPFunc: func(_ context.Context, rd *resp.Command) (resp.Value, error) {
			args, err := rd.Read()
			if err != nil {
				return nil, err
			}
			return args[0], nil
		},
	}
}

type runningRESP struct {
	server *RESP
	addr   string
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

func startRESP(t *testing.T) *runningRESP {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := &runningRESP{
		server: NewRESP("127.0.0.1:0", newTestHandler()),
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go func() {
		r.err = r.server.ListenAndServe(ctx)
		close(r.done)
	}()
	t.Cleanup(func() {
		cancel()
		r.server.Close()
		select {
		case <-r.done:
		case <-time.After(3 * time.Second):
			t.Error("listener did not stop")
		}
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancelShutdown()
		if err := r.server.Shutdown(shutdownCtx); err != nil {
			t.Errorf("connection handlers did not stop: %v", err)
		}
	})
	waitFor(t, func() bool {
		select {
		case <-r.done:
			t.Fatalf("server failed during startup: %v", r.err)
		default:
		}
		r.server.lnmu.Lock()
		defer r.server.lnmu.Unlock()
		if r.server.ln == nil {
			return false
		}
		r.addr = r.server.ln.Addr().String()
		return true
	})
	return r
}

func waitFor(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not become ready")
		}
		time.Sleep(time.Millisecond)
	}
}

func (r *runningRESP) wait(t *testing.T) error {
	t.Helper()
	select {
	case <-r.done:
		return r.err
	case <-time.After(3 * time.Second):
		t.Fatal("ListenAndServe did not return")
	}
	return nil
}

func (r *runningRESP) dial(t *testing.T) *net.TCPConn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", r.addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	echo(t, conn)
	return conn.(*net.TCPConn)
}

func echo(t *testing.T, conn net.Conn) {
	t.Helper()
	if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(conn, "*1\r\n$4\r\nping\r\n"); err != nil {
		t.Fatal(err)
	}
	var reply [10]byte
	if _, err := io.ReadFull(conn, reply[:]); err != nil {
		t.Fatal(err)
	}
	if string(reply[:]) != "$4\r\nping\r\n" {
		t.Fatalf("unexpected reply: %q", reply)
	}
}

func TestShutdownDrainsConnections(t *testing.T) {
	r := startRESP(t)
	conn := r.dial(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.server.Shutdown(ctx) }()
	if err := r.wait(t); !errors.Is(err, ErrServerClosed) {
		t.Fatalf("expected listener shutdown sentinel, got %v", err)
	}
	if probe, err := net.DialTimeout("tcp", r.addr, time.Second); err == nil {
		probe.Close()
		t.Fatal("listener accepted a connection during shutdown")
	}
	echo(t, conn)
	select {
	case err := <-done:
		t.Fatalf("Shutdown returned before the connection drained: %v", err)
	default:
	}
	if err := conn.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(conn); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("graceful shutdown: %v", err)
	}
}

func TestCloseInterruptsConnections(t *testing.T) {
	r := startRESP(t)
	conn := r.dial(t)
	if err := r.server.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.server.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.wait(t); !errors.Is(err, ErrServerClosed) {
		t.Fatal(err)
	}
	var reply [1]byte
	if _, err := conn.Read(reply[:]); !errors.Is(err, io.EOF) {
		t.Fatalf("connection was not closed: %v", err)
	}
}

func TestShutdownTimeoutOnlyStopsWaiting(t *testing.T) {
	r := startRESP(t)
	conn := r.dial(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := r.server.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected shutdown timeout, got %v", err)
	}
	if err := r.wait(t); !errors.Is(err, ErrServerClosed) {
		t.Fatal(err)
	}
	echo(t, conn)
	conn.Close()
	if err := r.server.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.server.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.server.Shutdown(ctx); err != nil {
		t.Fatalf("completed shutdown: %v", err)
	}
}

func TestCloseAndShutdownAreConcurrentAndIdempotent(t *testing.T) {
	r := startRESP(t)
	r.dial(t)
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Go(func() {
			var err error
			if i%2 == 0 {
				err = r.server.Close()
			} else {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				err = r.server.Shutdown(ctx)
			}
			if err != nil {
				t.Errorf("concurrent close/shutdown: %v", err)
			}
		})
	}
	wg.Wait()
	if err := r.wait(t); !errors.Is(err, ErrServerClosed) {
		t.Fatal(err)
	}
	if err := r.server.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestListenAndServeRejectsSecondStart(t *testing.T) {
	r := startRESP(t)
	if err := r.server.ListenAndServe(context.Background()); !errors.Is(err, ErrServerAlreadyServing) {
		t.Fatalf("expected already-serving sentinel, got %v", err)
	}
	echo(t, r.dial(t))
}

func TestShutdownWithoutServingPrefersCompletion(t *testing.T) {
	s := NewRESP("", newTestHandler())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.ListenAndServe(context.Background()); !errors.Is(err, ErrServerClosed) {
		t.Fatal(err)
	}
}

type blockingCloseConn struct {
	net.Conn
	closing chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *blockingCloseConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() { close(c.closing) })
	<-c.release
	return err
}

func TestShutdownWaitsForConnectionCleanup(t *testing.T) {
	s := NewRESP("", newTestHandler())
	conn, peer := net.Pipe()
	c := &blockingCloseConn{
		Conn: conn, closing: make(chan struct{}), release: make(chan struct{}),
	}
	defer close(c.release)
	t.Cleanup(func() {
		peer.Close()
		s.Close()
		s.connswg.Wait()
	})
	if !s.trackConn(c) {
		t.Fatal("connection was rejected")
	}
	peer.Close()
	select {
	case <-c.closing:
	case <-time.After(time.Second):
		t.Fatal("handler did not begin connection cleanup")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() { done <- s.Shutdown(ctx) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("shutdown completed before connection cleanup: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Shutdown ignored cancellation during connection cleanup")
	}
}

func TestServingContextCancellationDoesNotCloseConnections(t *testing.T) {
	r := startRESP(t)
	r.cancel()
	echo(t, r.dial(t))
	select {
	case <-r.done:
		t.Fatal("serving context cancellation stopped the listener")
	default:
	}
	if err := r.server.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.wait(t); !errors.Is(err, ErrServerClosed) {
		t.Fatal(err)
	}
}

func TestListenAndServeStartupFailure(t *testing.T) {
	s := NewRESP("127.0.0.1:invalid-port", newTestHandler())
	if err := s.ListenAndServe(context.Background()); err == nil {
		t.Fatal("expected listen failure")
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestShutdownRacesWithStartup(t *testing.T) {
	for range 30 {
		s := NewRESP("127.0.0.1:0", newTestHandler())
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- s.ListenAndServe(ctx) }()
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, ErrServerClosed) && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("startup shutdown did not complete")
		}
	}
}

func TestRESPClosesOversizedUnterminatedHeaders(t *testing.T) {
	const limit = 64 << 10
	for _, tc := range []struct {
		name   string
		prefix string
		reason string
	}{
		{name: "inline", prefix: "PING ", reason: "too big inline request"},
		{name: "array header", prefix: "*", reason: "too big mbulk count string"},
		{name: "bulk header", prefix: "*1\r\n$", reason: "too big bulk count string"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, peer := startRESPPipe(t, NewHandler(cmd.NewCommands(storage.NewXsync(), nil)))
			headerStart := strings.LastIndex(tc.prefix, "\n") + 1
			wire := tc.prefix + strings.Repeat("x", limit+1-(len(tc.prefix)-headerStart))
			// Keep the connection open without sending a newline. The server
			// must send a protocol error and close without further client input.
			if _, err := io.WriteString(peer, wire); err != nil {
				t.Fatal(err)
			}
			reply, err := io.ReadAll(peer)
			if err != nil {
				t.Fatalf("server did not close after oversized header: %v", err)
			}
			if string(reply) != "-ERR Protocol error: "+tc.reason+"\r\n" {
				t.Fatalf("reply = %q, want oversized-header error", reply)
			}
		})
	}
}

func TestRESPFlushesRepliesBeforeReadingPartialCommand(t *testing.T) {
	for _, tc := range []struct {
		name   string
		prefix string
		tail   string
	}{
		{name: "inline", prefix: "PI", tail: "NG\r\n"},
		{name: "array header", prefix: "*", tail: "1\r\n$4\r\nPING\r\n"},
		{name: "bulk header", prefix: "*1\r\n$", tail: "4\r\nPING\r\n"},
		{name: "bulk body", prefix: "*1\r\n$4\r\nPI", tail: "NG\r\n"},
		{name: "bulk terminator", prefix: "*1\r\n$4\r\nPING\r", tail: "\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, peer := startRESPPipe(t, NewHandler(cmd.NewCommands(storage.NewXsync(), nil)))
			// One pipe write guarantees that the partial command is buffered
			// together with the completed requests.
			if _, err := io.WriteString(peer, "PING\r\nPING one two\r\n"+tc.prefix); err != nil {
				t.Fatal(err)
			}
			want := "+PONG\r\n-ERR wrong number of arguments for 'ping' command\r\n"
			reply := make([]byte, len(want))
			if _, err := io.ReadFull(peer, reply); err != nil {
				t.Fatalf("completed replies blocked by partial command: %v", err)
			}
			if string(reply) != want {
				t.Fatalf("reply = %q, want %q", reply, want)
			}

			// Only finish the next request after receiving the earlier replies.
			if _, err := io.WriteString(peer, tc.tail); err != nil {
				t.Fatal(err)
			}
			pong := make([]byte, len("+PONG\r\n"))
			if _, err := io.ReadFull(peer, pong); err != nil || string(pong) != "+PONG\r\n" {
				t.Fatalf("completed partial command reply = %q, %v", pong, err)
			}
		})
	}
}

func TestRESPUsesHandlerMock(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		want      string
		closeConn bool
		nilReply  bool
	}{
		{name: "pipelined replies", want: "+MOCK\r\n+MOCK\r\n"},
		{name: "client error continues", err: fmt.Errorf("%w: command failed", ErrClient), want: "+MOCK\r\n+MOCK\r\n"},
		{name: "fatal client error closes", err: fmt.Errorf("%w: invalid frame", ErrClientFatal), want: "+MOCK\r\n", closeConn: true},
		{name: "server error closes", err: fmt.Errorf("%w: execution failed", ErrServer), want: "+MOCK\r\n", closeConn: true},
		{name: "server EOF replies", err: fmt.Errorf("storage: %w", fmt.Errorf("%w: %w", ErrServer, io.EOF)), want: "+MOCK\r\n", closeConn: true},
		{name: "unclassified failure uses safe reply", err: errors.New("unclassified failure"), want: "-ERR internal server error\r\n", closeConn: true},
		{name: "nil success reply", want: "$-1\r\n$-1\r\n", nilReply: true},
		{name: "nil client reply", err: ErrClient, want: "-ERR unknown error\r\n-ERR unknown error\r\n", nilReply: true},
		{name: "nil fatal client reply", err: ErrClientFatal, want: "-ERR unknown error\r\n", closeConn: true, nilReply: true},
		{name: "nil server reply", err: ErrServer, want: "-ERR internal server error\r\n", closeConn: true, nilReply: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := &HandlerMock{
				ServeRESPFunc: func(ctx context.Context, rd *resp.Command) (resp.Value, error) {
					if ctx == nil {
						t.Error("handler context is nil")
					}
					args, err := rd.Read()
					if err != nil {
						return nil, err
					}
					if len(args) != 1 || args[0].String() != "PING" {
						t.Errorf("unexpected command arguments: %v", args)
					}
					if tc.nilReply {
						return nil, tc.err
					}
					return resp.SimpleString("MOCK"), tc.err
				},
			}
			s := NewRESP("", mock)
			conn, peer := net.Pipe()
			done := make(chan struct{})
			go func() { s.serve(conn); close(done) }()
			t.Cleanup(func() {
				peer.Close()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("connection handler did not stop")
				}
			})
			if err := peer.SetDeadline(time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			if _, err := io.WriteString(peer, strings.Repeat("*1\r\n$4\r\nPING\r\n", 2)); err != nil {
				t.Fatal(err)
			}
			reply := make([]byte, len(tc.want))
			if _, err := io.ReadFull(peer, reply); err != nil {
				t.Fatal(err)
			}
			if string(reply) != tc.want {
				t.Fatalf("reply = %q, want %q", reply, tc.want)
			}
			if tc.closeConn {
				if _, err := peer.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
					t.Fatalf("fatal error did not close the connection: %v", err)
				}
			}
			peer.Close()
			<-done
			wantCalls := 3 // Two requests followed by EOF.
			if tc.closeConn {
				wantCalls = 1
			}
			if calls := mock.ServeRESPCalls(); len(calls) != wantCalls {
				t.Fatalf("ServeRESP called %d times, want %d", len(calls), wantCalls)
			} else if wantCalls > 1 && calls[0].Rd != calls[1].Rd {
				t.Error("server did not reuse its command reader")
			}
		})
	}
}

func startRESPPipe(t *testing.T, h Handler) (*RESP, net.Conn) {
	t.Helper()
	s := NewRESP("", h)
	conn, peer := net.Pipe()
	if err := peer.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if !s.trackConn(conn) {
		t.Fatal("connection was rejected")
	}
	t.Cleanup(func() {
		peer.Close()
		s.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := s.Shutdown(ctx); err != nil {
			t.Errorf("connection handler did not stop: %v", err)
		}
	})
	return s, peer
}

func TestRESPCompletesErrorRepliesDuringShutdown(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		want      string
		closeConn bool
	}{
		{name: "command error", err: cmd.NewUnknownCommandError([]resp.BulkString{resp.NewBulkString("TEST")}), want: "-ERR unknown command 'TEST'\r\n"},
		{name: "fatal client error", err: cmd.NewError(ErrClientFatal, "ERR", "unknown error"), want: "-ERR unknown error\r\n", closeConn: true},
		{name: "server error", err: errors.New("execution failed"), want: "-ERR internal server error\r\n", closeConn: true},
		{name: "execution EOF", err: io.EOF, want: "-ERR internal server error\r\n", closeConn: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			first := true
			h := NewHandler(cmd.NewCommands(storage.NewXsync(), nil), func(next HandlerFunc) HandlerFunc {
				return func(ctx context.Context, args []resp.BulkString) (resp.Value, error) {
					if first {
						first = false
						close(entered)
						<-release
						return nil, tc.err
					}
					return next(ctx, args)
				}
			})
			s, peer := startRESPPipe(t, h)
			t.Cleanup(unblock)
			if _, err := io.WriteString(peer, "PING\r\n"); err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("command did not begin execution")
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if err := s.Shutdown(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("Shutdown() = %v, want cancellation while a command is active", err)
			}
			unblock()
			reply := make([]byte, len(tc.want))
			if _, err := io.ReadFull(peer, reply); err != nil || string(reply) != tc.want {
				t.Fatalf("reply during shutdown = %q, %v, want %q", reply, err, tc.want)
			}
			if tc.closeConn {
				if _, err := peer.Read(make([]byte, 1)); err != io.EOF {
					t.Fatalf("fatal error did not close the connection: %v", err)
				}
			} else {
				if _, err := io.WriteString(peer, "PING\r\n"); err != nil {
					t.Fatal(err)
				}
				pong := make([]byte, len("+PONG\r\n"))
				if _, err := io.ReadFull(peer, pong); err != nil || string(pong) != "+PONG\r\n" {
					t.Fatalf("connection did not remain usable: %q, %v", pong, err)
				}
			}
			peer.Close()
			shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), time.Second)
			defer cancelShutdown()
			if err := s.Shutdown(shutdownCtx); err != nil {
				t.Fatalf("completed shutdown: %v", err)
			}
		})
	}
}

type failingReadConn struct {
	net.Conn
	rd io.Reader
}

func (c *failingReadConn) Read(p []byte) (int, error) { return c.rd.Read(p) }

func TestRESPReadFailureSendsServerErrorAndCloses(t *testing.T) {
	for _, prefix := range []string{"", "PING\r\n*1\r\n"} {
		t.Run(fmt.Sprintf("prefix=%q", prefix), func(t *testing.T) {
			s := NewRESP("", NewHandler(cmd.NewCommands(storage.NewXsync(), nil)))
			conn, peer := net.Pipe()
			defer peer.Close()
			peer.SetDeadline(time.Now().Add(time.Second))
			c := &failingReadConn{
				Conn: conn,
				rd:   io.MultiReader(strings.NewReader(prefix), failingCommandReader{err: errors.New("read failed")}),
			}
			if !s.trackConn(c) {
				t.Fatal("connection was rejected")
			}
			defer s.Close()
			wire, err := io.ReadAll(peer)
			if err != nil {
				t.Fatal(err)
			}
			want := "-ERR internal server error\r\n"
			if prefix != "" {
				want = "+PONG\r\n" + want
			}
			if string(wire) != want {
				t.Fatalf("reply = %q, want completed replies followed by a server error %q", wire, want)
			}
			s.connswg.Wait()
		})
	}
}

type failingWriteConn struct {
	net.Conn
	writes int
}

func (c *failingWriteConn) Write([]byte) (int, error) {
	c.writes++
	return 0, errors.New("write failed")
}

func TestRESPStopsAfterReplyWriteFailure(t *testing.T) {
	for _, tc := range []struct {
		name      string
		value     resp.Value
		err       error
		request   string
		wantCalls int
	}{
		{name: "normal reply flush", value: resp.SimpleString("OK")},
		{name: "flush before partial request read", value: resp.SimpleString("OK"), request: "PING\r\n*", wantCalls: 2},
		{name: "fatal reply flush", value: resp.SimpleString("ERROR"), err: ErrClientFatal},
		{name: "fatal reply direct write", value: resp.SimpleString(strings.Repeat("x", 8192)), err: ErrServer},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := &HandlerMock{ServeRESPFunc: func(_ context.Context, rd *resp.Command) (resp.Value, error) {
				if _, err := rd.Read(); err != nil {
					return nil, err
				}
				return tc.value, tc.err
			}}
			s := NewRESP("", mock)
			conn, peer := net.Pipe()
			defer peer.Close()
			peer.SetDeadline(time.Now().Add(time.Second))
			c := &failingWriteConn{Conn: conn}
			if !s.trackConn(c) {
				t.Fatal("connection was rejected")
			}
			defer s.Close()
			request := tc.request
			if request == "" {
				request = "PING\r\n"
			}
			if _, err := io.WriteString(peer, request); err != nil {
				t.Fatal(err)
			}
			if wire, err := io.ReadAll(peer); err != nil || len(wire) != 0 {
				t.Fatalf("failed reply = %q, %v, want closed connection", wire, err)
			}
			s.connswg.Wait()
			wantCalls := tc.wantCalls
			if wantCalls == 0 {
				wantCalls = 1
			}
			if c.writes != 1 || len(mock.ServeRESPCalls()) != wantCalls {
				t.Fatalf("write failure retried work: writes=%d, handler calls=%d", c.writes, len(mock.ServeRESPCalls()))
			}
		})
	}
}
