package main

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func TestAppRunContextCancellationDrainsRESP(t *testing.T) {
	testAppShutdown(t, false, nil)
}

func TestAppRunContextCancellationForcesRESPClose(t *testing.T) {
	testAppShutdown(t, true, nil)
}

func TestAppRunHTTPClosePreservesRESPShutdownTimeout(t *testing.T) {
	testAppShutdown(t, true, func(app *App) error { return app.httpsrv.Close() })
}

func testAppShutdown(t *testing.T, forceClose bool, stopServer func(*App) error) {
	t.Helper()
	// Reserve an address because RESP keeps its listener private.
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := reservation.Addr().String()
	reservation.Close()

	app := NewApp()
	app.respsrv.Addr = addr
	app.ShutdownTimeout = time.Second
	if forceClose {
		app.ShutdownTimeout = 30 * time.Millisecond
	}
	app.httpsrv.Addr = "127.0.0.1:0"
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var runErr error
	go func() { runErr = app.Run(ctx); close(done) }()
	t.Cleanup(func() {
		cancel()
		app.respsrv.Close()
		app.httpsrv.Close()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("App did not finish cleanup")
		}
	})

	var conn net.Conn
	deadline := time.Now().Add(3 * time.Second)
	for {
		conn, err = net.DialTimeout("tcp", addr, 20*time.Millisecond)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("RESP did not start: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(conn, "PING\r\n"); err != nil {
		t.Fatal(err)
	}
	var reply [7]byte
	if _, err := io.ReadFull(conn, reply[:]); err != nil {
		t.Fatal(err)
	}
	if string(reply[:]) != "+PONG\r\n" {
		t.Fatalf("unexpected PING reply: %q", reply)
	}

	if stopServer != nil {
		if err := stopServer(app); err != nil {
			t.Fatal(err)
		}
	} else {
		cancel()
	}
	if forceClose {
		select {
		case <-done:
			if !errors.Is(runErr, context.DeadlineExceeded) {
				t.Fatalf("shutdown timeout was lost: %v", runErr)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("App did not return after shutdown timeout")
		}
		if _, err := conn.Read(reply[:]); !errors.Is(err, io.EOF) {
			t.Fatalf("RESP connection was not closed: %v", err)
		}
		return
	}
	// An active stream must survive signal-context cancellation until it drains.
	if _, err := io.WriteString(conn, "PING hello\r\n"); err != nil {
		t.Fatal(err)
	}
	if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	remaining, err := io.ReadAll(conn)
	if err != nil {
		t.Fatal(err)
	}
	if string(remaining) != "$5\r\nhello\r\n" {
		t.Fatalf("lost response during drain: %q", remaining)
	}
	select {
	case <-done:
		if runErr != nil {
			t.Fatal(runErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("App did not return after cancellation")
	}
}

func TestAppRunPreservesServingFailures(t *testing.T) {
	app := NewApp()
	app.respsrv.Addr = "127.0.0.1:invalid-port"
	app.httpsrv.Addr = "127.0.0.1:invalid-port"
	done := make(chan error, 1)
	go func() { done <- app.Run(context.Background()) }()
	select {
	case err := <-done:
		var addrErr *net.AddrError
		var dnsErr *net.DNSError
		if !errors.As(err, &addrErr) && !errors.As(err, &dnsErr) {
			t.Fatalf("startup failure was lost: %v", err)
		}
	case <-time.After(3 * time.Second):
		app.respsrv.Close()
		app.httpsrv.Close()
		t.Fatal("App did not stop after a server startup failure")
	}
}

func TestAppRunAlreadyCanceled(t *testing.T) {
	app := NewApp()
	app.respsrv.Addr = "127.0.0.1:0"
	app.httpsrv.Addr = "127.0.0.1:0"
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() { done <- app.Run(ctx) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("normal cancellation was reported as a startup failure: %v", err)
		}
	case <-time.After(3 * time.Second):
		app.respsrv.Close()
		app.httpsrv.Close()
		t.Fatal("App did not stop with an already-canceled context")
	}
}

func TestAppRunServerExitTriggersShutdown(t *testing.T) {
	for _, tc := range []struct {
		name     string
		resp     bool
		graceful bool
	}{
		{name: "RESP Close", resp: true},
		{name: "RESP Shutdown", resp: true, graceful: true},
		{name: "HTTP Close"},
		{name: "HTTP Shutdown", graceful: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reserveAddress := func() string {
				t.Helper()
				ln, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				defer ln.Close()
				return ln.Addr().String()
			}
			app := NewApp()
			app.respsrv.Addr = reserveAddress()
			app.httpsrv.Addr = reserveAddress()
			app.ShutdownTimeout = time.Second
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			var runErr error
			go func() { runErr = app.Run(ctx); close(done) }()
			t.Cleanup(func() {
				cancel()
				app.respsrv.Close()
				app.httpsrv.Close()
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Error("App did not finish cleanup")
				}
			})
			for _, addr := range []string{app.respsrv.Addr, app.httpsrv.Addr} {
				deadline := time.Now().Add(3 * time.Second)
				for {
					conn, err := net.DialTimeout("tcp", addr, 20*time.Millisecond)
					if err == nil {
						conn.Close()
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("server on %s did not start: %v", addr, err)
					}
					time.Sleep(time.Millisecond)
				}
			}

			var srv interface {
				Shutdown(context.Context) error
				Close() error
			} = app.httpsrv
			if tc.resp {
				srv = app.respsrv
			}
			if tc.graceful {
				shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), time.Second)
				defer cancelShutdown()
				if err := srv.Shutdown(shutdownCtx); err != nil {
					t.Fatal(err)
				}
			} else if err := srv.Close(); err != nil {
				t.Fatal(err)
			}

			select {
			case <-done:
				if runErr != nil {
					t.Fatalf("normal server exit was reported as an error: %v", runErr)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("App did not shut down after a server exited")
			}
			for _, addr := range []string{app.respsrv.Addr, app.httpsrv.Addr} {
				if conn, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
					conn.Close()
					t.Errorf("server on %s is still accepting connections", addr)
				}
			}
		})
	}
}
