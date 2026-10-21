package main

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jr-dragon/peridot/internal/data"
	"github.com/jr-dragon/peridot/internal/persistence/rdb"
)

func testNewApp(t *testing.T) *App {
	t.Helper()
	app, err := NewApp(testConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	return app
}

func TestAppRunContextCancellationDrainsRESP(t *testing.T) {
	testAppShutdown(t, false, nil, testConfig(t))
}

func TestAppRunContextCancellationForcesRESPClose(t *testing.T) {
	testAppShutdown(t, true, nil, testConfig(t))
}

func TestAppRunHTTPClosePreservesRESPShutdownTimeout(t *testing.T) {
	testAppShutdown(t, true, func(app *App) error { return app.httpsrv.Close() }, testConfig(t))
}

func TestAppAOFShutdownDrainsAndCloses(t *testing.T) {
	testAppShutdown(t, false, nil, testAOFConfig(t, filepath.Join(t.TempDir(), "drain.aof")))
}

func TestAppAOFShutdownForcesClose(t *testing.T) {
	testAppShutdown(t, true, nil, testAOFConfig(t, filepath.Join(t.TempDir(), "forced.aof")))
}

func TestAppRejectsInvalidAOFBeforeServing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.aof")
	if err := os.WriteFile(path, []byte("REDIS0015"), 0600); err != nil {
		t.Fatal(err)
	}
	if app, err := NewApp(testAOFConfig(t, path)); app != nil || !errors.Is(err, rdb.ErrFormat) {
		t.Fatalf("app=%v, error=%v", app, err)
	}
}

func testAppShutdown(t *testing.T, forceClose bool, stopServer func(*App) error, config data.Config) {
	t.Helper()
	// Reserve an address because RESP keeps its listener private.
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := reservation.Addr().String()
	reservation.Close()

	app, err := NewApp(config)
	if err != nil {
		t.Fatal(err)
	}
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
	if config.AOFPath() != "" {
		if _, err := io.WriteString(conn, "SET initial durable\r\n"); err != nil {
			t.Fatal(err)
		}
		var ok [5]byte
		if _, err := io.ReadFull(conn, ok[:]); err != nil || string(ok[:]) != "+OK\r\n" {
			t.Fatalf("SET reply: %q,%v", ok, err)
		}
	}
	verifyPersistence := func(key, value string) {
		t.Helper()
		if config.AOFPath() == "" {
			return
		}
		restarted, err := NewApp(config)
		if err != nil {
			t.Fatal(err)
		}
		defer restarted.database.Close()
		if e, ok, err := restarted.database.Get(context.Background(), key); err != nil || !ok || e.Value != value {
			t.Fatalf("persisted %q = %v,%v,%v", key, e, ok, err)
		}
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
		verifyPersistence("initial", "durable")
		return
	}
	// An active stream must survive signal-context cancellation until it drains.
	request, want := "PING hello\r\n", "$5\r\nhello\r\n"
	if config.AOFPath() != "" {
		request, want = "SET draining completed\r\n"+request, "+OK\r\n"+want
	}
	if _, err := io.WriteString(conn, request); err != nil {
		t.Fatal(err)
	}
	if err := conn.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	remaining, err := io.ReadAll(conn)
	if err != nil {
		t.Fatal(err)
	}
	if string(remaining) != want {
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
	verifyPersistence("draining", "completed")
}

func TestAppRunPreservesServingFailures(t *testing.T) {
	app := testNewApp(t)
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
	app := testNewApp(t)
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
			app := testNewApp(t)
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

func testConfig(t *testing.T) data.Config {
	config := data.DefaultConfig()
	config.Dir = t.TempDir()
	config.Bind = []string{"127.0.0.1"}
	return config
}

func testAOFConfig(t *testing.T, path string) data.Config {
	config := testConfig(t)
	config.AppendOnly = true
	config.Dir, config.AppendFilename = filepath.Dir(path), filepath.Base(path)
	return config
}
