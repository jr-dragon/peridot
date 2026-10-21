package server

import (
	"context"
	"errors"
	"io"
	"net"
	"syscall"
	"testing"
	"time"
)

func TestMultipleListenersAcceptAndClose(t *testing.T) {
	ln, err := listenTCPAddresses(context.Background(), []string{"127.0.0.1:0", "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	m := ln.(*multiListener)
	for _, child := range m.listeners {
		client, err := net.DialTimeout("tcp", child.Addr().String(), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		accepted := make(chan acceptResult, 1)
		go func() { c, err := ln.Accept(); accepted <- acceptResult{c, err} }()
		select {
		case result := <-accepted:
			if result.err != nil {
				t.Fatal(result.err)
			}
			result.conn.Close()
		case <-time.After(time.Second):
			t.Fatal("Accept did not return")
		}
	}
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := ln.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Accept after Close: %v", err)
	}
	for _, child := range m.listeners {
		if c, err := net.DialTimeout("tcp", child.Addr().String(), time.Second); err == nil {
			c.Close()
			t.Fatal("child listener is still open")
		}
	}
}

func TestListenOptionalAddressAndStartupFailure(t *testing.T) {
	ln, err := listenTCPAddresses(context.Background(), []string{"-192.0.2.1:0", "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	ln.Close()
	if ln, err := listenTCPAddresses(context.Background(), []string{"-192.0.2.1:0"}); err == nil {
		ln.Close()
		t.Fatal("accepted zero available listeners")
	}
	occupied, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	reservation, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := reservation.Addr().String()
	reservation.Close()
	_, err = listenTCPAddresses(context.Background(), []string{address, "-" + occupied.Addr().String()})
	if !errors.Is(err, syscall.EADDRINUSE) {
		t.Fatalf("optional address hid bind error: %v", err)
	}
	reopened, err := net.Listen("tcp4", address)
	if err != nil {
		t.Fatalf("failed startup leaked first listener: %v", err)
	}
	reopened.Close()
}

func TestRESPMultipleListenersShutdown(t *testing.T) {
	for _, force := range []bool{false, true} {
		t.Run(map[bool]string{false: "drain", true: "force"}[force], func(t *testing.T) {
			s := NewRESP("127.0.0.1:0", newTestHandler())
			s.AdditionalAddresses = []string{"127.0.0.1:0"}
			done := make(chan error, 1)
			go func() { done <- s.ListenAndServe(context.Background()) }()
			t.Cleanup(func() { s.Close(); s.Shutdown(context.Background()) })
			var addresses []string
			waitFor(t, func() bool {
				s.lnmu.Lock()
				defer s.lnmu.Unlock()
				if s.ln == nil {
					return false
				}
				for _, ln := range s.ln.(*multiListener).listeners {
					addresses = append(addresses, ln.Addr().String())
				}
				return true
			})
			var clients []net.Conn
			for _, address := range addresses {
				c, err := net.DialTimeout("tcp", address, time.Second)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { c.Close() })
				c.SetDeadline(time.Now().Add(3 * time.Second))
				echo(t, c)
				clients = append(clients, c)
			}
			if force {
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if err := s.Shutdown(ctx); !errors.Is(err, context.Canceled) {
					t.Fatalf("active clients did not defer shutdown: %v", err)
				}
				for _, c := range clients {
					echo(t, c)
					c.(*net.TCPConn).CloseWrite()
				}
			}
			for _, c := range clients {
				if b, err := io.ReadAll(c); err != nil || len(b) != 0 {
					t.Fatalf("connection did not close: %q, %v", b, err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := s.Shutdown(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				if !errors.Is(err, ErrServerClosed) {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("serving did not stop")
			}
		})
	}
}
