package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"syscall"
)

// listenTCPAddresses opens all sockets before accepting any connections. This
// keeps startup failure atomic and lets RESP track one listener for shutdown.
func listenTCPAddresses(ctx context.Context, addresses []string) (net.Listener, error) {
	var listeners []net.Listener
	closeAll := func() {
		for _, ln := range listeners {
			ln.Close()
		}
	}
	for _, address := range addresses {
		optional := strings.HasPrefix(address, "-")
		address = strings.TrimPrefix(address, "-")
		network := "tcp"
		host, _, _ := net.SplitHostPort(address)
		if ip := net.ParseIP(host); ip != nil {
			if ip.To4() != nil {
				network = "tcp4"
			} else {
				network = "tcp6"
			}
		}
		var lc net.ListenConfig
		ln, err := lc.Listen(ctx, network, address)
		if err != nil {
			if errors.Is(err, syscall.EAFNOSUPPORT) || (optional && errors.Is(err, syscall.EADDRNOTAVAIL)) {
				continue
			}
			closeAll()
			return nil, fmt.Errorf("listen on %s: %w", address, err)
		}
		listeners = append(listeners, ln)
	}
	if len(listeners) == 0 {
		return nil, errors.New("no available TCP listen addresses")
	}
	if len(listeners) == 1 {
		return listeners[0], nil
	}
	m := &multiListener{listeners: listeners, accepted: make(chan acceptResult), done: make(chan struct{})}
	for _, ln := range listeners {
		m.wg.Go(func() {
			for {
				conn, err := ln.Accept()
				select {
				case m.accepted <- acceptResult{conn, err}:
				case <-m.done:
					if conn != nil {
						conn.Close()
					}
					return
				}
				if err != nil {
					return
				}
			}
		})
	}
	return m, nil
}

type acceptResult struct {
	conn net.Conn
	err  error
}

type multiListener struct {
	listeners []net.Listener
	accepted  chan acceptResult
	done      chan struct{}
	once      sync.Once
	wg        sync.WaitGroup
	err       error
}

func (m *multiListener) Accept() (net.Conn, error) {
	select {
	case result := <-m.accepted:
		return result.conn, result.err
	case <-m.done:
		return nil, net.ErrClosed
	}
}

func (m *multiListener) Addr() net.Addr { return m.listeners[0].Addr() }

func (m *multiListener) Close() error {
	m.once.Do(func() {
		close(m.done)
		for _, ln := range m.listeners {
			m.err = errors.Join(m.err, ln.Close())
		}
		m.wg.Wait()
	})
	return m.err
}
