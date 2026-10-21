package server

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/jr-dragon/peridot/internal/service/cmd"
	"github.com/jr-dragon/peridot/pkg/resp"
)

func TestRESPRedisProtocolCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name, wire, want string
		fatal            bool
	}{
		{name: "empty requests", wire: "\r\n\n \t\v\f\r\n*0\r\n*-10\r\n*-9223372036854775808\r\nPING\r\n", want: "+PONG\r\n"},
		{name: "LF inline", wire: "PING\n", want: "+PONG\r\n"},
		{name: "CR separator", wire: "PING\rhello\r\n", want: "$5\r\nhello\r\n"},
		{name: "negative bulk", wire: "*2\r\n$4\r\nPING\r\n$-1\r\n", want: "-ERR Protocol error: invalid bulk length\r\n", fatal: true},
		{name: "invalid negative bulk", wire: "*2\r\n$4\r\nPING\r\n$-oops\r\n", want: "-ERR Protocol error: invalid bulk length\r\n", fatal: true},
		{name: "leading zero bulk", wire: "*1\r\n$04\r\nPING\r\n", want: "-ERR Protocol error: invalid bulk length\r\n", fatal: true},
		{name: "leading zero array", wire: "*01\r\n$4\r\nPING\r\n", want: "-ERR Protocol error: invalid multibulk length\r\n", fatal: true},
		{name: "negative zero array", wire: "*-0\r\n", want: "-ERR Protocol error: invalid multibulk length\r\n", fatal: true},
		{name: "array overflow", wire: "*2147483648\r\n", want: "-ERR Protocol error: invalid multibulk length\r\n", fatal: true},
		{name: "bulk overflow", wire: "*1\r\n$536870913\r\n", want: "-ERR Protocol error: invalid bulk length\r\n", fatal: true},
		{name: "wrong argument type", wire: "*1\r\n+PING\r\n", want: "-ERR Protocol error: expected '$', got '+'\r\n", fatal: true},
		{name: "unbalanced quotes", wire: "PING \"hello\r\n", want: "-ERR Protocol error: unbalanced quotes in request\r\n", fatal: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, peer := startRESPPipe(t, NewHandler(cmd.NewCommands()))
			// A fatal error must prevent execution of the following pipelined PING.
			if _, err := io.WriteString(peer, tc.wire+"PING\r\n"); err != nil {
				t.Fatal(err)
			}
			if tc.fatal {
				reply, err := io.ReadAll(peer)
				if err != nil || string(reply) != tc.want {
					t.Fatalf("fatal reply = %q, %v; want %q and EOF", reply, err, tc.want)
				}
				return
			}
			want := tc.want + "+PONG\r\n"
			reply := make([]byte, len(want))
			if _, err := io.ReadFull(peer, reply); err != nil || string(reply) != want {
				t.Fatalf("reply = %q, %v; want %q", reply, err, want)
			}
			if _, err := io.WriteString(peer, "PING\r\n"); err != nil {
				t.Fatal(err)
			}
			pong := make([]byte, len("+PONG\r\n"))
			if _, err := io.ReadFull(peer, pong); err != nil || string(pong) != "+PONG\r\n" {
				t.Fatalf("connection no longer usable: %q, %v", pong, err)
			}
		})
	}
}

func TestHandlerPreservesProtocolErrorReason(t *testing.T) {
	h := NewHandler(cmd.NewCommands())
	value, err := h.ServeRESP(context.Background(), resp.NewCommand(strings.NewReader("*1\r\n+PING\r\n")))
	var protocolErr *resp.ProtocolError
	if !errors.Is(err, ErrClientFatal) || !errors.Is(err, resp.ErrProtocol) || !errors.As(err, &protocolErr) || protocolErr.Reason != "expected '$', got '+'" {
		t.Fatalf("protocol error lost reason or classification: %v", err)
	}
	if value == nil || string(value.Marshal()) != "-ERR Protocol error: expected '$', got '+'\r\n" {
		t.Fatalf("reply = %v", value)
	}
}
