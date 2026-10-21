package cmd

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jr-dragon/peridot/pkg/resp"
)

func TestPingExec(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
		err  error
	}{
		{name: "no command", want: "-ERR wrong number of arguments for 'ping' command\r\n", err: ErrWrongArity},
		{name: "pong", args: []string{"PING"}, want: "+PONG\r\n"},
		{name: "message", args: []string{"PING", "hello"}, want: "$5\r\nhello\r\n"},
		{name: "empty message", args: []string{"PING", ""}, want: "$0\r\n\r\n"},
		{name: "binary message", args: []string{"PING", "\x00\r\n\xff"}, want: "$4\r\n\x00\r\n\xff\r\n"},
		{name: "too many arguments", args: []string{"PING", "one", "two"}, want: "-ERR wrong number of arguments for 'ping' command\r\n", err: ErrWrongArity},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := make([]resp.BulkString, len(tc.args))
			for i, arg := range tc.args {
				args[i] = resp.NewBulkString(arg)
			}
			value, err := (&Ping{}).Exec(context.Background(), args)
			if !errors.Is(err, tc.err) {
				t.Fatalf("Exec() error = %v, want %v", err, tc.err)
			}
			if tc.err != nil {
				if value != nil || !errors.Is(err, ErrCommand) {
					t.Fatalf("Exec() = %v, %v, want nil and command error", value, err)
				}
				if !errors.As(err, &value) {
					t.Fatal("command error has no RESP reply")
				}
			}
			if value == nil || string(value.Marshal()) != tc.want {
				t.Fatalf("Exec() reply = %v, want %q", value, tc.want)
			}
			for i, arg := range args {
				if arg.String() != tc.args[i] {
					t.Fatalf("Exec() modified argument %d", i)
				}
			}
		})
	}
}

func TestPingResultsAreIndependent(t *testing.T) {
	args := []resp.BulkString{resp.NewBulkString("PING")}
	first, _ := (&Ping{}).Exec(context.Background(), args)
	first.(resp.SimpleString)[0] = 'X'
	second, _ := (&Ping{}).Exec(context.Background(), args)
	if got := string(second.Marshal()); got != "+PONG\r\n" {
		t.Fatalf("previous result mutation changed later reply: %q", got)
	}
}

func TestPingReplySurvivesNextCommandRead(t *testing.T) {
	reader := resp.NewCommand(strings.NewReader("*2\r\n$4\r\nPING\r\n$5\r\nfirst\r\n*2\r\n$4\r\nPING\r\n$6\r\nsecond\r\n"))
	var replies []resp.Value
	for range 2 {
		args, err := reader.Read()
		if err != nil {
			t.Fatal(err)
		}
		value, err := (&Ping{}).Exec(context.Background(), args)
		if err != nil {
			t.Fatal(err)
		}
		replies = append(replies, value)
	}
	for i, want := range []string{"$5\r\nfirst\r\n", "$6\r\nsecond\r\n"} {
		if got := string(replies[i].Marshal()); got != want {
			t.Fatalf("reply %d = %q, want %q", i, got, want)
		}
	}
}

func TestPingConcurrentExecution(t *testing.T) {
	ping := &Ping{}
	results := make(chan error, 16)
	for range 16 {
		go func() {
			value, err := ping.Exec(context.Background(), []resp.BulkString{resp.NewBulkString("PING")})
			if err == nil && (value == nil || string(value.Marshal()) != "+PONG\r\n") {
				err = fmt.Errorf("unexpected PING reply: %v", value)
			}
			if err == nil {
				value.(resp.SimpleString)[0] = 'X'
			}
			results <- err
		}()
	}
	for range 16 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
}
