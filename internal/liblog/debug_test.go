package liblog

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"runtime"
	"testing"
)

func TestDAttributesAndCallerSource(t *testing.T) {
	var output bytes.Buffer
	l := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug, AddSource: true})).
		With(slog.String("component", "get")).WithGroup("details")
	_, file, line, _ := runtime.Caller(0)
	D(context.Background(), l, "value lookup completed", slog.Bool("found", true), slog.String("value", "a\x00b"))
	var got struct {
		Level     string `json:"level"`
		Message   string `json:"msg"`
		Component string `json:"component"`
		Source    slog.Source
		Details   struct {
			Found bool
			Value string
		}
	}
	if err := json.Unmarshal(output.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Level != "DEBUG" || got.Message != "value lookup completed" || got.Component != "get" || !got.Details.Found || got.Details.Value != "a\x00b" {
		t.Fatalf("log record: %+v", got)
	}
	if got.Source.File != file || got.Source.Line != line+1 || got.Source.Function != "github.com/jr-dragon/peridot/internal/liblog.TestDAttributesAndCallerSource" {
		t.Fatalf("source = %+v, want %s:%d", got.Source, file, line+1)
	}
}

type contextHandler struct {
	slog.Handler
	enabled, handled context.Context
}

func (h *contextHandler) Enabled(ctx context.Context, _ slog.Level) bool {
	h.enabled = ctx
	return true
}

func (h *contextHandler) Handle(ctx context.Context, _ slog.Record) error {
	h.handled = ctx
	return nil
}

func TestDContext(t *testing.T) {
	type contextKey struct{}
	ctx := context.WithValue(context.Background(), contextKey{}, "request")
	for _, tc := range []struct {
		name string
		ctx  context.Context
		want context.Context
	}{
		{"request", ctx, ctx},
		{"nil", nil, context.Background()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := &contextHandler{}
			D(tc.ctx, slog.New(h), "message")
			if h.enabled != tc.want || h.handled != tc.want {
				t.Fatal("request context was not preserved")
			}
		})
	}
}

func TestDDisabledDoesNotAllocateOrEmit(t *testing.T) {
	var output bytes.Buffer
	l := slog.New(slog.NewTextHandler(&output, nil))
	ctx := context.Background()
	allocs := testing.AllocsPerRun(1000, func() {
		D(ctx, l, "set completed",
			slog.Bool("applied", true),
			slog.Bool("expired", false),
			slog.String("condition", "NX"),
			slog.String("expiration", "EX"),
			slog.Bool("return_previous", false),
		)
	})
	if allocs != 0 || output.Len() != 0 {
		t.Fatalf("disabled debug: %v allocs/run, %q output", allocs, output.String())
	}
}

func BenchmarkDisabledDebug(b *testing.B) {
	l := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx := context.Background()
	b.Run("D", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			D(ctx, l, "set completed", slog.Bool("applied", true), slog.Bool("expired", false), slog.String("condition", "NX"), slog.String("expiration", "EX"), slog.Bool("return_previous", false))
		}
	})
	b.Run("LogAttrs", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			l.LogAttrs(ctx, slog.LevelDebug, "set completed", slog.Bool("applied", true), slog.Bool("expired", false), slog.String("condition", "NX"), slog.String("expiration", "EX"), slog.Bool("return_previous", false))
		}
	})
	b.Run("GuardedDebugContext", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if l.Enabled(ctx, slog.LevelDebug) {
				l.DebugContext(ctx, "set completed", slog.Bool("applied", true), slog.Bool("expired", false), slog.String("condition", "NX"), slog.String("expiration", "EX"), slog.Bool("return_previous", false))
			}
		}
	})
	b.Run("DebugContext", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			l.DebugContext(ctx, "set completed", slog.Bool("applied", true), slog.Bool("expired", false), slog.String("condition", "NX"), slog.String("expiration", "EX"), slog.Bool("return_previous", false))
		}
	})
}
