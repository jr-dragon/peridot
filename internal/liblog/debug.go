// Package liblog provides typed logging helpers that preserve caller locations.
package liblog

import (
	"context"
	"log/slog"
	"runtime"
	"time"
)

// D logs a debug message with typed attributes and the caller's source location.
// Attributes are evaluated before D is called; guard expensive computations at
// the call site when they should run only while debug logging is enabled.
func D(ctx context.Context, l *slog.Logger, msg string, attrs ...slog.Attr) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !l.Enabled(ctx, slog.LevelDebug) {
		return
	}
	var pcs [1]uintptr
	runtime.Callers(2, pcs[:]) // Skip runtime.Callers and D.
	r := slog.NewRecord(time.Now(), slog.LevelDebug, msg, pcs[0])
	r.AddAttrs(attrs...)
	_ = l.Handler().Handle(ctx, r)
}
