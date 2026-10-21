package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/jr-dragon/peridot/internal/liberr"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		AddSource: true,
	})))

	ctx := context.TODO()
	if err := NewApp().Run(ctx); err != nil {
		slog.ErrorContext(ctx, "failed to run application",
			slog.String("component", "app"),
			liberr.Attr(err),
		)
		os.Exit(1)
	}
}
