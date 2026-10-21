package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/jr-dragon/peridot/internal/data"
	"github.com/jr-dragon/peridot/internal/liberr"
)

func main() {
	if len(os.Args) == 2 && (os.Args[1] == "--help" || os.Args[1] == "-h") {
		fmt.Fprintln(os.Stdout, "Usage: peridot-server [redis.conf | -] [--directive value ...]")
		fmt.Fprintln(os.Stdout, "Supported: bind, port, appendonly, appendfsync, appendfilename, appenddirname, aof-use-rdb-preamble, dbfilename, save, stop-writes-on-bgsave-error, dir, include")
		fmt.Fprintln(os.Stdout, "CLI-only: --pprof-addr address (default localhost:6060)")
		fmt.Fprintln(os.Stdout, "Other Redis directives are ignored. Persistence supports RDB snapshots and multipart AOF with always, everysec (default), or no fsync.")
		return
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		AddSource: true,
	})))

	ctx := context.Background()
	config, err := data.LoadConfig(os.Args[1:], os.Stdin)
	if err == nil {
		var app *App
		app, err = NewApp(config)
		if err == nil {
			err = app.Run(ctx)
		}
	}
	if err != nil {
		slog.ErrorContext(ctx, "failed to run application",
			slog.String("component", "app"),
			liberr.Attr(err),
		)
		os.Exit(1)
	}
}
