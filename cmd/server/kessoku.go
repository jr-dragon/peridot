package main

import (
	"log/slog"
	"net/http"

	"github.com/jr-dragon/peridot/internal/server"
	"github.com/jr-dragon/peridot/internal/service/cmd"
	"github.com/mazrean/kessoku"
)

//go:generate go tool kessoku $GOFILE
var _ = kessoku.Inject[*App](
	"NewApp",

	kessoku.Provide(cmd.NewCommands),
	kessoku.Provide(func(cmds []cmd.Command) server.Handler {
		return server.NewHandler(cmds)
	}),
	kessoku.Provide(func(h server.Handler) *server.RESP {
		return server.NewRESP(":6379", h)
	}),
	kessoku.Provide(func() *http.Server {
		return &http.Server{Addr: "localhost:6060"}
	}),
	kessoku.Provide(slog.Default),
	kessoku.Provide(func(httpsrv *http.Server, respsrv *server.RESP, l *slog.Logger) *App {
		return &App{
			ShutdownTimeout: shutdownTimeout,
			httpsrv:         httpsrv,
			respsrv:         respsrv,
			l:               l.With(slog.String("component", "app")),
		}
	}),
)
