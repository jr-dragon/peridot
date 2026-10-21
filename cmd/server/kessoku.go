package main

import (
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/jr-dragon/peridot/internal/data"
	"github.com/jr-dragon/peridot/internal/database"
	"github.com/jr-dragon/peridot/internal/persistence"
	"github.com/jr-dragon/peridot/internal/server"
	"github.com/jr-dragon/peridot/internal/service/cmd"
	"github.com/jr-dragon/peridot/internal/storage"
	"github.com/mazrean/kessoku"
)

//go:generate go tool kessoku $GOFILE
var _ = kessoku.Inject[*App](
	"initializeApp",

	kessoku.Provide(storage.New),
	kessoku.Provide(persistence.New),
	kessoku.Provide(func(config data.Config) []database.Option {
		return []database.Option{database.WithActiveExpiration(config.HZ, config.DynamicHZ)}
	}),
	kessoku.Provide(database.New),
	kessoku.Provide(cmd.NewCommandsFromConfig),
	kessoku.Provide(func(cmds []cmd.Command) server.Handler {
		return server.NewHandler(cmds)
	}),
	kessoku.Provide(newRESP),
	kessoku.Provide(func(config data.Config) *http.Server {
		return &http.Server{Addr: config.Extra.PprofAddress}
	}),
	kessoku.Provide(slog.Default),
	kessoku.Provide(func(httpsrv *http.Server, respsrv *server.RESP, l *slog.Logger, db *database.DB) *App {
		return &App{
			ShutdownTimeout: shutdownTimeout,
			httpsrv:         httpsrv,
			respsrv:         respsrv,
			database:        db,
			l:               l.With(slog.String("component", "app")),
		}
	}),
)

func newRESP(h server.Handler, config data.Config) *server.RESP {
	addresses := make([]string, len(config.Bind))
	for i, bind := range config.Bind {
		optional := strings.HasPrefix(bind, "-")
		bind = strings.TrimPrefix(bind, "-")
		switch bind {
		case "*":
			bind = "0.0.0.0"
		case "::*":
			bind = "::"
		}
		addresses[i] = net.JoinHostPort(bind, strconv.Itoa(config.Port))
		if optional {
			addresses[i] = "-" + addresses[i]
		}
	}
	s := server.NewRESP(addresses[0], h)
	s.AdditionalAddresses = addresses[1:]
	return s
}
