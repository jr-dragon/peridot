package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
)

const shutdownTimeout = 10 * time.Second

type App struct {
	httpsrv *http.Server

	l *slog.Logger
}

func NewApp() *App {
	return &App{
		httpsrv: &http.Server{Addr: "localhost:6060"},

		l: slog.Default().With(slog.String("component", "app")),
	}
}

func (app *App) Run(ctx context.Context) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	g, ctx := errgroup.WithContext(ctx)
	var serveErr, shutdownErr error
	g.Go(func() error {
		app.l.InfoContext(ctx, "starting pprof http server", slog.String("addr", app.httpsrv.Addr))

		if err := app.httpsrv.ListenAndServe(); err != nil {
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			serveErr = fmt.Errorf("serve pprof HTTP server on %s: %w", app.httpsrv.Addr, err)
			return serveErr
		}
		return nil
	})
	g.Go(func() error {
		<-ctx.Done()

		// ctx is already canceled, so using it would prevent Shutdown from waiting
		// for active requests to finish. Use an independent context to allow graceful
		// shutdown to proceed until shutdownTimeout expires.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()

		app.l.InfoContext(ctx, "shutting down pprof http server")
		if err := app.httpsrv.Shutdown(shutdownCtx); err != nil {
			shutdownErr = fmt.Errorf("shutdown pprof HTTP server on %s: %w", app.httpsrv.Addr, err)
			// Shutdown waits for active requests to finish, but a timeout only stops
			// waiting; it does not force their connections closed. Close any remaining
			// HTTP connections if graceful shutdown fails, interrupting those requests.
			if closeErr := app.httpsrv.Close(); closeErr != nil {
				shutdownErr = errors.Join(
					shutdownErr,
					fmt.Errorf("close pprof HTTP server on %s: %w", app.httpsrv.Addr, closeErr),
				)
			}
			return shutdownErr
		}

		return nil
	})

	// Wait for both goroutines before reading their errors. Wait only returns the
	// first error, so join the results to preserve any additional shutdown failure.
	_ = g.Wait()
	return errors.Join(serveErr, shutdownErr)
}
