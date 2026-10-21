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

	"github.com/jr-dragon/peridot/internal/data"
	"github.com/jr-dragon/peridot/internal/database"
	"github.com/jr-dragon/peridot/internal/server"
	"golang.org/x/sync/errgroup"
)

const shutdownTimeout = 10 * time.Second

type App struct {
	// ShutdownTimeout limits graceful shutdown before remaining connections close.
	// A non-positive value uses the default of ten seconds.
	ShutdownTimeout time.Duration

	httpsrv  *http.Server
	respsrv  *server.RESP
	database *database.DB

	l *slog.Logger
}

// NewApp validates listener configuration before opening persistent state.
func NewApp(config data.Config) (*App, error) {
	if config.Port == 0 || len(config.Bind) == 0 {
		return nil, errors.New("no RESP listener configured: port 0 or empty bind disables TCP; Unix sockets and TLS are not implemented")
	}
	return initializeApp(config)
}

func (app *App) Run(ctx context.Context) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	timeout := app.ShutdownTimeout
	if timeout <= 0 {
		timeout = shutdownTimeout
	}
	g, ctx := errgroup.WithContext(ctx)
	var respErr, httpErr, respShutdownErr, httpShutdownErr error
	g.Go(func() error {
		defer cancel()
		app.l.InfoContext(ctx, "starting resp server", slog.String("addr", app.respsrv.Addr))

		if err := app.respsrv.ListenAndServe(ctx); err != nil {
			if errors.Is(err, server.ErrServerClosed) || (ctx.Err() != nil && errors.Is(err, ctx.Err())) {
				return nil
			}
			respErr = fmt.Errorf("serve resp server on %s: %w", app.respsrv.Addr, err)
			return respErr
		}

		return nil
	})
	g.Go(func() error {
		defer cancel()
		app.l.InfoContext(ctx, "starting pprof http server", slog.String("addr", app.httpsrv.Addr))

		if err := app.httpsrv.ListenAndServe(); err != nil {
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			httpErr = fmt.Errorf("serve pprof HTTP server on %s: %w", app.httpsrv.Addr, err)
			return httpErr
		}
		return nil
	})
	g.Go(func() error {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		app.l.InfoContext(ctx, "shutting down resp server")
		if err := app.respsrv.Shutdown(shutdownCtx); err != nil {
			respShutdownErr = fmt.Errorf("shutdown resp server on %s: %w", app.respsrv.Addr, err)
			if closeErr := app.respsrv.Close(); closeErr != nil {
				respShutdownErr = errors.Join(respShutdownErr,
					fmt.Errorf("close resp server on %s: %w", app.respsrv.Addr, closeErr),
				)
			}
			return respShutdownErr
		}
		return nil
	})
	g.Go(func() error {
		<-ctx.Done()

		// ctx is already canceled, so using it would prevent Shutdown from waiting
		// for active requests to finish. Use an independent context to allow graceful
		// shutdown to proceed until shutdownTimeout expires.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		app.l.InfoContext(ctx, "shutting down pprof http server")
		if err := app.httpsrv.Shutdown(shutdownCtx); err != nil {
			httpShutdownErr = fmt.Errorf("shutdown pprof HTTP server on %s: %w", app.httpsrv.Addr, err)
			// Shutdown waits for active requests to finish, but a timeout only stops
			// waiting; it does not force their connections closed. Close any remaining
			// HTTP connections if graceful shutdown fails, interrupting those requests.
			if closeErr := app.httpsrv.Close(); closeErr != nil {
				httpShutdownErr = errors.Join(
					httpShutdownErr,
					fmt.Errorf("close pprof HTTP server on %s: %w", app.httpsrv.Addr, closeErr),
				)
			}
			return httpShutdownErr
		}

		return nil
	})

	// Both servers shut down concurrently. Wait for serving and shutdown work,
	// preserving every error rather than only the first returned by Wait.
	_ = g.Wait()
	// Forced socket closure does not itself wait for handlers to finish. Wait
	// before closing persistence so an in-flight mutation can complete safely.
	drainErr := app.respsrv.Shutdown(context.Background())
	var persistenceErr error
	if app.database != nil {
		persistenceErr = app.database.Close()
	}
	return errors.Join(respErr, httpErr, respShutdownErr, httpShutdownErr, drainErr, persistenceErr)
}
