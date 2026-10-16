package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	microsandboxruntime "github.com/floci-io/floci/tools/microsandbox-runtime"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		slog.Error("microsandbox host stopped", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	port := cmp.Or(os.Getenv("QUARKUS_HTTP_PORT"), os.Getenv("PORT"), "8186")
	number, err := strconv.Atoi(port)
	if err != nil || number < 0 || number > 65535 {
		return errors.New("HTTP port must be a TCP port between 0 and 65535")
	}
	settings := hostConfig{
		home: os.Getenv("MICROSANDBOX_HOME"), token: os.Getenv("MICROSANDBOX_TOKEN"),
		version: cmp.Or(os.Getenv("SIDECAR_VERSION"), "dev"), timeout: 180 * time.Second,
	}
	var runtime executor
	if settings.configured() {
		runtime, err = microsandboxruntime.New(microsandboxruntime.Config{
			Executable: cmp.Or(os.Getenv("MICROSANDBOX_EXECUTABLE"), "msb"), Home: settings.home,
		})
		if err != nil {
			return fmt.Errorf("configure runtime: %w", err)
		}
	}
	listener, err := net.Listen("tcp", net.JoinHostPort(cmp.Or(os.Getenv("QUARKUS_HTTP_HOST"), "127.0.0.1"), port))
	if err != nil {
		return err
	}
	server := &http.Server{
		Handler: newHandler(settings, runtime), ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout: time.Minute,
	}
	finished := make(chan error, 1)
	go func() { finished <- server.Serve(listener) }()
	slog.Info("microsandbox host listening", "address", listener.Addr().String(), "sdk", "0.7.8")
	select {
	case err := <-finished:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), settings.timeout+10*time.Second)
		defer cancel()
		return server.Shutdown(shutdown)
	}
}
