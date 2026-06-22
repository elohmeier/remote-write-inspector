package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/elohmeier/remote-write-inspector/internal/inspector"
)

type identityFlags []string

func (f *identityFlags) String() string {
	return ""
}

func (f *identityFlags) Set(value string) error {
	*f = append(*f, value)
	return nil
}

func main() {
	var identities identityFlags
	var cfg inspector.Config

	flag.StringVar(&cfg.ListenAddress, "listen-address", ":8080", "HTTP listen address.")
	flag.Int64Var(&cfg.MaxBodyBytes, "max-body-bytes", 32<<20, "Maximum compressed request body size.")
	flag.DurationVar(&cfg.StaleCutoff, "stale-cutoff", 23*time.Hour, "Samples older than now minus this duration are reported as stale.")
	flag.DurationVar(&cfg.FutureSkew, "future-skew", 10*time.Minute, "Samples newer than now plus this duration are reported as future samples.")
	flag.IntVar(&cfg.MaxLabels, "max-labels", 128, "Maximum accepted labels per series before reporting excessive_label_count.")
	flag.IntVar(&cfg.MaxLabelValueLength, "max-label-value-length", 4096, "Maximum accepted label value length before reporting excessive_label_value_length.")
	flag.IntVar(&cfg.MaxIdentityLength, "max-identity-length", 128, "Maximum identity value length used in metric labels.")
	flag.IntVar(&cfg.CacheSize, "cache-size", 500000, "Maximum entries per stateful detector cache.")
	flag.Int64Var(&cfg.CacheMemoryBytes, "cache-memory-bytes", 0, "Approximate memory budget in bytes for stateful detector caches. Overrides cache-size when set.")
	flag.DurationVar(&cfg.CacheTTL, "cache-ttl", 25*time.Hour, "TTL for stateful detector cache entries.")
	flag.IntVar(&cfg.TopSeriesSize, "top-series-size", 20, "Maximum top series exposed by the top-series collector.")
	flag.DurationVar(&cfg.TopSeriesWindow, "top-series-window", 5*time.Minute, "Window duration for top-series diagnostics.")
	flag.Float64Var(&cfg.LogSampleRate, "log-sample-rate", 0.01, "Probability for logging a detected bad series or sample example.")
	flag.BoolVar(&cfg.DisableDuplicateSampleDetection, "disable-duplicate-sample-detection", false, "Disable duplicate timestamp with different value detection.")
	flag.BoolVar(&cfg.DisableCrossPathCollisionDetection, "disable-cross-path-collision-detection", false, "Disable cross-input-path canonical labelset collision detection.")
	flag.Var(&identities, "identity", "Enable an identity shorthand. Repeatable. Supported: tenant, pipeline_sink, input_path, writer_id.")
	flag.Parse()

	cfg.IdentityNames = identities
	cfg.DiagnosticMetricPrefixes = []string{"remote_write_inspector_", "obspipeline_"}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	reg := prometheus.NewRegistry()

	srv, err := inspector.NewServer(cfg, reg, logger)
	if err != nil {
		logger.Error("invalid configuration", "err", err)
		os.Exit(2)
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("starting remote-write-inspector", "listen_address", cfg.ListenAddress)
		errCh <- srv.ListenAndServe()
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	select {
	case sig := <-stop:
		logger.Info("shutting down", "signal", sig.String())
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server failed", "err", err)
			os.Exit(1)
		}
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("shutdown failed", "err", err)
		os.Exit(1)
	}
}
