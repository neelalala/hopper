package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/neelalala/hopper/internal/batcher"
	"github.com/neelalala/hopper/internal/config"
	"github.com/neelalala/hopper/internal/event"
	"github.com/neelalala/hopper/internal/pipeline"
	"github.com/neelalala/hopper/internal/pipeline/processors"
	"github.com/neelalala/hopper/internal/transport/http/server"
)

type mockEvRepo struct {
	log *slog.Logger
}

func (m *mockEvRepo) CreateEvents(_ context.Context, evs []event.Event) error {
	m.log.Debug(
		"create events",
		"count", len(evs),
		"first", evs[0],
		"last", evs[len(evs)-1],
	)
	return nil
}

func main() {
	cfg, err := config.Parse("config.yaml")
	if err != nil {
		slog.Error("parse config", slog.String("error", err.Error()))
		return
	}

	log := mustMakeLogger(cfg.Logger)

	if err := run(cfg, log); err != nil {
		log.Error("running app", slog.String("error", err.Error()))
	}
}

func run(cfg config.Config, log *slog.Logger) error {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	evRepo := &mockEvRepo{log}
	batcherOpts := makeBatcherOptions(cfg.Batcher)
	batcherOpts = append(batcherOpts, batcher.WithLogger(log.With(slog.String("service", "batcher"))))
	b := batcher.New(evRepo, batcherOpts...)
	go b.Start()

	procs := []pipeline.Processor{new(processors.Validate)}
	p := pipeline.New(procs, b.Add, pipeline.WithLogger(log.With(slog.String("service", "pipeline"))), pipeline.WithWorkerCount(4))
	go p.Start()

	s := server.New(cfg.HTTPServer.Address, p, log.With(slog.String("service", "http server")))
	go func() {
		if err := s.Start(); err != nil {
			log.Error("run http server", slog.String("error", err.Error()))
		}
	}()

	<-signalChan

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := s.Shutdown(ctx); err != nil {
		return fmt.Errorf("shutdown server: %w", err)
	}

	if err := p.Shutdown(ctx); err != nil {
		return fmt.Errorf("shutdown pipeline: %w", err)
	}

	if err := b.Shutdown(ctx); err != nil {
		return fmt.Errorf("shutdown batcher: %w", err)
	}

	return nil
}

func mustMakeLogger(cfg config.LoggerConfig) *slog.Logger {
	var level slog.Level
	switch cfg.Level {
	case "DEBUG":
		level = slog.LevelDebug
	case "INFO":
		level = slog.LevelInfo
	case "WARN":
		level = slog.LevelWarn
	case "ERROR":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := slog.HandlerOptions{
		AddSource: cfg.AddSource,
		Level:     level,
	}
	handler := slog.NewJSONHandler(os.Stdout, &opts)
	return slog.New(handler)
}

func makeBatcherOptions(cfg config.BatcherConfig) []batcher.Option {
	var opts []batcher.Option
	if cfg.BatchSize != 0 {
		opts = append(opts, batcher.WithBatchSize(cfg.BatchSize))
	}
	if cfg.EventsBufferSize != 0 {
		opts = append(opts, batcher.WithEventsChanBufferSize(cfg.EventsBufferSize))
	}
	if cfg.FlushChannelSize != 0 {
		opts = append(opts, batcher.WithFlushChanBufferSize(cfg.FlushChannelSize))
	}
	if cfg.WorkerCount != 0 {
		opts = append(opts, batcher.WithWorkerCount(cfg.WorkerCount))
	}
	if cfg.SendTickerDuration != 0 {
		opts = append(opts, batcher.WithSendTickerDuration(cfg.SendTickerDuration))
	}
	if cfg.WorkerTimeout != 0 {
		opts = append(opts, batcher.WithWorkerTimeout(cfg.WorkerTimeout))
	}

	return opts
}
