package services

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/neelalala/hopper/internal/core/domain"
	"github.com/neelalala/hopper/internal/core/ports"
)

const DefaultWorkerTimeout = time.Minute

type WorkerPoolPipeline struct {
	processors []ports.Processor

	workerCount   int
	WorkerTimeout time.Duration

	events    chan domain.Event
	processed chan domain.Event

	log *slog.Logger
}

func NewWorkerPoolPipeline(processors []ports.Processor, bufferSize, workerCount int, log *slog.Logger) (*WorkerPoolPipeline, error) {
	if bufferSize <= 0 {
		return nil, fmt.Errorf("buffer size must be positive, got: %d", bufferSize)
	}
	if workerCount <= 0 {
		return nil, fmt.Errorf("worker count must be positive, got: %d", workerCount)
	}
	if log == nil {
		return nil, fmt.Errorf("nil logger")
	}
	events := make(chan domain.Event, bufferSize)
	processed := make(chan domain.Event, bufferSize)

	return &WorkerPoolPipeline{
		processors:    processors,
		workerCount:   workerCount,
		WorkerTimeout: DefaultWorkerTimeout,
		events:        events,
		processed:     processed,
		log:           log,
	}, nil
}

func (p *WorkerPoolPipeline) IngestEvent(e domain.Event) error {
	p.log.Debug(
		"ingesting event",
		eventToSlogAttr(e),
	)

	select {
	case p.events <- e:
		return nil
	default:
		return fmt.Errorf("ingest event: %w", domain.ErrChanIsFull)
	}
}

func (p *WorkerPoolPipeline) ProcessedEvents() <-chan domain.Event {
	return p.processed
}

func (p *WorkerPoolPipeline) Start(ctx context.Context) {
	p.log.Info("pipeline started")

	for {
		select {
		default:

		case <-ctx.Done():
			p.log.Info("pipeline stopped")
			return
		}
	}
}
func eventToSlogAttr(e domain.Event) slog.Attr {
	return slog.Group(
		"event",
		slog.String("id", e.ID.String()),
		slog.String("name", e.Name),
		slog.Group(
			"user",
			slog.String("id", e.UserID),
			slog.String("agent", e.UserAgent),
			slog.String("device", e.UserDevice),
			slog.String("os", e.UserOS),
			slog.String("ip", e.IP),
		),
		slog.Time("client_time", e.ClientTime),
		slog.Time("server_time", e.ServerTime),
	)
}
