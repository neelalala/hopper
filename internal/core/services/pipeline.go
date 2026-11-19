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

func (p *WorkerPoolPipeline) IngestEvent(ev domain.Event) error {
	p.log.Debug(
		"ingesting event",
		eventToSlogAttr(ev),
	)

	select {
	case p.events <- ev:
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

	p.startWorkers(ctx)
	p.log.Info("pipeline stopped")
}

func (p *WorkerPoolPipeline) startWorkers(ctx context.Context) {
	workCtx, cancel := context.WithCancel(context.Background())
	workersDone := make(chan struct{})

	go func() {
		<-ctx.Done()
		timer := time.NewTimer(p.WorkerTimeout)
		defer timer.Stop()

		select {
		case <-timer.C:
			cancel()
		case <-workersDone:
			cancel()
		}
	}()

	var wg sync.WaitGroup
	for range p.workerCount {
		wg.Go(func() {
			for {

				var (
					ev domain.Event
					ok bool
				)

				select {
				case <-ctx.Done():
				case ev, ok = <-p.events:
				}
				if ctx.Err() != nil || !ok {
					return
				}

				var drop bool

				for _, processor := range p.processors {
					var err error
					drop, err = processor.Process(workCtx, &ev)
					if err != nil {
						p.log.Error("error processing event", eventToSlogAttr(ev), slog.String("error", err.Error()))
						drop = true
					}
					if drop {
						break
					}
				}

				if !drop {
					p.sendProcessed(workCtx, ev)
				}
			}
		})
	}

	wg.Wait()
	close(workersDone)
}

func (p *WorkerPoolPipeline) sendProcessed(ctx context.Context, ev domain.Event) {
	select {
	case <-ctx.Done():
		p.log.Error("error sending processed event", eventToSlogAttr(ev), slog.String("error", ctx.Err().Error()))
	case p.processed <- ev:
		return
	}
}

func eventToSlogAttr(ev domain.Event) slog.Attr {
	return slog.Group(
		"event",
		slog.String("id", ev.ID.String()),
		slog.String("name", ev.Name),
		slog.Group(
			"user",
			slog.String("id", ev.UserID),
			slog.String("agent", ev.UserAgent),
			slog.String("device", ev.UserDevice),
			slog.String("os", ev.UserOS),
			slog.String("ip", ev.IP),
		),
		slog.Time("client_time", ev.ClientTime),
		slog.Time("server_time", ev.ServerTime),
	)
}
