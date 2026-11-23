package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/neelalala/hopper/internal/event"
)

const (
	DefaultWorkerTimeout = time.Minute
	DefaultBufferSize    = 10_000
	DefaultWorkerCount   = 16
)

var (
	ErrChanIsFull     = errors.New("chan is full")
	ErrShuttingDown   = errors.New("shutting down")
	ErrAlreadyStopped = errors.New("already stopped")
)

type Option func(p *Pipeline)

type Processor interface {
	Process(ctx context.Context, event *event.Event) (drop bool, err error)
}

type Pipeline struct {
	eventsBufferSize    int
	processedBufferSize int

	processors []Processor

	workerCount   int
	workerTimeout time.Duration

	mu     *sync.RWMutex
	closed bool
	events chan event.Event

	processed chan event.Event

	log *slog.Logger
}

func New(processors []Processor, opts ...Option) *Pipeline {
	p := &Pipeline{
		eventsBufferSize:    DefaultBufferSize,
		processedBufferSize: DefaultBufferSize,
		processors:          processors,
		workerCount:         DefaultWorkerCount,
		workerTimeout:       DefaultWorkerTimeout,
		log:                 slog.New(slog.DiscardHandler),
	}

	for _, opt := range opts {
		opt(p)
	}

	p.events = make(chan event.Event, p.eventsBufferSize)
	p.processed = make(chan event.Event, p.processedBufferSize)

	return p
}

func (p *Pipeline) Ingest(ev event.Event) error {
	p.log.Debug(
		"ingesting event",
		slog.Any("event", ev),
	)

	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return ErrShuttingDown
	}
	select {
	case p.events <- ev:
		return nil
	default:
		return fmt.Errorf("ingest event: %w", ErrChanIsFull)
	}
}

func (p *Pipeline) Shutdown() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrAlreadyStopped
	}
	close(p.events)
	p.closed = true
	return nil
}

func (p *Pipeline) Processed() <-chan event.Event {
	return p.processed
}

func (p *Pipeline) Start(ctx context.Context) {
	p.log.Info("pipeline started")

	p.runWorkers(ctx)
	close(p.processed)
	p.log.Info("pipeline stopped")
}

func (p *Pipeline) runWorkers(ctx context.Context) {
	var wg sync.WaitGroup
	for range p.workerCount {
		wg.Go(func() {
			for {

				var (
					ev event.Event
					ok bool
				)

				select {
				case <-ctx.Done():
				case ev, ok = <-p.events:
					if !ok {
						p.log.Info("events drained")
						return
					}
				}
				if err := ctx.Err(); err != nil {
					p.log.Warn("forced stop pipeline", slog.String("error", err.Error()))
					return
				}

				var drop bool

				for _, processor := range p.processors {
					var err error
					drop, err = processor.Process(ctx, &ev)
					if err != nil {
						p.log.Error("error processing event", slog.Any("event", ev), slog.String("error", err.Error()))
						drop = true
					}
					if drop {
						break
					}
				}

				if !drop {
					p.sendProcessed(ctx, ev)
				}
			}
		})
	}

	wg.Wait()
}

func (p *Pipeline) sendProcessed(ctx context.Context, ev event.Event) {
	select {
	case <-ctx.Done():
		p.log.Error("error sending processed event", slog.Any("event", ev), slog.String("error", ctx.Err().Error()))
	case p.processed <- ev:
		return
	}
}
