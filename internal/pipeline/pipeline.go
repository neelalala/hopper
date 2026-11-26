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
	DefaultSinkTimeout   = 10 * time.Second
	DefaultWorkerTimeout = 30 * time.Second
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
	Process(context.Context, *event.Event) (drop bool, err error)
}

type Sink func(context.Context, event.Event) error

type Pipeline struct {
	eventsBufferSize int

	processors  []Processor
	sink        Sink
	sinkTimeout time.Duration

	workerCount   int
	workerTimeout time.Duration

	mu     sync.RWMutex
	closed bool
	events chan event.Event

	stopCtx    context.Context
	stopCancel context.CancelFunc
	done       chan struct{}

	log *slog.Logger
}

func New(processors []Processor, sink Sink, opts ...Option) *Pipeline {
	stopCtx, stopCancel := context.WithCancel(context.Background())

	p := &Pipeline{
		eventsBufferSize: DefaultBufferSize,
		processors:       processors,
		sink:             sink,
		sinkTimeout:      DefaultSinkTimeout,
		workerCount:      DefaultWorkerCount,
		workerTimeout:    DefaultWorkerTimeout,
		stopCtx:          stopCtx,
		stopCancel:       stopCancel,
		done:             make(chan struct{}),
		log:              slog.New(slog.DiscardHandler),
	}

	for _, opt := range opts {
		opt(p)
	}

	p.events = make(chan event.Event, p.eventsBufferSize)

	return p
}

func (p *Pipeline) Ingest(ev event.Event) error {
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

func (p *Pipeline) Start() {
	defer func() {
		p.stopCancel()
		close(p.done)

		p.log.Info("pipeline stopped")
	}()

	p.log.Info("pipeline started")

	p.runWorkers()
}

func (p *Pipeline) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrAlreadyStopped
	}
	close(p.events)
	p.closed = true
	p.mu.Unlock()

	select {
	case <-ctx.Done():
		p.stopCancel()
		return ctx.Err()
	case <-p.done:
		return nil
	}
}

func (p *Pipeline) runWorkers() {
	var wg sync.WaitGroup
	for range p.workerCount {
		wg.Go(func() {
			for {
				select {

				case <-p.stopCtx.Done():
					if n := len(p.events); n > 0 {
						p.log.Warn("events left in pipeline", slog.Int("count", n))
					}
					return

				case ev, ok := <-p.events:
					if !ok {
						p.log.Info("events drained")
						return
					}

					if p.stopCtx.Err() != nil {
						p.log.Warn("events left in pipeline", slog.Int("count", len(p.events)+1))
						return
					}

					procCtx, cancel := context.WithTimeout(p.stopCtx, p.workerTimeout)
					drop := p.runProcessors(procCtx, &ev)
					cancel()

					if !drop {
						sinkCtx, cancel := context.WithTimeout(p.stopCtx, p.sinkTimeout)
						err := p.sink(sinkCtx, ev)
						cancel()
						if err != nil {
							p.log.Error(
								"send processed event",
								slog.Any("event", ev),
								slog.String("error", err.Error()),
							)
						}
					}

				}
			}
		})
	}

	wg.Wait()
}

func (p *Pipeline) runProcessors(ctx context.Context, ev *event.Event) bool {
	var drop bool

	for _, processor := range p.processors {
		var err error
		drop, err = processor.Process(ctx, ev)
		if err != nil {
			p.log.Error(
				"process event",
				slog.Any("event", ev),
				slog.String("error", err.Error()),
			)
			drop = true
		}
		if drop {
			return drop
		}
	}
	return false
}
