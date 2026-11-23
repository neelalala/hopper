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

var ErrChanIsFull = errors.New("chan is full")

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

	events    chan event.Event
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

	select {
	case p.events <- ev:
		return nil
	default:
		return fmt.Errorf("ingest event: %w", ErrChanIsFull)
	}
}

func (p *Pipeline) Processed() <-chan event.Event {
	return p.processed
}

func (p *Pipeline) Start(ctx context.Context) {
	p.log.Info("pipeline started")

	p.startWorkers(ctx)
	close(p.processed)
	p.log.Info("pipeline stopped")
}

func (p *Pipeline) startWorkers(ctx context.Context) {
	workCtx, cancel := context.WithCancel(context.Background())
	workersDone := make(chan struct{})

	go func() {
		<-ctx.Done()
		timer := time.NewTimer(p.workerTimeout)
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
					ev event.Event
					ok bool
				)

				select {
				case <-ctx.Done(): // TODO: if ctx.Done(), i want to drain p.events first
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
						p.log.Error("error processing event", slog.Any("event", ev), slog.String("error", err.Error()))
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

func (p *Pipeline) sendProcessed(ctx context.Context, ev event.Event) {
	select {
	case <-ctx.Done():
		p.log.Error("error sending processed event", slog.Any("event", ev), slog.String("error", ctx.Err().Error()))
	case p.processed <- ev:
		return
	}
}
