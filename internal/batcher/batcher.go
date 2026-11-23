package batcher

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/neelalala/hopper/internal/event"
)

const (
	DefaultBatchSendTickerDuration = time.Second
	DefaultFlushChanSize           = 10
	DefaultEventsBufferSize        = 1_000
	DefaultBatchSize               = 10_000
	DefaultWorkerCount             = 1
)

type Option func(*Batcher)

type EventRepository interface {
	CreateEvents(ctx context.Context, events []event.Event) error
}

type Batcher struct {
	eventsChSize            int
	flushChSize             int
	batchSendTickerDuration time.Duration

	workerCount int
	batchSize   int

	eventsCh chan event.Event
	flushCh  chan []event.Event
	ticker   *time.Ticker
	evRepo   EventRepository

	log *slog.Logger
}

func New(evRepo EventRepository, opts ...Option) *Batcher {
	b := &Batcher{
		eventsChSize:            DefaultEventsBufferSize, // may be use DefaultBatchSize as default?
		flushChSize:             DefaultFlushChanSize,
		batchSendTickerDuration: DefaultBatchSendTickerDuration,
		workerCount:             DefaultWorkerCount,
		batchSize:               DefaultBatchSize,
		log:                     slog.New(slog.DiscardHandler),
	}

	for _, opt := range opts {
		opt(b)
	}

	b.eventsCh = make(chan event.Event, b.eventsChSize)
	b.flushCh = make(chan []event.Event, b.flushChSize)
	b.ticker = time.NewTicker(b.batchSendTickerDuration)
	b.evRepo = evRepo

	return b
}

func (b *Batcher) Add(ctx context.Context, ev event.Event) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case b.eventsCh <- ev:
		return nil
	}
}

func (b *Batcher) Start(ctx context.Context) {
	b.log.Info("batcher started")

	go b.startWorkers(ctx)
	b.startLoop(ctx)

	b.log.Info("batcher stopped")
}

func (b *Batcher) startLoop(ctx context.Context) {
	batch := make([]event.Event, 0, b.batchSize)
	for {
		var (
			ev event.Event
			ok bool
		)
		select {
		case <-ctx.Done():
			return
		case ev, ok = <-b.eventsCh:
		}
		if ctx.Err() != nil || !ok {
			return
		}

		batch = append(batch, ev)

		if len(batch) == b.batchSize {
			batch = b.flush(batch)
		}
	}
}

func (b *Batcher) flush(batch []event.Event) []event.Event {
	if len(batch) > 0 {
		b.flushCh <- batch
		batch = make([]event.Event, 0, b.batchSize)
	}
	return batch
}

func (b *Batcher) startWorkers(ctx context.Context) {
	var wg sync.WaitGroup

	for range b.workerCount {
		wg.Go(func() {
			for {
				var (
					batch []event.Event
					ok    bool
				)

				select {
				case <-ctx.Done():
				case batch, ok = <-b.flushCh:
				}
				if ctx.Err() != nil || !ok {
					return
				}

				if err := b.evRepo.CreateEvents(ctx, batch); err != nil {
					b.log.Error("save events in repository", slog.String("error", err.Error()))
				}
			}
		})
	}

	wg.Wait()
}
