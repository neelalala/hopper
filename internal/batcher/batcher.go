package batcher

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/neelalala/hopper/internal/event"
)

const (
	DefaultBatchSendTickerDuration = time.Second
	DefaultFlushChanSize           = 10
	DefaultBatchSize               = 10_000
	DefaultEventsBufferSize        = DefaultBatchSize
	DefaultWorkerCount             = 1
	DefaultWorkerTimeout           = 30 * time.Second
)

var (
	ErrShuttingDown    = errors.New("shutting down")
	ErrAlreadyStoppped = errors.New("already stopped")
)

type Option func(*Batcher)

type EventRepository interface {
	CreateEvents(ctx context.Context, events []event.Event) error
}

type Batcher struct {
	eventsChSize            int
	flushChSize             int
	batchSendTickerDuration time.Duration

	workerCount   int
	workerTimeout time.Duration
	batchSize     int

	mu       sync.RWMutex
	eventsCh chan event.Event

	flushCh chan []event.Event
	evRepo  EventRepository

	stopCtx    context.Context
	stopCancel context.CancelFunc
	closeOnce  sync.Once
	closing    chan struct{}
	done       chan struct{}

	log *slog.Logger
}

func New(evRepo EventRepository, opts ...Option) *Batcher {
	stopCtx, stopCancel := context.WithCancel(context.Background())

	b := &Batcher{
		eventsChSize:            DefaultEventsBufferSize,
		flushChSize:             DefaultFlushChanSize,
		batchSendTickerDuration: DefaultBatchSendTickerDuration,
		workerCount:             DefaultWorkerCount,
		workerTimeout:           DefaultWorkerTimeout,
		batchSize:               DefaultBatchSize,
		evRepo:                  evRepo,
		stopCtx:                 stopCtx,
		stopCancel:              stopCancel,
		done:                    make(chan struct{}),
		log:                     slog.New(slog.DiscardHandler),
	}

	for _, opt := range opts {
		opt(b)
	}

	b.eventsCh = make(chan event.Event, b.eventsChSize)
	b.flushCh = make(chan []event.Event, b.flushChSize)

	return b
}

func (b *Batcher) Add(ctx context.Context, ev event.Event) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	select {
	case <-b.closing:
		return ErrShuttingDown
	default:
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case b.eventsCh <- ev:
		return nil
	case <-b.closing:
		return ErrShuttingDown
	}
}

func (b *Batcher) Start() {
	defer func() {
		b.stopCancel()
		close(b.done)

		b.log.Info("batcher stopped")
	}()
	b.log.Info("batcher started")

	var workersWg sync.WaitGroup
	workersWg.Go(b.startWorkers)
	b.startLoop()
	close(b.flushCh)
	workersWg.Wait()
}

func (b *Batcher) Shutdown(ctx context.Context) error {
	alreadyStopped := true
	b.closeOnce.Do(func() {
		alreadyStopped = false
		close(b.closing)
	})
	if alreadyStopped {
		return ErrAlreadyStoppped
	}

	b.mu.Lock()
	close(b.eventsCh)
	b.mu.Unlock()

	select {
	case <-ctx.Done():
		b.stopCancel()
		return ctx.Err()
	case <-b.done:
		return nil
	}
}

func (b *Batcher) startLoop() {
	ticker := time.NewTicker(b.batchSendTickerDuration)
	defer ticker.Stop()

	batch := make([]event.Event, 0, b.batchSize)
	for {
		select {

		case <-b.stopCtx.Done():
			if n := len(b.eventsCh) + len(batch); n > 0 {
				b.log.Warn("events left in batcher", slog.Int("count", n))
			}
			return

		case ev, ok := <-b.eventsCh:
			if !ok {
				b.flush(batch)
				b.log.Info("events drained")
				return
			}

			if b.stopCtx.Err() != nil {
				b.log.Warn("events left in batcher", slog.Int("count", len(b.eventsCh)+len(batch)+1))
				return
			}

			batch = append(batch, ev)

			if len(batch) == b.batchSize {
				batch = b.flush(batch)
				ticker.Reset(b.batchSendTickerDuration)
			}

		case <-ticker.C:
			batch = b.flush(batch)
		}
	}
}

func (b *Batcher) flush(batch []event.Event) []event.Event {
	if len(batch) == 0 {
		return batch
	}
	select {
	case b.flushCh <- batch:
		return make([]event.Event, 0, b.batchSize)
	case <-b.stopCtx.Done():
		b.log.Warn("batch dropped on shutdown", slog.Int("count", len(batch)))
		return nil
	}
}

func (b *Batcher) startWorkers() {
	var wg sync.WaitGroup
	for range b.workerCount {
		wg.Go(func() {
			for {
				select {

				case <-b.stopCtx.Done():
					if n := len(b.flushCh); n > 0 {
						b.log.Warn("batches left in bathcer", slog.Int("count", n))
					}
					return

				case batch, ok := <-b.flushCh:
					if !ok {
						b.log.Info("batches drained")
						return
					}

					if b.stopCtx.Err() != nil {
						b.log.Warn("bathces left in batcher", slog.Int("count", len(b.flushCh)+1))
						return
					}

					workCtx, workCancel := context.WithTimeout(b.stopCtx, b.workerTimeout)
					err := b.evRepo.CreateEvents(workCtx, batch)
					workCancel()
					if err != nil {
						b.log.Error("save events in repository", slog.String("error", err.Error()))
					}

				}
			}
		})
	}

	wg.Wait()
}
