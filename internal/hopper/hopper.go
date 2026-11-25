package hopper

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/neelalala/hopper/internal/event"
)

const DefaultSinkTimeout = 10 * time.Second

var (
	ErrHopperClosed  = errors.New("hopper is closed")
	ErrAlreadyClosed = errors.New("already closed")
)

type Option func(h *Hopper)

type Pipeline interface {
	Ingest(event.Event) error
	Processed() <-chan event.Event
}

type Sink func(ctx context.Context, ev event.Event) error

type Hopper struct {
	pipeline Pipeline
	sink     Sink

	sinkTimeout time.Duration

	closed *atomic.Bool

	stopCtx    context.Context
	stopCancel context.CancelFunc
	done       chan struct{}

	log *slog.Logger
}

func New(pipeline Pipeline, sink Sink, opts ...Option) *Hopper {
	stopCtx, stopCancel := context.WithCancel(context.Background())

	h := &Hopper{
		pipeline:    pipeline,
		sink:        sink,
		sinkTimeout: DefaultSinkTimeout,
		stopCtx:     stopCtx,
		stopCancel:  stopCancel,
		done:        make(chan struct{}),
		log:         slog.New(slog.DiscardHandler),
	}

	for _, opt := range opts {
		opt(h)
	}

	return h
}

func (h *Hopper) Ingest(ev event.Event) error {
	if h.closed.Load() {
		h.log.Warn("ingesting in closed hopper", slog.Any("event", ev))
		return ErrHopperClosed
	}

	h.log.Debug("ingesting event", slog.Any("event", ev))
	return h.pipeline.Ingest(ev)
}

func (h *Hopper) Start() {
	defer h.log.Info("hopper stopped")
	h.log.Info("hopper started")

	for {
		select {

		case <-h.stopCtx.Done():
			return

		case ev, ok := <-h.pipeline.Processed():
			if !ok {
				h.log.Info("events drained")
				return
			}

			if h.stopCtx.Err() != nil {
				h.log.Warn("processed events left")
				return
			}

			sinkCtx, cancel := context.WithTimeout(h.stopCtx, h.sinkTimeout)
			err := h.sink(sinkCtx, ev)
			cancel()
			if err != nil {
				h.log.Error(
					"error sending processed event",
					slog.String("error", err.Error()),
					slog.Any("event", ev),
				)
			}

		}
	}
}

func (h *Hopper) Shutdown(ctx context.Context) error {
	if old := h.closed.Swap(true); old == true {
		return ErrAlreadyClosed
	}

	select {
	case <-ctx.Done():
		h.stopCancel()
		return ctx.Err()
	case <-h.done:
		return nil
	}
}
