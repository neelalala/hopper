package hopper

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"

	"github.com/neelalala/hopper/internal/event"
)

var ErrHopperClosed = errors.New("hopper is closed")

type Pipeline interface {
	Ingest(event.Event) error
	Processed() <-chan event.Event
}

type Sink func(ctx context.Context, ev event.Event) error

type Hopper struct {
	pipeline Pipeline
	sink     Sink

	done atomic.Bool

	log *slog.Logger
}

func New(pipeline Pipeline, sink Sink, log *slog.Logger) *Hopper {
	return &Hopper{
		pipeline: pipeline,
		sink:     sink,
		log:      log,
	}
}

func (h *Hopper) Ingest(_ context.Context, ev event.Event) error {
	if h.done.Load() {
		h.log.Warn("ingesting in closed hopper", slog.Any("event", ev))
		return ErrHopperClosed
	}

	h.log.Debug("ingesting event", slog.Any("event", ev))
	return h.pipeline.Ingest(ev)
}

func (h *Hopper) Start(ctx context.Context) {
	h.log.Info("starting hopper")

	for {
		select {

		case <-ctx.Done():
			h.done.Store(true)
			return

		case ev := <-h.pipeline.Processed():
			err := h.sink(ctx, ev)
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
