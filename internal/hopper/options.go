package hopper

import (
	"log/slog"
	"time"
)

func WithSinkTimeout(timeout time.Duration) Option {
	if timeout <= 0 {
		panic("sink timeout must be positive")
	}
	return func(h *Hopper) {
		h.sinkTimeout = timeout
	}
}

func WithLogger(log *slog.Logger) Option {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return func(h *Hopper) {
		h.log = log
	}
}
