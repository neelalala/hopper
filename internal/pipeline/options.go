package pipeline

import (
	"log/slog"
	"time"
)

func WithBufferSize(size int) Option {
	if size < 0 {
		panic("negative buffer size")
	}
	return func(p *Pipeline) {
		p.eventsBufferSize = size
	}
}

func WithWorkerCount(count int) Option {
	if count < 1 {
		panic("worker count must be positive")
	}
	return func(p *Pipeline) {
		p.workerCount = count
	}
}

func WithWorkerTimeout(timeout time.Duration) Option {
	if timeout <= 0 {
		panic("worker timeout must be positive")
	}
	return func(p *Pipeline) {
		p.workerTimeout = timeout
	}
}

func WithSinkTimeout(timeout time.Duration) Option {
	if timeout <= 0 {
		panic("sink timeout must be positive")
	}
	return func(p *Pipeline) {
		p.sinkTimeout = timeout
	}
}

func WithLogger(log *slog.Logger) Option {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return func(p *Pipeline) {
		p.log = log
	}
}
