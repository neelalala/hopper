package pipeline

import "log/slog"

func WithBufferSize(size int) Option {
	return func(p *Pipeline) {
		if size < 0 {
			panic("buffer size must not be negative")
		}
		p.eventsBufferSize = size
		p.processedBufferSize = size
	}
}

func WithEventsBufferSize(size int) Option {
	return func(p *Pipeline) {
		if size < 0 {
			panic("events buffer size must not be negative")
		}
		p.eventsBufferSize = size
	}
}

func WithProcessedBufferSize(size int) Option {
	return func(p *Pipeline) {
		if size < 0 {
			panic("processed buffer size must not be negative")
		}
		p.processedBufferSize = size
	}
}

func WithWorkerCount(count int) Option {
	return func(p *Pipeline) {
		if count < 1 {
			panic("worker count must be positive")
		}
		p.workerCount = count
	}
}

func WithLogger(log *slog.Logger) Option {
	return func(p *Pipeline) {
		if log == nil {
			log = slog.New(slog.DiscardHandler)
		}
		p.log = log
	}
}
