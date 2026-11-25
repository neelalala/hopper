package pipeline

import "log/slog"

func WithBufferSize(size int) Option {
	if size < 0 {
		panic("negative buffer size")
	}
	return func(p *Pipeline) {
		p.eventsBufferSize = size
		p.processedBufferSize = size
	}
}

func WithEventsBufferSize(size int) Option {
	if size < 0 {
		panic("negative events buffer size")
	}
	return func(p *Pipeline) {
		p.eventsBufferSize = size
	}
}

func WithProcessedBufferSize(size int) Option {
	if size < 0 {
		panic("negative buffer size")
	}
	return func(p *Pipeline) {
		p.processedBufferSize = size
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

func WithLogger(log *slog.Logger) Option {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return func(p *Pipeline) {
		p.log = log
	}
}
