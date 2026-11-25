package batcher

import (
	"log/slog"
	"time"
)

func WithBatchSize(size int) Option {
	if size <= 0 {
		panic("batch size must be postitive")
	}
	return func(b *Batcher) {
		b.batchSize = size
	}
}

func WithFlushChanBufferSize(size int) Option {
	if size < 0 {
		panic("negative flush buffer size")
	}
	return func(b *Batcher) {
		b.flushChSize = size
	}
}

func WithEventsChanBufferSize(size int) Option {
	if size < 0 {
		panic("negative evnts buffer size")
	}
	return func(b *Batcher) {
		b.flushChSize = size
	}
}

func WithWorkerCount(count int) Option {
	if count <= 0 {
		panic("worker count must be postitive")
	}
	return func(b *Batcher) {
		b.workerCount = count
	}
}

func WithSendTickerDuration(duration time.Duration) Option {
	if duration < 0 {
		panic("negative ticker duration")
	}
	return func(b *Batcher) {
		b.batchSendTickerDuration = duration
	}
}

func WithLogger(log *slog.Logger) Option {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return func(b *Batcher) {
		b.log = log
	}
}
