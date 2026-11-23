package batcher

import (
	"log/slog"
	"time"
)

func WithBatchSize(size int) Option {
	return func(b *Batcher) {
		if size <= 0 {
			panic("not positive batch size")
		}
		b.batchSize = size
	}
}

func WithFlushChanBufferSize(size int) Option {
	return func(b *Batcher) {
		if size < 0 {
			panic("negative flush buffer size")
		}
		b.flushChSize = size
	}
}

func WithEventsChanBufferSize(size int) Option {
	return func(b *Batcher) {
		if size < 0 {
			panic("negative eevnts buffer size")
		}
		b.flushChSize = size
	}
}

func WithWorkerCount(count int) Option {
	return func(b *Batcher) {
		if count <= 0 {
			panic("not positive worker count")
		}
		b.workerCount = count
	}
}

func WithSendTickerDuration(duration time.Duration) Option {
	return func(b *Batcher) {
		if duration < 0 {
			panic("negative ticker duration")
		}
		b.batchSendTickerDuration = duration
	}
}

func WithLogger(log *slog.Logger) Option {
	return func(b *Batcher) {
		if log == nil {
			log = slog.New(slog.DiscardHandler)
		}
		b.log = log
	}
}
