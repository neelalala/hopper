package config

import "time"

type PipelineConfig struct {
	SinkTimeout   time.Duration
	WorkerTimeout time.Duration
	BufferSize    int
	WorkerCount   int
}

type BatcherConfig struct {
	SendTickerDuration time.Duration
	BatchSize          int
	EventsBufferSize   int
	FlushChannelSize   int
	WorkerCount        int
	WorkerTimeout      time.Duration
}

type HTTPServerConfig struct {
	Address string
}

type LoggerConfig struct {
	Level     string
	AddSource bool
}

type Config struct {
	pipeline   PipelineConfig
	batcher    BatcherConfig
	httpServer HTTPServerConfig
	logger     LoggerConfig
}
