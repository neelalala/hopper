package config

import "time"

type PipelineConfig struct {
	SinkTimeout   time.Duration `yaml:"sink-timeout" env:"SINK_TIMEOUT"`
	WorkerTimeout time.Duration `yaml:"worker-timeout" env:"PIPELINE_WORKER_TIMEOUT"`
	BufferSize    int           `yaml:"buffer-size" env:"PIPELINE_BUFFER_SIZE"`
	WorkerCount   int           `yaml:"worker-count" env:"PIPELINE_WORKER_COUNT"`
}

type BatcherConfig struct {
	SendTickerDuration time.Duration `yaml:"ticker-duration" env:"BATCHER_TICKER_DURATION"`
	BatchSize          int           `yaml:"batch-size" env:"BATCH_SIZE"`
	EventsBufferSize   int           `yaml:"events-buffer-size" env:"BATCHER_EVENTS_BUFFER"`
	FlushChannelSize   int           `yaml:"flush-channel-size" env:"BATCHER_FLUSH_CHANNEL_SIZE"`
	WorkerCount        int           `yaml:"worker-count" env:"BATCHER_WORKER_COUNT"`
	WorkerTimeout      time.Duration `yaml:"worker-timeout" env:"BATCHER_WORKER_TIMEOUT"`
}

type HTTPServerConfig struct {
	Address string
}

type LoggerConfig struct {
	Level     string
	AddSource bool
}

type Config struct {
	pipeline   PipelineConfig   `yaml:"pipeline"`
	batcher    BatcherConfig    `yaml:"batcher"`
	httpServer HTTPServerConfig
	logger     LoggerConfig
}
