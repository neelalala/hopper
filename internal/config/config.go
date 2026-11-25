package config

import "time"

type PipelineConfig struct {
	SinkTimeout   time.Duration `yaml:"sink-timeout" env:"SINK_TIMEOUT"`
	WorkerTimeout time.Duration `yaml:"worker-timeout" env:"PIPELINE_WORKER_TIMEOUT"`
	BufferSize    int           `yaml:"buffer-size" env:"PIPELINE_BUFFER_SIZE"`
	WorkerCount   int           `yaml:"worker-count" env:"PIPELINE_WORKER_COUNT"`
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
	pipeline   PipelineConfig `yaml:"pipeline"`
	batcher    BatcherConfig
	httpServer HTTPServerConfig
	logger     LoggerConfig
}
