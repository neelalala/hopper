package config

import (
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

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
	// TODO: timeouts
	Address string `yaml:"address" env:"HTTP_SERVER_ADDRESS"`
}

type ClickhouseConfig struct {
	Address  string `yaml:"address" env:"CLICKHOUSE_ADDRESS"`
	Database string `yaml:"database" env:"CLICKHOUSE_DATABASE"`
	Username string `yaml:"username" env:"CLICKHOUSE_USERNAME"`
	Password string `yaml:"password" env:"CLICKHOUSE_PASSWORD"`
}

type LoggerConfig struct {
	Level     string `yaml:"level" env:"LOG_LEVEL"`
	AddSource bool   `yaml:"add-sorce" env:"LOG_ADD_SOURCE"`
}

type Config struct {
	Pipeline   PipelineConfig   `yaml:"pipeline"`
	Batcher    BatcherConfig    `yaml:"batcher"`
	HTTPServer HTTPServerConfig `yaml:"http-server"`
	Clickhouse ClickhouseConfig `yaml:"clickhouse"`
	Logger     LoggerConfig     `yaml:"logger"`
}

func Parse(configPath string) (Config, error) {
	var cfg Config
	err := cleanenv.ReadConfig(configPath, &cfg)
	if err != nil {
		return Config{}, err
	}

	return cfg, nil
}
