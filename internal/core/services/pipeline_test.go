package services

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/neelalala/hopper/internal/core/domain"
	"github.com/neelalala/hopper/internal/core/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPipeline_New(t *testing.T) {
	log := slog.New(slog.DiscardHandler)
	tests := []struct {
		name        string
		bufferSize  int
		workerCount int
		log         *slog.Logger
		wantErr     bool
	}{
		{
			name:        "valid params",
			bufferSize:  10,
			workerCount: 1,
			log:         log,
			wantErr:     false,
		},
		{
			name:        "zero buffer size",
			bufferSize:  0,
			workerCount: 1,
			log:         log,
			wantErr:     true,
		},
		{
			name:        "negative buffer size",
			bufferSize:  -10,
			workerCount: 1,
			log:         log,
			wantErr:     true,
		},
		{
			name:        "nil logger",
			bufferSize:  10,
			workerCount: 1,
			log:         nil,
			wantErr:     true,
		},
		{
			name:        "zero workers",
			bufferSize:  10,
			workerCount: 0,
			log:         log,
			wantErr:     true,
		},
		{
			name:        "negative workers",
			bufferSize:  10,
			workerCount: -1,
			log:         log,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := NewWorkerPoolPipeline(
				[]ports.Processor{},
				tt.bufferSize,
				tt.workerCount,
				tt.log,
			)
			if tt.wantErr {
				assert.Error(t, err, "wanted error")
				assert.Nil(t, pipeline, "pipeline should be nil on error")
			} else {
				assert.NoError(t, err, "wanted no error")
				assert.NotNil(t, pipeline, "pipeline must not be nil if no error")
			}
		})
	}
}

func TestPipeline_IngestEvent(t *testing.T) {
	ev1 := domain.Event{
		ID:         uuid.New(),
		Name:       "login",
		UserID:     "user-001",
		UserAgent:  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:159.0) Gecko/20100101 Firefox/159.0",
		IP:         "127.0.0.1",
		ClientTime: time.Now().Add(-time.Second),
		ServerTime: time.Now(),
	}

	ev2 := domain.Event{
		ID:         uuid.New(),
		Name:       "login",
		UserID:     "user-002",
		UserAgent:  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:159.0) Gecko/20100101 Firefox/159.0",
		IP:         "127.0.0.2",
		ClientTime: time.Now().Add(-time.Second),
		ServerTime: time.Now(),
	}

	ev3 := domain.Event{
		ID:         uuid.New(),
		Name:       "banner_click",
		UserID:     "user-001",
		UserAgent:  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:159.0) Gecko/20100101 Firefox/159.0",
		IP:         "127.0.0.1",
		ClientTime: time.Now().Add(-time.Second),
		ServerTime: time.Now(),
	}

	log := slog.New(slog.DiscardHandler)

	tests := []struct {
		name       string
		bufferSize int
		wantErr    error
		events     []domain.Event
	}{
		{
			name:       "buffered pipeline",
			bufferSize: 10,
			wantErr:    nil,
			events:     []domain.Event{ev1, ev2, ev3},
		},
		{
			name:       "full channel",
			bufferSize: 1,
			wantErr:    domain.ErrChanIsFull,
			events:     []domain.Event{ev1, ev2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := NewWorkerPoolPipeline(nil, tt.bufferSize, 1, log)
			require.NoError(t, err, "pipeline must be created")

			for _, ev := range tt.events {
				err := pipeline.IngestEvent(ev)
				if err != nil {
					if tt.wantErr != nil {
						assert.ErrorIs(t, err, tt.wantErr, "wanted error")
						break
					} else {
						t.Error("unexpected error", "error", err)
					}
				}
			}
		})
	}
}

func TestPipeline_IngestEvent_ProcessedEvent(t *testing.T) {
	log := slog.New(slog.DiscardHandler)

	events := []domain.Event{
		{
			ID:         uuid.New(),
			Name:       "login",
			UserID:     "user-001",
			UserAgent:  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:159.0) Gecko/20100101 Firefox/159.0",
			IP:         "127.0.0.1",
			ClientTime: time.Now().Add(-time.Second),
			ServerTime: time.Now(),
		},
		{
			ID:         uuid.New(),
			Name:       "login",
			UserID:     "user-002",
			UserAgent:  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:159.0) Gecko/20100101 Firefox/159.0",
			IP:         "127.0.0.2",
			ClientTime: time.Now().Add(-time.Second),
			ServerTime: time.Now(),
		},
		{
			ID:         uuid.New(),
			Name:       "banner_click",
			UserID:     "user-001",
			UserAgent:  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:159.0) Gecko/20100101 Firefox/159.0",
			IP:         "127.0.0.1",
			ClientTime: time.Now().Add(-time.Second),
			ServerTime: time.Now(),
		},
	}

	pipeline, err := NewWorkerPoolPipeline(nil, 10, 1, log)
	require.NoError(t, err, "new pipeline")

	allSent := make(chan struct{})

	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		pipeline.Start(ctx)
		<-allSent
	}()

	time.Sleep(time.Second)

	for _, ev := range events {
		err := pipeline.IngestEvent(ev)
		require.NoError(t, err, "ingest event")
		select {
		case got, ok := <-pipeline.ProcessedEvents():
			require.True(t, ok, "processed evnet chan not closed")
			assert.Equal(t, ev, got, "ingested and processed event with no processors")
		case <-time.After(time.Second):
			t.Error("wanted processed event")
		}
	}
	close(allSent)
}
