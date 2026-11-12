package services

import (
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
	tests := []struct {
		name       string
		bufferSize int
		log        *slog.Logger
		wantErr    bool
	}{
		{
			name:       "valid params",
			bufferSize: 10,
			log:        slog.New(slog.DiscardHandler),
			wantErr:    false,
		},
		{
			name:       "zero buffer size",
			bufferSize: 0,
			log:        slog.New(slog.DiscardHandler),
			wantErr:    true,
		},
		{
			name:       "negative buffer size",
			bufferSize: -10,
			log:        slog.New(slog.DiscardHandler),
			wantErr:    true,
		},
		{
			name:       "nil logger",
			bufferSize: 10,
			log:        nil,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, err := NewPipeline([]ports.Processor{}, tt.bufferSize, tt.log)
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
			pipeline, err := NewPipeline(nil, tt.bufferSize, log)
			require.NoError(t, err, "pipeline must be created")

			for _, e := range tt.events {
				err := pipeline.IngestEvent(e)
				if err != nil {
					assert.ErrorIs(t, err, tt.wantErr, "wanted error")
					break
				}
			}
		})
	}
}
