package pipeline

import (
	"testing"
	"time"
	"uuid"

	"github.com/neelalala/hopper/internal/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPipeline_IngestEvent(t *testing.T) {
	ev1 := event.Event{
		ID:         uuid.New(),
		Name:       "login",
		UserID:     "user-001",
		UserAgent:  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:159.0) Gecko/20100101 Firefox/159.0",
		IP:         "127.0.0.1",
		ClientTime: time.Now().Add(-time.Second),
		ServerTime: time.Now(),
	}

	ev2 := event.Event{
		ID:         uuid.New(),
		Name:       "login",
		UserID:     "user-002",
		UserAgent:  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:159.0) Gecko/20100101 Firefox/159.0",
		IP:         "127.0.0.2",
		ClientTime: time.Now().Add(-time.Second),
		ServerTime: time.Now(),
	}

	ev3 := event.Event{
		ID:         uuid.New(),
		Name:       "banner_click",
		UserID:     "user-001",
		UserAgent:  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:159.0) Gecko/20100101 Firefox/159.0",
		IP:         "127.0.0.1",
		ClientTime: time.Now().Add(-time.Second),
		ServerTime: time.Now(),
	}

	tests := []struct {
		name       string
		bufferSize int
		wantErr    error
		events     []event.Event
	}{
		{
			name:       "buffered pipeline",
			bufferSize: 10,
			wantErr:    nil,
			events:     []event.Event{ev1, ev2, ev3},
		},
		{
			name:       "full channel",
			bufferSize: 1,
			wantErr:    ErrChanIsFull,
			events:     []event.Event{ev1, ev2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline := New(nil, WithBufferSize(tt.bufferSize), WithWorkerCount(1))

			for _, ev := range tt.events {
				err := pipeline.Ingest(ev)
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
	events := []event.Event{
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

	pipeline := New(nil, WithBufferSize(10), WithWorkerCount(1))

	allSent := make(chan struct{})

	go func() {
		pipeline.Start()
		<-allSent
	}()

	time.Sleep(time.Second)

	for _, ev := range events {
		err := pipeline.Ingest(ev)
		require.NoError(t, err, "ingest event")
		select {
		case got, ok := <-pipeline.Processed():
			require.True(t, ok, "processed evnet chan not closed")
			assert.Equal(t, ev, got, "ingested and processed event with no processors")
		case <-time.After(time.Second):
			t.Error("wanted processed event")
		}
	}
	close(allSent)
}
