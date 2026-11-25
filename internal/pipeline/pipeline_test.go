package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"
	"uuid"

	"github.com/neelalala/hopper/internal/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockSink struct {
	mock.Mock
}

func (m *mockSink) Sink(ctx context.Context, ev event.Event) error {
	args := m.Called(ctx, ev)
	return args.Error(0)
}

type mockProcessor struct {
	mock.Mock
}

func (m *mockProcessor) Process(ctx context.Context, ev *event.Event) (bool, error) {
	args := m.Called(ctx, ev)
	return args.Bool(0), args.Error(1)
}

func newTestEvent(name, userID string) event.Event {
	now := time.Now()
	return event.Event{
		ID:         uuid.New(),
		Name:       name,
		UserID:     userID,
		UserAgent:  "Mozilla/5.0",
		IP:         "127.0.0.1",
		ClientTime: now.Add(-time.Second),
		ServerTime: now,
	}
}

func TestPipeline_Ingest(t *testing.T) {
	ev1 := newTestEvent("login", "user-001")
	ev2 := newTestEvent("login", "user-002")
	ev3 := newTestEvent("banner_click", "user-001")

	tests := []struct {
		name       string
		bufferSize int
		events     []event.Event
		wantErr    error
	}{
		{
			name:       "buffered pipeline accepts all events",
			bufferSize: 10,
			events:     []event.Event{ev1, ev2, ev3},
			wantErr:    nil,
		},
		{
			name:       "full channel returns ErrChanIsFull",
			bufferSize: 1,
			events:     []event.Event{ev1, ev2},
			wantErr:    ErrChanIsFull,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sink := new(mockSink)

			p := New(nil, sink.Sink, WithBufferSize(tt.bufferSize), WithWorkerCount(1))

			var lastErr error
			for _, ev := range tt.events {
				if err := p.Ingest(ev); err != nil {
					lastErr = err
					break
				}
			}

			if tt.wantErr != nil {
				require.ErrorIs(t, lastErr, tt.wantErr)
			} else {
				require.NoError(t, lastErr)
			}
		})
	}
}

func TestPipeline_ProcessAndSendToSink(t *testing.T) {
	ev1 := newTestEvent("login", "user-001")
	ev2 := newTestEvent("banner_click", "user-002")

	want1 := ev1
	want1.Name = "login_enriched"
	want2 := ev2
	want2.Name = "banner_click_enriched"

	proc := new(mockProcessor)

	proc.On("Process", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			ev := args.Get(1).(*event.Event)
			ev.Name += "_enriched"
		}).
		Return(false, nil).
		Times(2)

	sink := new(mockSink)
	sink.On("Sink", mock.Anything, want1).Return(nil).Once()
	sink.On("Sink", mock.Anything, want2).Return(nil).Once()

	p := New([]Processor{proc}, sink.Sink, WithBufferSize(10), WithWorkerCount(2))
	go p.Start()

	require.NoError(t, p.Ingest(ev1))
	require.NoError(t, p.Ingest(ev2))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, p.Shutdown(ctx))

	proc.AssertExpectations(t)
	sink.AssertExpectations(t)
}

func TestPipeline_ProcessorDropsEvents(t *testing.T) {
	tests := []struct {
		name    string
		drop    bool
		procErr error
	}{
		{
			name:    "processor drops event",
			drop:    true,
			procErr: nil,
		},
		{
			name:    "processor returns error",
			drop:    false,
			procErr: errors.New("error"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			first := new(mockProcessor)
			second := new(mockProcessor)
			sink := new(mockSink)

			first.On("Process", mock.Anything, mock.Anything).
				Return(tt.drop, tt.procErr).
				Once()

			p := New([]Processor{first, second}, sink.Sink, WithBufferSize(10), WithWorkerCount(1))
			go p.Start()

			require.NoError(t, p.Ingest(newTestEvent("login", "user-001")))

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			require.NoError(t, p.Shutdown(ctx))

			first.AssertExpectations(t)
			second.AssertNotCalled(t, "Process", mock.Anything, mock.Anything)
			sink.AssertNotCalled(t, "Sink", mock.Anything, mock.Anything)
		})
	}
}

func TestPipeline_ShutdownErrors(t *testing.T) {
	sink := new(mockSink)
	p := New(nil, sink.Sink, WithBufferSize(10), WithWorkerCount(1))
	go p.Start()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	require.NoError(t, p.Shutdown(ctx))
	assert.ErrorIs(t, p.Shutdown(ctx), ErrAlreadyStopped)
	assert.ErrorIs(t, p.Ingest(newTestEvent("login", "user-001")), ErrShuttingDown)
}

func TestPipeline_ShutdownTimeout(t *testing.T) {
	ev := newTestEvent("slow_event", "user-001")
	sinkStarted := make(chan struct{})

	sink := new(mockSink)
	sink.On("Sink", mock.Anything, ev).
		Run(func(args mock.Arguments) {
			close(sinkStarted)

			ctx := args.Get(0).(context.Context)
			<-ctx.Done()
		}).
		Return(context.Canceled).
		Once()

	p := New(nil, sink.Sink, WithBufferSize(10), WithWorkerCount(1))
	go p.Start()

	require.NoError(t, p.Ingest(ev))

	select {
	case <-sinkStarted:
	case <-time.After(time.Second):
		t.Fatal("sink was not called in time")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := p.Shutdown(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	select {
	case <-p.done:
	case <-time.After(time.Second):
		t.Fatal("pipeline did not stop after cancel")
	}

	sink.AssertExpectations(t)
}
