package pipeline

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
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

func TestPipeline_Options(t *testing.T) {
	sink := new(mockSink)
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	p := New(
		nil,
		sink.Sink,
		WithBufferSize(128),
		WithWorkerCount(5),
		WithWorkerTimeout(3*time.Second),
		WithSinkTimeout(7*time.Second),
		WithLogger(logger),
	)

	assert.Equal(t, 128, p.eventsBufferSize)
	assert.Equal(t, 128, cap(p.events))
	assert.Equal(t, 5, p.workerCount)
	assert.Equal(t, 3*time.Second, p.workerTimeout)
	assert.Equal(t, 7*time.Second, p.sinkTimeout)
	assert.Same(t, logger, p.log)

	pZero := New(nil, sink.Sink, WithBufferSize(0), WithLogger(nil))
	assert.Equal(t, 0, cap(pZero.events))
	assert.NotNil(t, pZero.log)

	invalidOpts := []struct {
		name string
		fn   func()
	}{
		{name: "negative buffer size", fn: func() { WithBufferSize(-1) }},
		{name: "zero worker count", fn: func() { WithWorkerCount(0) }},
		{name: "negative worker count", fn: func() { WithWorkerCount(-1) }},
		{name: "zero worker timeout", fn: func() { WithWorkerTimeout(0) }},
		{name: "negative worker timeout", fn: func() { WithWorkerTimeout(-time.Millisecond) }},
		{name: "zero sink timeout", fn: func() { WithSinkTimeout(0) }},
		{name: "negative sink timeout", fn: func() { WithSinkTimeout(-time.Millisecond) }},
	}

	for _, tc := range invalidOpts {
		t.Run(tc.name, func(t *testing.T) {
			require.Panics(t, tc.fn)
		})
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

func TestPipeline_MultipleProcessorsChain(t *testing.T) {
	ev := newTestEvent("purchase", "user-001")
	want := ev
	want.Name = "purchase_validated"
	want.UserOS = "Linux"

	first := new(mockProcessor)
	second := new(mockProcessor)
	sink := new(mockSink)

	first.On("Process", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			e := args.Get(1).(*event.Event)
			e.Name += "_validated"
		}).
		Return(false, nil).
		Once()

	second.On("Process", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			e := args.Get(1).(*event.Event)
			assert.Equal(t, "purchase_validated", e.Name)
			e.UserOS = "Linux"
		}).
		Return(false, nil).
		Once()

	sink.On("Sink", mock.Anything, want).Return(nil).Once()

	p := New([]Processor{first, second}, sink.Sink, WithBufferSize(10), WithWorkerCount(1))
	go p.Start()

	require.NoError(t, p.Ingest(ev))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, p.Shutdown(ctx))

	first.AssertExpectations(t)
	second.AssertExpectations(t)
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

func TestPipeline_SinkErrorDoesNotStopPipeline(t *testing.T) {
	ev1 := newTestEvent("ev1", "user-001")
	ev2 := newTestEvent("ev2", "user-002")

	sink := new(mockSink)
	sink.On("Sink", mock.Anything, ev1).Return(errors.New("temporary sink failure")).Once()
	sink.On("Sink", mock.Anything, ev2).Return(nil).Once()

	p := New(nil, sink.Sink, WithBufferSize(10), WithWorkerCount(1))
	go p.Start()

	require.NoError(t, p.Ingest(ev1))
	require.NoError(t, p.Ingest(ev2))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, p.Shutdown(ctx))

	sink.AssertExpectations(t)
}

func TestPipeline_WorkerTimeout(t *testing.T) {
	evSlow := newTestEvent("slow", "user-001")
	evFast := newTestEvent("fast", "user-002")

	procErrCh := make(chan error, 1)
	proc := new(mockProcessor)
	proc.On("Process", mock.Anything, mock.MatchedBy(func(ev *event.Event) bool {
		return ev.ID == evSlow.ID
	})).
		Run(func(args mock.Arguments) {
			ctx := args.Get(0).(context.Context)
			<-ctx.Done()
			procErrCh <- ctx.Err()
		}).
		Return(false, context.DeadlineExceeded).
		Once()

	proc.On("Process", mock.Anything, mock.MatchedBy(func(ev *event.Event) bool {
		return ev.ID == evFast.ID
	})).
		Return(false, nil).
		Once()

	sink := new(mockSink)
	sink.On("Sink", mock.Anything, evFast).Return(nil).Once()

	p := New(
		[]Processor{proc},
		sink.Sink,
		WithBufferSize(10),
		WithWorkerCount(1),
		WithWorkerTimeout(25*time.Millisecond),
		WithSinkTimeout(time.Second),
	)
	go p.Start()

	require.NoError(t, p.Ingest(evSlow))
	select {
	case err := <-procErrCh:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for worker timeout")
	}

	require.NoError(t, p.Ingest(evFast))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, p.Shutdown(ctx))

	proc.AssertExpectations(t)
	sink.AssertExpectations(t)
	sink.AssertNotCalled(t, "Sink", mock.Anything, evSlow)
}

func TestPipeline_SinkTimeout(t *testing.T) {
	evSlow := newTestEvent("slow", "user-001")
	evFast := newTestEvent("fast", "user-002")

	sinkErrCh := make(chan error, 1)
	sink := new(mockSink)
	sink.On("Sink", mock.Anything, evSlow).
		Run(func(args mock.Arguments) {
			ctx := args.Get(0).(context.Context)
			<-ctx.Done()
			sinkErrCh <- ctx.Err()
		}).
		Return(context.DeadlineExceeded).
		Once()
	sink.On("Sink", mock.Anything, evFast).Return(nil).Once()

	p := New(
		nil,
		sink.Sink,
		WithBufferSize(10),
		WithWorkerCount(1),
		WithWorkerTimeout(time.Second),
		WithSinkTimeout(25*time.Millisecond),
	)
	go p.Start()

	require.NoError(t, p.Ingest(evSlow))
	select {
	case err := <-sinkErrCh:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for sink timeout")
	}

	require.NoError(t, p.Ingest(evFast))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, p.Shutdown(ctx))

	sink.AssertExpectations(t)
}

func TestPipeline_ShutdownDrainsBufferedEvents(t *testing.T) {
	ev1 := newTestEvent("ev1", "user-001")
	ev2 := newTestEvent("ev2", "user-002")
	ev3 := newTestEvent("ev3", "user-003")
	ev4 := newTestEvent("ev4", "user-004")

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})

	sink := new(mockSink)
	sink.On("Sink", mock.Anything, ev1).
		Run(func(args mock.Arguments) {
			close(firstStarted)
			<-releaseFirst
		}).
		Return(nil).
		Once()
	sink.On("Sink", mock.Anything, ev2).Return(nil).Once()
	sink.On("Sink", mock.Anything, ev3).Return(nil).Once()
	sink.On("Sink", mock.Anything, ev4).Return(nil).Once()

	p := New(nil, sink.Sink, WithBufferSize(3), WithWorkerCount(1))
	go p.Start()

	require.NoError(t, p.Ingest(ev1))
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first event in sink")
	}

	// With the sole worker blocked in sink, these remain buffered in p.events.
	require.NoError(t, p.Ingest(ev2))
	require.NoError(t, p.Ingest(ev3))
	require.NoError(t, p.Ingest(ev4))

	shutdownErrCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		shutdownErrCh <- p.Shutdown(ctx)
	}()

	// Wait until Shutdown has closed p.events before unblocking the worker.
	require.Eventually(t, func() bool {
		return errors.Is(p.Ingest(newTestEvent("probe", "user-000")), ErrShuttingDown)
	}, time.Second, time.Millisecond)

	close(releaseFirst)

	select {
	case err := <-shutdownErrCh:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Shutdown")
	}

	sink.AssertExpectations(t)
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
	sinkErrCh := make(chan error, 1)

	sink := new(mockSink)
	sink.On("Sink", mock.Anything, ev).
		Run(func(args mock.Arguments) {
			close(sinkStarted)

			ctx := args.Get(0).(context.Context)
			<-ctx.Done()
			sinkErrCh <- ctx.Err()
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

	select {
	case sinkErr := <-sinkErrCh:
		require.ErrorIs(t, sinkErr, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("sink context was not canceled")
	}

	sink.AssertExpectations(t)
}

func TestPipeline_ShutdownTimeoutInProcessor(t *testing.T) {
	ev := newTestEvent("slow_proc", "user-001")
	procStarted := make(chan struct{})

	proc := new(mockProcessor)
	proc.On("Process", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			close(procStarted)
			ctx := args.Get(0).(context.Context)
			<-ctx.Done()
		}).
		Return(false, context.Canceled).
		Once()

	sink := new(mockSink)

	p := New([]Processor{proc}, sink.Sink, WithBufferSize(10), WithWorkerCount(1))
	go p.Start()

	require.NoError(t, p.Ingest(ev))

	select {
	case <-procStarted:
	case <-time.After(time.Second):
		t.Fatal("processor was not called in time")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := p.Shutdown(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	select {
	case <-p.done:
	case <-time.After(time.Second):
		t.Fatal("pipeline did not stop after cancel")
	}

	proc.AssertExpectations(t)
	sink.AssertNotCalled(t, "Sink", mock.Anything, mock.Anything)
}

func TestPipeline_ConcurrentIngestAndShutdown(t *testing.T) {
	var processed atomic.Int64
	sink := func(_ context.Context, _ event.Event) error {
		processed.Add(1)
		return nil
	}

	p := New(nil, sink, WithBufferSize(64), WithWorkerCount(4))
	go p.Start()

	var accepted atomic.Int64
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for {
				err := p.Ingest(newTestEvent("concurrent", "user-001"))
				if err == nil {
					accepted.Add(1)
					continue
				}
				if errors.Is(err, ErrChanIsFull) {
					time.Sleep(time.Millisecond)
					continue
				}
				assert.ErrorIs(t, err, ErrShuttingDown)
				return
			}
		})
	}

	time.Sleep(15 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, p.Shutdown(ctx))

	wg.Wait()
	assert.Equal(t, accepted.Load(), processed.Load())
}
