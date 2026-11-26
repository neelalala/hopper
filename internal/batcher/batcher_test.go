package batcher

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/neelalala/hopper/internal/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"uuid"
)

type mockRepo struct {
	mu      sync.Mutex
	batches [][]event.Event
	err     error
	fn      func(ctx context.Context, events []event.Event) error
	called  chan struct{}
}

func newMockRepo() *mockRepo {
	return &mockRepo{
		called: make(chan struct{}, 100),
	}
}

func (m *mockRepo) CreateEvents(ctx context.Context, events []event.Event) error {
	m.mu.Lock()
	cp := make([]event.Event, len(events))
	copy(cp, events)
	m.batches = append(m.batches, cp)
	fn := m.fn
	err := m.err
	m.mu.Unlock()

	defer func() {
		select {
		case m.called <- struct{}{}:
		default:
		}
	}()

	if fn != nil {
		return fn(ctx, events)
	}
	return err
}

func (m *mockRepo) getBatches() [][]event.Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	res := make([][]event.Event, len(m.batches))
	copy(res, m.batches)
	return res
}

func (m *mockRepo) totalEvents() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	total := 0
	for _, batch := range m.batches {
		total += len(batch)
	}
	return total
}

func newTestEvent(name string) event.Event {
	now := time.Now()
	return event.Event{
		ID:         uuid.New(),
		Name:       name,
		UserID:     "user-1",
		UserAgent:  "Mozilla/5.0",
		IP:         "127.0.0.1",
		ClientTime: now.Add(-time.Second),
		ServerTime: now,
	}
}

func TestBatcher_Options(t *testing.T) {
	repo := newMockRepo()
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	b := New(
		repo,
		WithBatchSize(42),
		WithFlushChanBufferSize(7),
		WithEventsChanBufferSize(11),
		WithWorkerCount(3),
		WithSendTickerDuration(250*time.Millisecond),
		WithWorkerTimeout(5*time.Second),
		WithLogger(logger),
	)

	assert.Equal(t, 42, b.batchSize)
	assert.Equal(t, 7, b.flushChSize)
	assert.Equal(t, 7, cap(b.flushCh))
	assert.Equal(t, 11, b.eventsChSize)
	assert.Equal(t, 11, cap(b.eventsCh))
	assert.Equal(t, 3, b.workerCount)
	assert.Equal(t, 250*time.Millisecond, b.batchSendTickerDuration)
	assert.Equal(t, 5*time.Second, b.workerTimeout)
	assert.Same(t, logger, b.log)

	bZero := New(
		repo,
		WithFlushChanBufferSize(0),
		WithEventsChanBufferSize(0),
		WithLogger(nil),
	)
	assert.Equal(t, 0, cap(bZero.flushCh))
	assert.Equal(t, 0, cap(bZero.eventsCh))
	assert.NotNil(t, bZero.log)

	invalidOpts := []struct {
		name string
		fn   func()
	}{
		{name: "zero batch size", fn: func() { WithBatchSize(0) }},
		{name: "negative batch size", fn: func() { WithBatchSize(-1) }},
		{name: "negative flush buffer size", fn: func() { WithFlushChanBufferSize(-1) }},
		{name: "negative events buffer size", fn: func() { WithEventsChanBufferSize(-1) }},
		{name: "zero worker count", fn: func() { WithWorkerCount(0) }},
		{name: "negative worker count", fn: func() { WithWorkerCount(-1) }},
		{name: "zero ticker duration", fn: func() { WithSendTickerDuration(0) }},
		{name: "negative ticker duration", fn: func() { WithSendTickerDuration(-time.Millisecond) }},
		{name: "zero worker timeout", fn: func() { WithWorkerTimeout(0) }},
		{name: "negative worker timeout", fn: func() { WithWorkerTimeout(-time.Millisecond) }},
	}

	for _, tc := range invalidOpts {
		t.Run(tc.name, func(t *testing.T) {
			require.Panics(t, tc.fn)
		})
	}
}

func TestBatcher_FlushOnBatchSize(t *testing.T) {
	repo := newMockRepo()
	b := New(
		repo,
		WithBatchSize(3),
		WithSendTickerDuration(time.Minute),
	)

	go b.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	})

	ev1 := newTestEvent("ev1")
	ev2 := newTestEvent("ev2")
	ev3 := newTestEvent("ev3")

	ctx := context.Background()
	require.NoError(t, b.Add(ctx, ev1))
	require.NoError(t, b.Add(ctx, ev2))
	require.NoError(t, b.Add(ctx, ev3))

	select {
	case <-repo.called:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for batch flush")
	}

	batches := repo.getBatches()
	require.Len(t, batches, 1)
	assert.Equal(t, []event.Event{ev1, ev2, ev3}, batches[0])
}

func TestBatcher_FlushOnTicker(t *testing.T) {
	repo := newMockRepo()
	b := New(
		repo,
		WithBatchSize(10),
		WithSendTickerDuration(30*time.Millisecond),
	)

	ev1 := newTestEvent("ev1")
	ev2 := newTestEvent("ev2")

	// Buffer both events before starting so the initial ticker period cannot split them.
	ctx := context.Background()
	require.NoError(t, b.Add(ctx, ev1))
	require.NoError(t, b.Add(ctx, ev2))

	go b.Start()
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = b.Shutdown(shutdownCtx)
	})

	select {
	case <-repo.called:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ticker flush")
	}

	batches := repo.getBatches()
	require.Len(t, batches, 1)
	assert.Equal(t, []event.Event{ev1, ev2}, batches[0])
}

func TestBatcher_EmptyTickerDoesNotFlush(t *testing.T) {
	repo := newMockRepo()
	b := New(
		repo,
		WithBatchSize(10),
		WithSendTickerDuration(15*time.Millisecond),
	)

	go b.Start()
	time.Sleep(50 * time.Millisecond)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, b.Shutdown(shutdownCtx))

	assert.Empty(t, repo.getBatches())
}

func TestBatcher_ShutdownDrainsRemainingEvents(t *testing.T) {
	t.Run("partial batch", func(t *testing.T) {
		repo := newMockRepo()
		b := New(
			repo,
			WithBatchSize(10),
			WithSendTickerDuration(time.Minute),
		)

		go b.Start()

		ev1 := newTestEvent("ev1")
		ev2 := newTestEvent("ev2")

		ctx := context.Background()
		require.NoError(t, b.Add(ctx, ev1))
		require.NoError(t, b.Add(ctx, ev2))

		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, b.Shutdown(shutdownCtx))

		batches := repo.getBatches()
		require.Len(t, batches, 1)
		assert.Equal(t, []event.Event{ev1, ev2}, batches[0])
	})

	t.Run("multiple batches buffered before shutdown", func(t *testing.T) {
		repo := newMockRepo()
		b := New(
			repo,
			WithBatchSize(2),
			WithEventsChanBufferSize(10),
			WithSendTickerDuration(time.Minute),
		)

		evs := []event.Event{
			newTestEvent("ev1"),
			newTestEvent("ev2"),
			newTestEvent("ev3"),
			newTestEvent("ev4"),
			newTestEvent("ev5"),
		}
		for _, ev := range evs {
			require.NoError(t, b.Add(context.Background(), ev))
		}

		go b.Start()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, b.Shutdown(shutdownCtx))

		batches := repo.getBatches()
		require.Len(t, batches, 3)
		assert.Equal(t, evs[0:2], batches[0])
		assert.Equal(t, evs[2:4], batches[1])
		assert.Equal(t, evs[4:5], batches[2])
	})
}

func TestBatcher_AddAfterShutdown(t *testing.T) {
	repo := newMockRepo()
	b := New(repo, WithSendTickerDuration(time.Minute))

	go b.Start()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, b.Shutdown(shutdownCtx))

	err := b.Add(context.Background(), newTestEvent("after-shutdown"))
	require.ErrorIs(t, err, ErrShuttingDown)
}

func TestBatcher_AddContextCanceled(t *testing.T) {
	repo := newMockRepo()
	b := New(repo, WithEventsChanBufferSize(1))

	require.NoError(t, b.Add(context.Background(), newTestEvent("fill-buffer")))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := b.Add(ctx, newTestEvent("canceled"))
	require.ErrorIs(t, err, context.Canceled)
}

func TestBatcher_AddUnblocksOnShutdown(t *testing.T) {
	repo := newMockRepo()
	workerStarted := make(chan struct{})
	releaseWorker := make(chan struct{})
	var callNum atomic.Int32
	repo.fn = func(ctx context.Context, events []event.Event) error {
		if callNum.Add(1) == 1 {
			close(workerStarted)
			<-releaseWorker
		}
		return nil
	}

	b := New(
		repo,
		WithBatchSize(1),
		WithEventsChanBufferSize(0),
		WithFlushChanBufferSize(0),
		WithWorkerCount(1),
		WithSendTickerDuration(time.Minute),
	)

	go b.Start()

	ev1 := newTestEvent("ev1")
	ev2 := newTestEvent("ev2")
	ev3 := newTestEvent("blocked")

	require.NoError(t, b.Add(context.Background(), ev1))
	select {
	case <-workerStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for worker to block")
	}

	// ev2 is handed off to startLoop, which then blocks trying to send to unbuffered flushCh.
	require.NoError(t, b.Add(context.Background(), ev2))

	addErrCh := make(chan error, 1)
	go func() {
		addErrCh <- b.Add(context.Background(), ev3)
	}()

	time.Sleep(15 * time.Millisecond)

	shutdownErrCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		shutdownErrCh <- b.Shutdown(ctx)
	}()

	select {
	case err := <-addErrCh:
		require.ErrorIs(t, err, ErrShuttingDown)
	case <-time.After(time.Second):
		t.Fatal("blocked Add did not unblock on Shutdown")
	}

	close(releaseWorker)

	select {
	case err := <-shutdownErrCh:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Shutdown")
	}

	batches := repo.getBatches()
	require.Len(t, batches, 2)
	assert.Equal(t, []event.Event{ev1}, batches[0])
	assert.Equal(t, []event.Event{ev2}, batches[1])
}

func TestBatcher_DoubleShutdown(t *testing.T) {
	repo := newMockRepo()
	b := New(repo, WithSendTickerDuration(time.Minute))

	go b.Start()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	require.NoError(t, b.Shutdown(ctx))
	require.ErrorIs(t, b.Shutdown(ctx), ErrAlreadyStoppped)
}

func TestBatcher_ShutdownTimeout(t *testing.T) {
	repo := newMockRepo()
	started := make(chan struct{})
	ctxErrCh := make(chan error, 1)
	repo.fn = func(ctx context.Context, events []event.Event) error {
		close(started)
		<-ctx.Done()
		ctxErrCh <- ctx.Err()
		return ctx.Err()
	}

	b := New(
		repo,
		WithBatchSize(1),
		WithSendTickerDuration(time.Minute),
		WithWorkerTimeout(time.Minute),
	)

	go b.Start()

	require.NoError(t, b.Add(context.Background(), newTestEvent("blocking")))

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for worker to start")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := b.Shutdown(shutdownCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	select {
	case <-b.done:
	case <-time.After(time.Second):
		t.Fatal("batcher did not stop after shutdown timeout")
	}

	select {
	case workerErr := <-ctxErrCh:
		require.ErrorIs(t, workerErr, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("worker context was not canceled")
	}
}

func TestBatcher_ShutdownTimeoutDropsPendingFlush(t *testing.T) {
	repo := newMockRepo()
	started := make(chan struct{})
	repo.fn = func(ctx context.Context, events []event.Event) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}

	b := New(
		repo,
		WithBatchSize(1),
		WithEventsChanBufferSize(0),
		WithFlushChanBufferSize(0),
		WithWorkerCount(1),
		WithSendTickerDuration(time.Minute),
		WithWorkerTimeout(time.Minute),
	)

	go b.Start()

	ev1 := newTestEvent("blocking-worker")
	ev2 := newTestEvent("blocked-in-flush")

	require.NoError(t, b.Add(context.Background(), ev1))
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for worker to start")
	}

	require.NoError(t, b.Add(context.Background(), ev2))

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := b.Shutdown(shutdownCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	select {
	case <-b.done:
	case <-time.After(time.Second):
		t.Fatal("batcher did not stop after shutdown timeout")
	}

	batches := repo.getBatches()
	require.Len(t, batches, 1)
	assert.Equal(t, []event.Event{ev1}, batches[0])
}

func TestBatcher_WorkerTimeout(t *testing.T) {
	repo := newMockRepo()
	var attempts atomic.Int32
	firstErrCh := make(chan error, 1)
	repo.fn = func(ctx context.Context, events []event.Event) error {
		if attempts.Add(1) == 1 {
			<-ctx.Done()
			firstErrCh <- ctx.Err()
			return ctx.Err()
		}
		return nil
	}

	b := New(
		repo,
		WithBatchSize(1),
		WithSendTickerDuration(time.Minute),
		WithWorkerTimeout(25*time.Millisecond),
	)

	go b.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = b.Shutdown(ctx)
	})

	ev1 := newTestEvent("slow")
	ev2 := newTestEvent("fast")

	require.NoError(t, b.Add(context.Background(), ev1))
	select {
	case err := <-firstErrCh:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for worker timeout")
	}

	require.NoError(t, b.Add(context.Background(), ev2))
	for range 2 {
		select {
		case <-repo.called:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for batches")
		}
	}

	batches := repo.getBatches()
	require.Len(t, batches, 2)
	assert.Equal(t, []event.Event{ev1}, batches[0])
	assert.Equal(t, []event.Event{ev2}, batches[1])
}

func TestBatcher_MultipleWorkers(t *testing.T) {
	repo := newMockRepo()
	const workers = 3
	started := make(chan struct{}, workers)
	release := make(chan struct{})
	repo.fn = func(ctx context.Context, events []event.Event) error {
		started <- struct{}{}
		<-release
		return nil
	}

	b := New(
		repo,
		WithBatchSize(2),
		WithWorkerCount(workers),
		WithSendTickerDuration(time.Minute),
	)

	go b.Start()

	evs := make([]event.Event, 0, workers*2)
	for range workers * 2 {
		ev := newTestEvent("ev")
		evs = append(evs, ev)
		require.NoError(t, b.Add(context.Background(), ev))
	}

	for range workers {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("expected all workers to process batches concurrently")
		}
	}
	close(release)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, b.Shutdown(shutdownCtx))

	batches := repo.getBatches()
	require.Len(t, batches, workers)

	var got []event.Event
	for _, batch := range batches {
		require.Len(t, batch, 2)
		got = append(got, batch...)
	}
	assert.ElementsMatch(t, evs, got)
}

func TestBatcher_RepoErrorDoesNotStopBatcher(t *testing.T) {
	repo := newMockRepo()
	var attempts atomic.Int32
	repo.fn = func(ctx context.Context, events []event.Event) error {
		if attempts.Add(1) == 1 {
			return errors.New("temporary db error")
		}
		return nil
	}

	b := New(
		repo,
		WithBatchSize(1),
		WithSendTickerDuration(time.Minute),
	)

	go b.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, b.Shutdown(ctx))
	})

	ev1 := newTestEvent("ev1")
	ev2 := newTestEvent("ev2")

	require.NoError(t, b.Add(context.Background(), ev1))
	require.NoError(t, b.Add(context.Background(), ev2))

	for range 2 {
		select {
		case <-repo.called:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for batches")
		}
	}

	batches := repo.getBatches()
	require.Len(t, batches, 2)
	assert.Equal(t, []event.Event{ev1}, batches[0])
	assert.Equal(t, []event.Event{ev2}, batches[1])
}

func TestBatcher_ConcurrentAddAndShutdown(t *testing.T) {
	repo := newMockRepo()
	b := New(
		repo,
		WithBatchSize(5),
		WithEventsChanBufferSize(64),
		WithFlushChanBufferSize(16),
		WithWorkerCount(3),
		WithSendTickerDuration(5*time.Millisecond),
	)

	go b.Start()

	var accepted atomic.Int64
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for {
				err := b.Add(context.Background(), newTestEvent("concurrent"))
				if err != nil {
					assert.ErrorIs(t, err, ErrShuttingDown)
					return
				}
				accepted.Add(1)
			}
		})
	}

	time.Sleep(15 * time.Millisecond)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, b.Shutdown(shutdownCtx))

	wg.Wait()
	assert.Equal(t, int(accepted.Load()), repo.totalEvents())
}
