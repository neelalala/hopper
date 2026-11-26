package batcher

import (
	"context"
	"errors"
	"sync"
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

	select {
	case m.called <- struct{}{}:
	default:
	}

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

func TestBatcher_FlushOnBatchSize(t *testing.T) {
	repo := newMockRepo()
	b := New(
		repo,
		WithBatchSize(3),
		WithSendTickerDuration(time.Minute),
	)

	go b.Start()
	t.Cleanup(func() {
		_ = b.Shutdown(context.Background())
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
		WithSendTickerDuration(20*time.Millisecond),
	)

	go b.Start()
	t.Cleanup(func() {
		_ = b.Shutdown(context.Background())
	})

	ev1 := newTestEvent("ev1")
	ev2 := newTestEvent("ev2")

	ctx := context.Background()
	require.NoError(t, b.Add(ctx, ev1))
	require.NoError(t, b.Add(ctx, ev2))

	select {
	case <-repo.called:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ticker flush")
	}

	batches := repo.getBatches()
	require.Len(t, batches, 1)
	assert.Equal(t, []event.Event{ev1, ev2}, batches[0])
}

func TestBatcher_ShutdownDrainsRemainingEvents(t *testing.T) {
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
	repo.fn = func(ctx context.Context, events []event.Event) error {
		close(started)
		<-ctx.Done()
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
}

func TestBatcher_RepoErrorDoesNotStopBatcher(t *testing.T) {
	repo := newMockRepo()
	var attempts int
	repo.fn = func(ctx context.Context, events []event.Event) error {
		attempts++
		if attempts == 1 {
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
		_ = b.Shutdown(context.Background())
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
