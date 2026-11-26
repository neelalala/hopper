package clickhouse

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/neelalala/hopper/internal/event"
)

const PingTimeout = 10 * time.Second

type EventRepository struct {
	conn clickhouse.Conn

	log *slog.Logger
}

func NewEventRepository(addr, database, username, password string, log *slog.Logger) (*EventRepository, error) {
	ctx, cancel := context.WithTimeout(context.Background(), PingTimeout)
	defer cancel()

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("connect to clickhouse: %w", err)
	}

	err = conn.Ping(ctx)
	if err != nil {
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}

	return &EventRepository{
		conn: conn,
		log:  log,
	}, err
}

func (r *EventRepository) Close() error {
	return r.conn.Close()
}

func (r *EventRepository) CreateEvents(ctx context.Context, evs []event.Event) error {
	r.log.Debug("create events", slog.Int("count", len(evs)))

	batch, err := r.conn.PrepareBatch(ctx, "INSERT INTO events")
	if err != nil {
		return fmt.Errorf("create batch: %w", err)
	}
	defer func() {
		if err := batch.Close(); err != nil {
			r.log.Warn("close batch", slog.String("error", err.Error()))
		}
	}()
	for _, ev := range evs {
		err := batch.Append(
			ev.ID,
			ev.Name,
			ev.UserID,
			ev.ClientTime,
			ev.ServerTime,
			ev.IP,
			ev.UserAgent,
			ev.UserOS,
			ev.UserDevice,
			ev.Properties,
		)
		if err != nil {
			return fmt.Errorf("append event %s to batch: %w", ev.ID, err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("send batch: %w", err)
	}
	return nil
}
