package services

import (
	"fmt"
	"log/slog"

	"github.com/neelalala/hopper/internal/core/domain"
	"github.com/neelalala/hopper/internal/core/ports"
)

type Pipeline struct {
	processors []ports.Processor

	events    chan domain.Event
	processed chan domain.Event

	log *slog.Logger
}

func NewPipeline(processors []ports.Processor, bufferSize int, log *slog.Logger) (*Pipeline, error) {
	if bufferSize <= 0 {
		return nil, fmt.Errorf("buffer size for channels must be positive, got: %d", bufferSize)
	}
	if log == nil {
		return nil, fmt.Errorf("nil logger")
	}
	events := make(chan domain.Event, bufferSize)
	processed := make(chan domain.Event, bufferSize)

	return &Pipeline{
		processors: processors,
		events:     events,
		processed:  processed,
		log:        log,
	}, nil
}

func (p *Pipeline) IngestEvent(e domain.Event) error {
	p.log.Debug(
		"ingesting event",
		eventToSlogAttr(e),
	)

	select {
	case p.events <- e:
		return nil
	default:
		return fmt.Errorf("ingest event: %w", domain.ErrChanIsFull)
	}
}

func eventToSlogAttr(e domain.Event) slog.Attr {
	return slog.Group(
		"event",
		slog.String("id", e.ID.String()),
		slog.String("name", e.Name),
		slog.Group(
			"user",
			slog.String("id", e.UserID),
			slog.String("agent", e.UserAgent),
			slog.String("device", e.UserDevice),
			slog.String("os", e.UserOS),
			slog.String("ip", e.IP),
		),
		slog.Time("client_time", e.ClientTime),
		slog.Time("server_time", e.ServerTime),
	)
}
