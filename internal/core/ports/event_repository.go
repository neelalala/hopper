package ports

import (
	"context"

	"github.com/neelalala/hopper/internal/core/domain"
)

type EventRepository interface {
	CreateEvents(ctx context.Context, events []domain.Event) error
}
