package ports

import (
	"context"

	"github.com/neelalala/hopper/internal/core/domain"
)

type Processor interface {
	Process(ctx context.Context, event *domain.Event) (drop bool, err error)
}
