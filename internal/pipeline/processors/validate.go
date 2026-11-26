package processors

import (
	"context"
	"uuid"

	"github.com/neelalala/hopper/internal/event"
)

type Validate struct{}

func (v *Validate) Process(_ context.Context, ev *event.Event) (drop bool, err error) {
	if ev.ID == uuid.Nil() || ev.Name == "" || ev.UserID == "" || !ev.Properties.IsValid(nil) {
		drop = true
	}

	return
}
