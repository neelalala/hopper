package event

import (
	"encoding/json"
	"time"
	"uuid"
)

type Event struct {
	ID         uuid.UUID
	Name       string
	UserID     string
	ClientTime time.Time
	ServerTime time.Time
	IP         string
	UserAgent  string
	UserOS     string
	UserDevice string
	Properties json.RawMessage
}
