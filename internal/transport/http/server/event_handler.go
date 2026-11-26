package server

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"
	"uuid"

	"github.com/neelalala/hopper/internal/event"
	"github.com/neelalala/hopper/internal/pipeline"
)

const (
	ResponseErrorInternalError  = "INTERNAL_ERROR"
	ResponseErrorBadRequestBody = "BAD_REQUEST_BODY"
	ResponseErrorBadEventID     = "BAD_EVENT_ID"
	ResponseErrorChanIsFull     = "CHAN_IS_FULL"
	ResponseErrorShuttingDown   = "SHUTTING_DOWN"
)

type eventJSON struct {
	ID         string          `json:"id"`
	Name       string          `json:"name"`
	UserID     string          `json:"user_id"`
	ClientTime time.Time       `json:"client_time"`
	Properties json.RawMessage `json:"properties"`
}

type errorResponseJSON struct {
	Error   string
	Message string
}

type Handler struct {
	ingester EventIngester

	log *slog.Logger
}

func NewHandler(ingester EventIngester, log *slog.Logger) *Handler {
	return &Handler{
		ingester: ingester,
		log:      log,
	}
}

func (h *Handler) HandleEvent(w http.ResponseWriter, req *http.Request) {
	var reqEvent eventJSON
	if err := json.NewDecoder(req.Body).Decode(&reqEvent); err != nil {
		h.writeError(w, http.StatusBadRequest, ResponseErrorBadRequestBody, "unable to parse request body: event json")
		return
	}
	evID, err := uuid.Parse(reqEvent.ID)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, ResponseErrorBadEventID, "unable to parse request event id as uuid")
		return
	}

	ev := event.Event{
		ID:         evID,
		Name:       reqEvent.Name,
		UserID:     reqEvent.UserID,
		ClientTime: reqEvent.ClientTime,
		ServerTime: time.Now(),
		IP:         getIP(req),
		UserAgent:  req.Header.Get("User-Agent"),
		Properties: reqEvent.Properties,
	}

	if err := h.ingester.Ingest(ev); err != nil {
		code, error, message := mapError(err)
		if code == http.StatusInternalServerError {
			h.log.Error("ingest event", slog.String("error", err.Error()))
		}
		h.writeError(w, code, error, message)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (h *Handler) writeError(w http.ResponseWriter, code int, error, message string) {
	w.WriteHeader(code)
	resp := errorResponseJSON{
		Error:   error,
		Message: message,
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		h.log.Error(
			"marshalling error response",
			slog.String("error", err.Error()),
			slog.Group(
				"response",
				slog.String("error", error),
				slog.String("message", message),
			),
		)
	}
}

func getIP(req *http.Request) string {
	if header := req.Header.Get("X-Forwarded-For"); header != "" {
		ips := strings.Split(header, ", ")
		return ips[0]
	}
	return req.RemoteAddr
}

func mapError(err error) (code int, error string, message string) {
	switch {
	case errors.Is(err, pipeline.ErrChanIsFull):
		return http.StatusTooManyRequests, ResponseErrorChanIsFull, "events queue is full, try again later"
	case errors.Is(err, pipeline.ErrShuttingDown):
		return http.StatusServiceUnavailable, ResponseErrorShuttingDown, "service is shutting down"
	default:
		return http.StatusInternalServerError, ResponseErrorInternalError, "internal server error"
	}
}
