package server

import (
	"log/slog"
	"net/http"

	"github.com/neelalala/hopper/internal/event"
)

type EventIngester interface {
	Ingest(event.Event) error
}

type Server struct {
	httpServer *http.Server

	ingester EventIngester

	log *slog.Logger
}

func New(addr string, ingester EventIngester, log *slog.Logger) *Server {
	mux := http.NewServeMux()

	httpServer := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	server := &Server{
		httpServer: httpServer,
		ingester:   ingester,
		log:        log,
	}

	mux.HandleFunc("POST /events", server.HandleEvent)

	return server
}
