package server

import (
	"context"
	"errors"
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
	handler := NewHandler(ingester, log.With(slog.String("handler", "events")))

	mux := http.NewServeMux()

	mux.HandleFunc("POST /events", handler.HandleEvent)

	httpServer := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	server := &Server{
		httpServer: httpServer,
		ingester:   ingester,
		log:        log,
	}

	return server
}

func (s *Server) Start() error {
	defer s.log.Info("server stopped")
	s.log.Info("server satrted", slog.String("address", s.httpServer.Addr))
	if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	defer s.log.Info("server shutted down")
	s.log.Info("shutting down server")
	return s.httpServer.Shutdown(ctx)
}
