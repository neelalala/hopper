package event

import "log/slog"

func (ev Event) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("id", ev.ID.String()),
		slog.String("name", ev.Name),
		slog.Group(
			"user",
			slog.String("id", ev.UserID),
			slog.String("agent", ev.UserAgent),
			slog.String("device", ev.UserDevice),
			slog.String("os", ev.UserOS),
			slog.String("ip", ev.IP),
		),
		slog.Time("client_time", ev.ClientTime),
		slog.Time("server_time", ev.ServerTime),
	)
}
