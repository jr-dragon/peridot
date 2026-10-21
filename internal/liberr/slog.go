package liberr

import "log/slog"

func Attr(err error) slog.Attr {
	return slog.Any("error", err)
}
