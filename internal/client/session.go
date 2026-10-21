// Package client carries connection-local protocol state through command contexts.
package client

import "context"

type contextKey struct{}

// Session is owned by one connection's sequential command loop.
type Session struct {
	ID       uint64
	Protocol int
}

func WithSession(ctx context.Context, session *Session) context.Context {
	return context.WithValue(ctx, contextKey{}, session)
}

func FromContext(ctx context.Context) *Session {
	session, _ := ctx.Value(contextKey{}).(*Session)
	return session
}
