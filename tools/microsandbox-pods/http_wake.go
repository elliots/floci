package main

import (
	"context"
	"errors"
)

type httpWakeKey struct{}
type acceptedHTTPWakeKey struct{}

var errColdWakeNotDispatched = errors.New("HTTP wake cancelled before native dispatch")

func isHTTPWake(ctx context.Context) bool {
	marked, _ := ctx.Value(httpWakeKey{}).(bool)
	return marked
}

// Capacity and preflight checks keep the request context. Once admission has
// succeeded, finish the whole transition even if that client disconnects.
func acceptHTTPWake(ctx context.Context, e *podEntry) (context.Context, context.CancelFunc, error) {
	if !isHTTPWake(ctx) {
		return ctx, func() {}, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if e.lifetime == nil {
		return nil, nil, errors.New("HTTP wake requires a Pod lifetime")
	}
	if err := e.lifetime.Err(); err != nil {
		return nil, nil, err
	}
	accepted, cancel := boundedContext(context.WithoutCancel(ctx))
	stop := context.AfterFunc(e.lifetime, cancel)
	accepted = context.WithValue(accepted, acceptedHTTPWakeKey{}, true)
	return accepted, func() { stop(); cancel() }, nil
}
