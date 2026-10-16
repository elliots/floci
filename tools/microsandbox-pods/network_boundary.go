package main

import "context"

const reservedCAPath = "/etc/floci/certs/ca.crt"

type nativeBoundary interface {
	VerifyNetworkBoundary(context.Context) error
	NetworkPolicyFingerprint() string
	GuestCAPath() string
}

func (r commandRunner) verifyNetworkBoundary(ctx context.Context) error {
	if runtime, ok := r.runtime.(nativeBoundary); ok {
		return runtime.VerifyNetworkBoundary(ctx)
	}
	return nil
}

func (r commandRunner) networkFingerprint() string {
	if runtime, ok := r.runtime.(nativeBoundary); ok {
		return runtime.NetworkPolicyFingerprint()
	}
	return ""
}

func (r commandRunner) guestCAPath() string {
	if runtime, ok := r.runtime.(nativeBoundary); ok && runtime.GuestCAPath() == reservedCAPath {
		return reservedCAPath
	}
	return ""
}
