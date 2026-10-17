package main

import (
	"context"
	"errors"
)

const reservedCAPath = "/etc/floci/certs/ca.crt"

type nativeBoundary interface {
	VerifyNetworkBoundary(context.Context) error
	NetworkPolicyFingerprint() string
	GuestCAPath() string
}

func (r commandRunner) verifyNetworkBoundary(ctx context.Context) error {
	if runtime, ok := r.runtime.(nativeBoundary); ok {
		if err := runtime.VerifyNetworkBoundary(ctx); err != nil {
			return err
		}
		if runtime.NetworkPolicyFingerprint() != "" {
			return errors.New("native Pod environment isolation requires a protected Kubernetes network peer; " +
				"the SDK guest firewall alone does not constrain tunneled traffic or peer DNS forwarding")
		}
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
