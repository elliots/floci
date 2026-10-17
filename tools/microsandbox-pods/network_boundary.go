package main

import (
	"context"
	"errors"
	msbruntime "github.com/floci-io/floci/tools/microsandbox-runtime"
)

const reservedCAPath = "/etc/floci/certs/ca.crt"

type nativeBoundary interface {
	VerifyNetworkBoundary(context.Context) error
	NetworkPolicyFingerprint() string
	GuestCAPath() string
}

func (r *commandRunner) verifyNetworkBoundary(ctx context.Context) error {
	if runtime, ok := r.runtime.(nativeBoundary); ok {
		if err := runtime.VerifyNetworkBoundary(ctx); err != nil {
			return err
		}
		if runtime.NetworkPolicyFingerprint() != "" {
			boundary, ok := r.runtime.(interface {
				NetworkBoundary() msbruntime.NetworkBoundaryInfo
			})
			if !ok || r.cfg.environmentNetwork == "" || r.cfg.isolationHelper == "" || r.cfg.environmentDNS == "" {
				return errors.New("native Pod environment isolation requires a protected Kubernetes network peer and its Floci helper")
			}
			if err := r.cfg.validateEnvironment(); err != nil {
				return err
			}
			if r.environment == nil {
				environment, err := newEnvironmentBoundary(ctx, *r, boundary.NetworkBoundary())
				if err != nil {
					return err
				}
				r.environment = environment
			}
			return r.environment.verify(ctx)
		}
	}
	if r.cfg.environmentNetwork != "" || r.cfg.isolationHelper != "" || r.cfg.environmentDNS != "" {
		return errors.New("protected peer configuration requires enabled native environment isolation")
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
