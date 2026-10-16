package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

func TestLoopbackReachabilityValidatesThePeerChallenge(t *testing.T) {
	for _, correct := range []bool{true, false} {
		err := verifyLoopbackReachability(t.Context(), func(ctx context.Context, port int, challenge string) error {
			dialer := net.Dialer{}
			connection, err := dialer.DialContext(ctx, "tcp4", fmt.Sprintf("127.0.0.1:%d", port))
			if err != nil {
				return err
			}
			defer connection.Close()
			if !correct {
				challenge = strings.Repeat("x", len(challenge))
			}
			if _, err := fmt.Fprintf(connection, "GET /%s HTTP/1.1\r\nHost: probe\r\nConnection: close\r\n\r\n", challenge); err != nil {
				return err
			}
			_, err = io.Copy(io.Discard, connection)
			return err
		})
		if correct && err != nil || !correct && err == nil {
			t.Fatalf("correct=%t returned %v", correct, err)
		}
	}
}

func TestLoopbackReachabilityCancelsMissingAndSilentPeers(t *testing.T) {
	for _, silentConnection := range []bool{false, true} {
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		started := time.Now()
		err := verifyLoopbackReachability(ctx, func(probe context.Context, port int, challenge string) error {
			if !silentConnection {
				return nil
			}
			dialer := net.Dialer{}
			connection, err := dialer.DialContext(probe, "tcp4", fmt.Sprintf("127.0.0.1:%d", port))
			if err != nil {
				return err
			}
			defer connection.Close()
			_, err = io.Copy(io.Discard, connection)
			return err
		})
		cancel()
		if err == nil || time.Since(started) > time.Second {
			t.Fatalf("unreachable peer must fail promptly: silent=%t elapsed=%v error=%v", silentConnection, time.Since(started), err)
		}
	}
}

func TestLoopbackReachabilityStopsImmediatelyWhenPeerCommandFails(t *testing.T) {
	failure := errors.New("peer connection refused")
	started := time.Now()
	err := verifyLoopbackReachability(t.Context(), func(context.Context, int, string) error { return failure })
	if !errors.Is(err, failure) || time.Since(started) > time.Second {
		t.Fatalf("failed command did not cancel listener: %v after %v", err, time.Since(started))
	}
}

func TestPeerReachabilityRejectsNonliteralHostAddresses(t *testing.T) {
	for _, host := range []string{"host.docker.internal", "127.0.0.1; false", "0.0.0.0", "::1", "224.0.0.1"} {
		if err := verifyPeerReachability(t.Context(), commandRunner{}, host); err == nil {
			t.Errorf("accepted invalid host %q", host)
		}
	}
}

func TestOwnedDockerPeerReachesHostLoopback(t *testing.T) {
	peer, owner := os.Getenv("FLOCI_NATIVE_RELAY_PEER"), os.Getenv("FLOCI_NATIVE_RELAY_OWNER")
	if peer == "" || owner == "" {
		t.Skip("requires an explicitly owned disposable Docker peer")
	}
	runner := commandRunner{cfg: config{peer: peer, peerOwner: owner}}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	actual, err := runner.run(ctx, "docker", "inspect", "--format", "{{ index .Config.Labels \"io.floci.pod-probe\" }}", peer)
	if err != nil || actual != owner {
		t.Fatalf("Docker peer ownership mismatch: %v", err)
	}
	host, err := runner.peer(ctx, "nslookup host.docker.internal | awk '/^Address: / {print $2}' | tail -n1")
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyPeerReachability(ctx, runner, host); err != nil {
		t.Fatal(err)
	}
	t.Logf("owned Linux peer reached host loopback through %s", host)
}
