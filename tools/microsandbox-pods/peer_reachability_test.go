package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
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

func TestOwnedDockerPeerReachesExplicitHostAddress(t *testing.T) {
	peer, owner, host := os.Getenv("FLOCI_NATIVE_RELAY_PEER"), os.Getenv("FLOCI_NATIVE_RELAY_OWNER"), os.Getenv("FLOCI_NATIVE_RELAY_NODE_IP")
	if peer == "" || owner == "" || host == "" {
		t.Skip("requires an explicitly owned disposable Docker peer and reachable host IPv4 address")
	}
	runner := commandRunner{cfg: config{peer: peer, peerOwner: owner}}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	actual, err := runner.run(ctx, "docker", "inspect", "--format", "{{ index .Config.Labels \"io.floci.pod-probe\" }}", peer)
	if err != nil || actual != owner {
		t.Fatalf("Docker peer ownership mismatch: %v", err)
	}
	listener, err := peerListenerAddress(ctx, runner, host)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("owned Linux peer reached host listener %s through %s", listener, host)
}

func TestPeerListenerUsesOnlyVerifiedLoopbackOrOwnedInterface(t *testing.T) {
	for _, scenario := range []struct {
		name                      string
		loopback, local, specific bool
		want                      string
		attempts                  []string
	}{
		{"loopback", true, false, false, "127.0.0.1", []string{"127.0.0.1"}},
		{"specific interface", false, true, true, "192.0.2.10", []string{"127.0.0.1", "192.0.2.10"}},
		{"unassigned address", false, false, true, "", []string{"127.0.0.1"}},
		{"unreachable interface", false, true, false, "", []string{"127.0.0.1", "192.0.2.10"}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			var attempts []string
			actual, err := selectPeerListener("192.0.2.10", func(string) (bool, error) { return scenario.local, nil }, func(host string) error {
				attempts = append(attempts, host)
				if host == "127.0.0.1" && scenario.loopback || host == "192.0.2.10" && scenario.specific {
					return nil
				}
				return errors.New("unreachable")
			})
			if actual != scenario.want || (err != nil) != (scenario.want == "") || !slices.Equal(attempts, scenario.attempts) {
				t.Fatalf("address=%q error=%v attempts=%v", actual, err, attempts)
			}
		})
	}
}

func TestPeerListenerNeverBindsWildcardAndUsesExplicitNodeIP(t *testing.T) {
	for _, invalid := range []string{"", "0.0.0.0", "::", "::1", "host.docker.internal", "224.0.0.1"} {
		if _, err := peerHostAddress(invalid); err == nil {
			t.Fatalf("invalid node address accepted: %s", invalid)
		}
		if err := verifyBoundReachability(t.Context(), invalid, func(context.Context, int, string) error {
			t.Fatal("invalid bind address reached network probe")
			return nil
		}); err == nil {
			t.Fatalf("invalid bind address accepted: %s", invalid)
		}
	}
	if actual, err := peerHostAddress("192.0.2.10"); err != nil || actual != "192.0.2.10" {
		t.Fatal("explicit node address was not retained", actual, err)
	}
	if local, err := localHostAddress("127.0.0.1"); err != nil || !local {
		t.Fatal("local loopback interface was not found", local, err)
	}
}

func TestLifecycleProxyURLUsesSelectedFrontendOnly(t *testing.T) {
	e := &podEntry{pod: validPod(), proxyHost: "192.0.2.10", httpPort: 18080, proxyPorts: []proxyPortBinding{{ProxyPort: 18081}}}
	actual := view(e)
	if actual.ProxyURL != "http://192.0.2.10:18081" || actual.BackendURL != "http://127.0.0.1:18080" {
		t.Fatal("frontend selection changed backend exposure or reported the wrong URL", actual)
	}
}
