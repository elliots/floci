package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type networkRelay struct {
	runner    commandRunner
	directory string
}

type relayListenerView struct {
	Ready         bool   `json:"ready"`
	ID            string `json:"id"`
	ListenAddress string `json:"listenAddress"`
	TargetAddress string `json:"targetAddress"`
}

func (r *networkRelay) inventory(ctx context.Context) (map[string]relayListenerView, error) {
	data, err := r.request(ctx, "GET", "/listeners", nil)
	if err != nil {
		return nil, err
	}
	var response struct {
		Owner     string
		Listeners []relayListenerView
	}
	if err := json.Unmarshal(data, &response); err != nil {
		return nil, err
	}
	if response.Owner != r.runner.cfg.peerOwner {
		return nil, errors.New("relay inventory ownership changed")
	}
	result := map[string]relayListenerView{}
	for _, listener := range response.Listeners {
		result[listener.ID] = listener
	}
	return result, nil
}

func matchingRelayBinding(current relayListenerView, e *podEntry, b proxyPortBinding, host string) bool {
	return current.ID == relayBindingID(e, b.ContainerPort) && current.ListenAddress == fmt.Sprintf("%s:%d", e.ip, b.ContainerPort) && current.TargetAddress == fmt.Sprintf("%s:%d", host, b.ProxyPort)
}

func (p *provider) ensureRelay(ctx context.Context) (*networkRelay, error) {
	p.relayMu.Lock()
	defer p.relayMu.Unlock()
	if p.relay != nil {
		return p.relay, nil
	}
	if p.runner.cfg.relayBinary == "" {
		return nil, errors.New("proxy availability requires --network-relay-binary")
	}
	if err := verifyPeerReachability(ctx, p.runner, p.network.hostIP); err != nil {
		return nil, err
	}
	r := &networkRelay{runner: p.runner, directory: "/tmp/fvp-relay-" + p.runner.cfg.peerOwner}
	if _, err := p.runner.peer(ctx, "umask 077; mkdir -p "+quote(r.directory)+"; chmod 700 "+quote(r.directory)); err != nil {
		return nil, err
	}
	tokenPath := filepath.Join(p.runner.cfg.state, "relay-token")
	token, err := os.ReadFile(tokenPath)
	if errors.Is(err, os.ErrNotExist) {
		bytes := make([]byte, 32)
		if _, err := rand.Read(bytes); err != nil {
			return nil, err
		}
		token = []byte(hex.EncodeToString(bytes))
		if err := os.WriteFile(tokenPath, token, 0600); err != nil {
			return nil, err
		}
		if err := syncDirectory(p.runner.cfg.state); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	if len(token) < 32 {
		return nil, errors.New("invalid stored relay token")
	}
	if _, err := p.runner.peer(ctx, "test -S "+quote(r.directory+"/control.sock")); err == nil {
		if err := r.health(ctx, p.runner.cfg.peerOwner); err != nil {
			return nil, err
		}
		p.relay = r
		return r, nil
	}
	if _, err := p.runner.run(ctx, "docker", "cp", p.runner.cfg.relayBinary, p.runner.cfg.peer+":"+r.directory+"/relay"); err != nil {
		return nil, err
	}
	cmd := p.runner.command(ctx, "docker", "exec", "-i", p.runner.cfg.peer, "sh", "-ec", "umask 077; cat >"+quote(r.directory+"/token")+"; chmod 700 "+quote(r.directory+"/relay"))
	cmd.Stdin = strings.NewReader(string(token))
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("install relay token: %w", err)
	}
	if _, err := p.runner.run(ctx, "docker", "exec", "-d", p.runner.cfg.peer, r.directory+"/relay", "serve",
		"--socket", r.directory+"/control.sock", "--token-file", r.directory+"/token", "--owner", p.runner.cfg.peerOwner,
		"--listen-cidr", p.network.prefix.String(), "--target-host", p.network.hostIP, "--max-connections", "1024"); err != nil {
		return nil, err
	}
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for {
		if err := r.health(ctx, p.runner.cfg.peerOwner); err == nil {
			p.relay = r
			return r, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline.C:
			return nil, errors.New("Linux network relay did not become healthy")
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (r *networkRelay) request(ctx context.Context, method, path string, value any) ([]byte, error) {
	data := ""
	if value != nil {
		bytes, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		data = string(bytes)
	}
	cmd := r.runner.command(ctx, "docker", "exec", "-i", r.runner.cfg.peer, r.directory+"/relay", "client", "--socket", r.directory+"/control.sock",
		"--token-file", r.directory+"/token", "--method", method, "--path", path)
	cmd.Stdin = strings.NewReader(data)
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("network relay %s %s: %w", method, path, err)
	}
	return output, nil
}

func (r *networkRelay) health(ctx context.Context, owner string) error {
	data, err := r.request(ctx, "GET", "/health", nil)
	if err != nil {
		return err
	}
	var health struct {
		Owner, Status string
		Contract      int
	}
	if err := json.Unmarshal(data, &health); err != nil {
		return err
	}
	if health.Owner != owner || health.Status != "ok" || health.Contract != 1 {
		return errors.New("network relay ownership or contract mismatch")
	}
	return nil
}

func (r *networkRelay) bind(ctx context.Context, e *podEntry, b proxyPortBinding, host string) error {
	data, err := r.request(ctx, "PUT", "/listeners/"+relayBindingID(e, b.ContainerPort), map[string]string{
		"listenAddress": fmt.Sprintf("%s:%d", e.ip, b.ContainerPort), "targetAddress": fmt.Sprintf("%s:%d", host, b.ProxyPort)})
	if err != nil {
		return err
	}
	var listener relayListenerView
	if err := json.Unmarshal(data, &listener); err != nil {
		return err
	}
	if !listener.Ready || !matchingRelayBinding(listener, e, b, host) {
		return errors.New("relay binding did not become available")
	}
	return nil
}

func relayBindingID(e *podEntry, port int) string { return fmt.Sprintf("%s-%d", e.pod.UID, port) }

func readyRelayBinding(current relayListenerView, e *podEntry, b proxyPortBinding, host string) bool {
	return current.Ready && matchingRelayBinding(current, e, b, host)
}
