package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"
)

func (e *environmentBoundary) chain() string {
	hash := sha256.Sum256([]byte(e.runner.cfg.peerOwner))
	return fmt.Sprintf("FEG%x", hash[:8])
}

func (e *environmentBoundary) peerRules(ipv6 bool) string {
	chain := e.chain()
	var rules strings.Builder
	fmt.Fprintf(&rules, "*mangle\n:%s - [0:0]\n-F %s\n", chain, chain)
	fmt.Fprintf(&rules, "-A %s -o lo -j RETURN\n", chain)
	fmt.Fprintf(&rules, "-A %s -m conntrack --ctdir REPLY --ctstate ESTABLISHED,RELATED -j RETURN\n", chain)
	if !ipv6 {
		// Transport grants apply only to sockets created by the trusted peer.
		// Forwarded application packets keep their Pod source until POSTROUTING's
		// later source-NAT hook and cannot use these grants to reach arbitrary host ports.
		for _, port := range e.portList() {
			fmt.Fprintf(&rules, "-A %s -s %s/32 -d %s/32 -p %s -m %s --dport %d -j RETURN\n", chain, e.identity.PeerIP, e.identity.HostIP, port.Protocol, port.Protocol, port.Port)
		}
		for _, endpoint := range e.policy.GlobalEgress {
			fmt.Fprintf(&rules, "-A %s -d %s -p %s -m %s --dport %s -j RETURN\n", chain, endpoint.Destination, endpoint.Protocol, endpoint.Protocol, strings.ReplaceAll(endpoint.Ports, "-", ":"))
		}
		// Bridge gateways are Docker-host addresses, not simulator endpoints.
		// Only explicit permissions above may admit them; their protected subnet
		// alone must not authorize every host listener.
		for _, gateway := range e.identity.Gateways {
			fmt.Fprintf(&rules, "-A %s -d %s/32 -j DROP\n", chain, gateway)
		}
		for _, prefix := range append([]string{e.runner.cfg.clusterCIDR, e.runner.cfg.serviceCIDR}, e.identity.Prefixes...) {
			fmt.Fprintf(&rules, "-A %s -d %s -j RETURN\n", chain, prefix)
		}
	}
	fmt.Fprintf(&rules, "-A %s -j DROP\nCOMMIT\n", chain)
	return rules.String()
}

func (e *environmentBoundary) installPeer(ctx context.Context) error {
	// A separate mangle hook is deliberate: Kubernetes filter-chain ACCEPTs
	// cannot bypass it. POSTROUTING sees the final Service DNAT destination, before
	// masquerading erases the workload source. A batch replaces only our chain.
	for _, ipv6 := range []bool{false, true} {
		binary := "iptables"
		if ipv6 {
			binary = "ip6tables"
		}
		script := binary + "-restore --wait 5 --noflush; if ! " + binary + " -w 5 -t mangle -C POSTROUTING -j " + e.chain() + " 2>/dev/null; then " + binary + " -w 5 -t mangle -I POSTROUTING 1 -j " + e.chain() + "; fi"
		cmd := e.runner.command(ctx, "docker", "exec", "-i", e.identity.PeerID, "sh", "-ec", script)
		cmd.Stdin = strings.NewReader(e.peerRules(ipv6))
		if output, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("install protected peer egress: %w (%s)", err, strings.TrimSpace(string(output)))
		}
	}
	return nil
}

func (e *environmentBoundary) verifyPeer(ctx context.Context) error {
	for _, ipv6 := range []bool{false, true} {
		binary := "iptables"
		if ipv6 {
			binary = "ip6tables"
		}
		output, err := e.runner.peer(ctx, binary+" -w 5 -t mangle -S "+e.chain()+"; "+binary+" -w 5 -t mangle -S POSTROUTING")
		if err != nil {
			return err
		}
		if err := verifyEnvironmentPeerRules(output, e.chain(), e.peerRules(ipv6)); err != nil {
			return err
		}
	}
	return nil
}

func verifyEnvironmentPeerRules(output, chain, expected string) error {
	var actualRules, wantRules []string
	firstHook := ""
	for line := range strings.SplitSeq(output, "\n") {
		if strings.HasPrefix(line, "-A "+chain+" ") {
			actualRules = append(actualRules, normalizeEnvironmentRule(line))
		}
		if firstHook == "" && strings.HasPrefix(line, "-A POSTROUTING ") {
			firstHook = line
		}
	}
	for line := range strings.SplitSeq(expected, "\n") {
		if strings.HasPrefix(line, "-A "+chain+" ") {
			wantRules = append(wantRules, normalizeEnvironmentRule(line))
		}
	}
	if firstHook != "-A POSTROUTING -j "+chain || !slices.Equal(actualRules, wantRules) {
		return errors.New("protected peer egress rules or hook ordering changed")
	}
	return nil
}

func normalizeEnvironmentRule(line string) string {
	// iptables canonicalizes option ordering for the conntrack extension.
	line = strings.ReplaceAll(line, "--ctdir REPLY --ctstate ESTABLISHED,RELATED", "--ctstate RELATED,ESTABLISHED --ctdir REPLY")
	line = strings.ReplaceAll(line, "--ctstate ESTABLISHED,RELATED", "--ctstate RELATED,ESTABLISHED")
	return strings.Join(strings.Fields(line), " ")
}

func (e *environmentBoundary) revoke(ports ...environmentPort) {
	if e == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := e.changePorts(ctx, ports, false); err != nil {
		slog.Error("could not remove protected peer transport allowance", "error", err)
	}
}

func (p *provider) watchEnvironment(ctx context.Context) {
	e := p.runner.environment
	if e == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
		check, cancel := context.WithTimeout(ctx, 10*time.Second)
		before := e.healthy.Load()
		err := e.verify(check)
		cancel()
		if err != nil && before {
			slog.Error("protected native environment unavailable", "error", err)
		}
		if p.policies != nil && before != e.healthy.Load() {
			p.policies.notify()
		}
	}
}

func (e *environmentBoundary) revokeAll() {
	if e == nil {
		return
	}
	e.mu.Lock()
	ports := e.portList()
	e.mu.Unlock()
	e.revoke(ports...)
}
