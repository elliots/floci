package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/floci-io/floci/tools/microsandbox-pods/policyguard"
)

type policyEndpoint struct {
	uid, address, backend, link string
	plan                        podPolicyPlan
	pending                     bool
}

func policyChain(owner, suffix string) string {
	digest := sha256.Sum256([]byte(owner))
	return fmt.Sprintf("FNP%x%s", digest[:6], suffix)
}

func policyEndpointChain(owner, uid, suffix string) string {
	digest := sha256.Sum256([]byte(uid))
	return policyChain(owner, "") + fmt.Sprintf("%x%s", digest[:4], suffix)
}

func policyPlanHash(plan podPolicyPlan) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%#v", plan))))
}

func policyRuleLine(chain, direction string, rule policyRule) string {
	line := "-A " + chain
	if rule.CIDR != "0.0.0.0/0" {
		line += " -" + direction + " " + rule.CIDR
	}
	if rule.Protocol != "" {
		line += " -p " + rule.Protocol
	}
	if rule.Port != "" {
		line += " --dport " + rule.Port
	}
	return line + " -j RETURN\n"
}

func policyDirection(chain, direction, node string, isolated bool, rules []policyRule) string {
	var result strings.Builder
	result.WriteString("-F " + chain + "\n")
	if !isolated {
		result.WriteString("-A " + chain + " -j RETURN\n")
		return result.String()
	}
	// Replies to admitted connections are implicitly allowed. Original-direction
	// packets are still checked after policy changes, including established TCP.
	result.WriteString("-A " + chain + " -m conntrack --ctdir REPLY --ctstate ESTABLISHED,RELATED -j RETURN\n")
	if node != "" {
		result.WriteString("-A " + chain + " -" + direction + " " + node + "/32 -j RETURN\n")
	}
	for _, rule := range rules {
		result.WriteString(policyRuleLine(chain, direction, rule))
	}
	result.WriteString("-A " + chain + " -j DROP\n")
	return result.String()
}

func peerPolicyRules(owner, node, publicCIDR, backendCIDR string, endpoints []policyEndpoint) string {
	base := policyChain(owner, "P")
	origin := policyChain(owner, "O")
	var declarations, rules strings.Builder
	declarations.WriteString("*filter\n:" + base + " - [0:0]\n:" + origin + " - [0:0]\n")
	rules.WriteString("-F " + base + "\n")
	rules.WriteString("-F " + origin + "\n")
	for _, prefix := range []string{publicCIDR, backendCIDR} {
		// Only the owned bridge can originate a native Pod address. OUTPUT is
		// excluded so the trusted peer can send proxy responses from public IPs.
		rules.WriteString("-A " + origin + " -s " + prefix + " ! -i " + bridgeName + " -j DROP\n")
	}
	slices.SortFunc(endpoints, func(a, b policyEndpoint) int { return strings.Compare(a.uid, b.uid) })
	for _, endpoint := range endpoints {
		ingress, egress := policyEndpointChain(owner, endpoint.uid, "I"), policyEndpointChain(owner, endpoint.uid, "E")
		declarations.WriteString(":" + ingress + " - [0:0]\n:" + egress + " - [0:0]\n")
		if endpoint.pending {
			rules.WriteString("-F " + ingress + "\n-A " + ingress + " -j DROP\n-F " + egress + "\n-A " + egress + " -j DROP\n")
		} else {
			rules.WriteString(policyDirection(ingress, "s", node, endpoint.plan.ingressIsolated, endpoint.plan.ingress))
			rules.WriteString(policyDirection(egress, "d", node, endpoint.plan.egressIsolated, endpoint.plan.egress))
		}
		address := endpoint.address
		if endpoint.backend != "" {
			address = endpoint.backend
		}
		if endpoint.link != "" {
			// Each VXLAN device belongs to one Pod; reject inner-source spoofing.
			rules.WriteString("-A " + base + " -m physdev --physdev-in " + endpoint.link + " ! -s " + address + "/32 -j DROP\n")
		}
		for _, ip := range []string{endpoint.address, endpoint.backend} {
			if ip == "" {
				continue
			}
			rules.WriteString("-A " + base + " -s " + ip + "/32 -j " + egress + "\n")
			rules.WriteString("-A " + base + " -d " + ip + "/32 -j " + ingress + "\n")
		}
	}
	// Return only after both endpoints' policy chains ran. ACCEPT here would
	// bypass the independent environment firewall later in the hook.
	for _, endpoint := range endpoints {
		for _, ip := range []string{endpoint.address, endpoint.backend} {
			if ip != "" {
				rules.WriteString("-A " + base + " -s " + ip + "/32 -j RETURN\n-A " + base + " -d " + ip + "/32 -j RETURN\n")
			}
		}
	}
	for _, prefix := range []string{publicCIDR, backendCIDR} {
		rules.WriteString("-A " + base + " -s " + prefix + " -j DROP\n-A " + base + " -d " + prefix + " -j DROP\n")
	}
	return declarations.String() + rules.String() + "COMMIT\n"
}

func guestPolicyScript(plan podPolicyPlan, clusterCIDR, serviceCIDR string, peerPort int) string {
	const base = "FLOCI-POD-EGRESS"
	var rules strings.Builder
	rules.WriteString("*filter\n:" + base + " - [0:0]\n-F " + base + "\n")
	if plan.egressIsolated {
		// Node sockets are marked before the first image pull. Application
		// cgroups are outside the trusted node cgroup and cannot inherit it.
		rules.WriteString(fmt.Sprintf("-A %s -m mark --mark 0x%x -j RETURN\n", base, policyguard.NodeMark))
		rules.WriteString("-A " + base + " -o lo -j RETURN\n")
		rules.WriteString("-A " + base + " -m conntrack --ctdir REPLY --ctstate ESTABLISHED,RELATED -j RETURN\n")
		// Cluster packets are evaluated on the peer after Service DNAT. The
		// VXLAN transport itself must remain available for that enforcement.
		for _, cidr := range []string{clusterCIDR, serviceCIDR} {
			rules.WriteString("-A " + base + " -o pod0 -d " + cidr + " -j RETURN\n")
		}
		rules.WriteString("-A " + base + " -o pod0 -d 169.254.170.23/32 -j RETURN\n")
		rules.WriteString("-A " + base + " -o eth0 -d GATEWAY/32 -p udp --dport " + strconv.Itoa(peerPort) + " -j RETURN\n")
		for _, rule := range plan.egress {
			rules.WriteString(policyRuleLine(base, "d", rule))
		}
		rules.WriteString("-A " + base + " -j DROP\n")
	} else {
		rules.WriteString("-A " + base + " -j RETURN\n")
	}
	rules.WriteString("COMMIT\n")
	ipv6 := "*filter\n:" + base + " - [0:0]\n-F " + base + "\n"
	if plan.egressIsolated {
		ipv6 += fmt.Sprintf("-A %s -m mark --mark 0x%x -j RETURN\n", base, policyguard.NodeMark) +
			"-A " + base + " -o lo -j RETURN\n" +
			"-A " + base + " -p ipv6-icmp --icmpv6-type 135 -j RETURN\n" +
			"-A " + base + " -p ipv6-icmp --icmpv6-type 136 -j RETURN\n" +
			"-A " + base + " -m conntrack --ctdir REPLY --ctstate ESTABLISHED,RELATED -j RETURN\n" +
			"-A " + base + " -j DROP\n"
	} else {
		ipv6 += "-A " + base + " -j RETURN\n"
	}
	ipv6 += "COMMIT\n"
	return "command -v iptables-restore >/dev/null; command -v ip6tables-restore >/dev/null; gateway=$(ip -4 route show default | awk 'NR == 1 {print $3}'); " +
		"test -n \"$gateway\"; printf %s " + quote(rules.String()) + " | sed \"s/GATEWAY/$gateway/g\" | iptables-restore --wait 5 --noflush; " +
		"if ! iptables -w 5 -C OUTPUT -j " + base + " 2>/dev/null; then iptables -w 5 -I OUTPUT 1 -j " + base + "; fi; " +
		"printf %s " + quote(ipv6) + " | ip6tables-restore --wait 5 --noflush; " +
		"if ! ip6tables -w 5 -C OUTPUT -j " + base + " 2>/dev/null; then ip6tables -w 5 -I OUTPUT 1 -j " + base + "; fi"
}

func (p *provider) installPeerPolicy(ctx context.Context, endpoints []policyEndpoint) error {
	rules := peerPolicyRules(p.runner.cfg.peerOwner, p.network.peerIP, p.network.prefix.String(), p.network.backendPrefix.String(), endpoints)
	base := policyChain(p.runner.cfg.peerOwner, "P")
	origin := policyChain(p.runner.cfg.peerOwner, "O")
	script := "test \"$(cat /proc/sys/net/bridge/bridge-nf-call-iptables)\" = 1; iptables-restore --wait 5 --noflush; "
	if p.runner.cfg.networkPolicy {
		// Reject outer transport packets from ordinary local CNI workloads before
		// Service/host-publication DNAT can make them look like host-origin traffic.
		// Native applications are separately denied these UDP destinations by the
		// cgroup connect/sendmsg hooks, including IPv4-mapped IPv6 sockets.
		transport := policyChain(p.runner.cfg.peerOwner, "T")
		source := policyChain(p.runner.cfg.peerOwner, "S")
		raw := "*raw\n:" + transport + " - [0:0]\n:" + source + " - [0:0]\n-F " + transport + "\n-F " + source + "\n" +
			"-A " + transport + " -j " + source + "\n" +
			"-A " + transport + " -i cni0 -p udp -m multiport --dports 8472," + strconv.Itoa(p.network.peerPort) + " -j DROP\nCOMMIT\n"
		for _, prefix := range []string{p.network.prefix.String(), p.network.backendPrefix.String(), p.network.peerIP + "/32"} {
			raw = strings.TrimSuffix(raw, "COMMIT\n") + "-A " + source + " -i cni0 -s " + prefix + " -j DROP\nCOMMIT\n"
		}
		script += "printf %s " + quote(raw) + " | iptables-restore --wait 5 --noflush; " +
			"if ! iptables -t raw -w 5 -C PREROUTING -j " + transport + " 2>/dev/null; then iptables -t raw -w 5 -I PREROUTING 1 -j " + transport + "; fi; "
	}
	for _, hook := range []string{"INPUT", "FORWARD", "OUTPUT"} {
		script += "if ! iptables -w 5 -C " + hook + " -j " + base + " 2>/dev/null; then iptables -w 5 -I " + hook + " 1 -j " + base + "; fi; "
		if hook != "OUTPUT" {
			script += "if ! iptables -w 5 -C " + hook + " -j " + origin + " 2>/dev/null; then iptables -w 5 -I " + hook + " 1 -j " + origin + "; fi; "
		}
	}
	// Removed Pods must not leave an unbounded collection of kernel chains.
	// Only chains under this controller's owner prefix can be reclaimed.
	keep := " " + base + " " + origin + " "
	for _, endpoint := range endpoints {
		keep += policyEndpointChain(p.runner.cfg.peerOwner, endpoint.uid, "I") + " " + policyEndpointChain(p.runner.cfg.peerOwner, endpoint.uid, "E") + " "
	}
	script += "keep=" + quote(keep) + "; for chain in $(iptables -w 5 -S | awk '$1 == \"-N\" { print $2 }'); do case \"$chain\" in " + policyChain(p.runner.cfg.peerOwner, "") + "*) case \"$keep\" in *\" $chain \"*) ;; *) iptables -w 5 -F \"$chain\"; iptables -w 5 -X \"$chain\" ;; esac ;; esac; done"
	command := p.runner.command(ctx, "docker", "exec", "-i", p.runner.cfg.peer, "sh", "-ec", script)
	command.Stdin = strings.NewReader(rules)
	if err := command.Run(); err != nil {
		return fmt.Errorf("install native peer NetworkPolicy: %w", err)
	}
	return nil
}
