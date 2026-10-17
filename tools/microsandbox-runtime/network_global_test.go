package microsandboxruntime

import (
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	msb "github.com/superradcompany/microsandbox/sdk/go"
)

func globalBoundaryConfig(destination, protocol, ports string) string {
	return "network:\n  isolation: {enabled: true}\n  allowed-egress:\n    - destination: " + destination +
		"\n      protocol: " + protocol + "\n      ports: '" + ports + "'\n" +
		"  native:\n    dns-nameservers: ['127.0.0.1:5300']\n    allowed-egress:\n" +
		"      - {destination: host, protocol: tcp, ports: '4566'}\n"
}

func TestGlobalBoundaryValidatesSharedCIDRsPortsAndFields(t *testing.T) {
	file := filepath.Join(t.TempDir(), "network.yaml")
	read := func(data string) (*nativeNetworkBoundary, error) {
		t.Helper()
		if err := os.WriteFile(file, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		return readNetworkBoundary(file)
	}
	for _, destination := range []string{"203.0.113.0/24", "8.0.0.0/6", "172.24.0.0/16", "100.64.0.0/10"} {
		boundary, err := read(globalBoundaryConfig(destination, "tcp", "443-443"))
		if err != nil || len(boundary.Allowed) != 2 || len(boundary.global) != 1 || boundary.global[0].Ports != "443" {
			t.Fatalf("valid destination %s: %#v %v", destination, boundary, err)
		}
	}
	for _, destination := range []string{
		"host", "public", "example.test", "8.8.8.8", "0.0.0.0/0", "203.0.113.1/24", "203.00.113.0/24", "::/0",
		"0.0.0.0/8", "126.0.0.0/7", "169.254.0.0/16", "169.0.0.0/8", "224.0.0.0/4", "240.0.0.0/4",
	} {
		if _, err := read(globalBoundaryConfig(destination, "tcp", "443")); err == nil {
			t.Fatalf("unsafe global destination accepted: %s", destination)
		}
	}
	for _, ports := range []string{"", "0", "65536", "0443", "443-0444", "443-1", "+443", "443 ", "443,444"} {
		if _, err := read(globalBoundaryConfig("203.0.113.0/24", "udp", ports)); err == nil {
			t.Fatalf("unsafe global ports accepted: %q", ports)
		}
	}
	for _, ports := range []string{"53", "853", "1-54", "800-900"} {
		if _, err := read(globalBoundaryConfig("203.0.113.0/24", "udp", ports)); err != nil {
			t.Fatalf("explicit DNS allowance rejected: %s: %v", ports, err)
		}
	}
	for _, config := range []string{
		globalBoundaryConfig("203.0.113.0/24", "TCP", "443"),
		globalBoundaryConfig("203.0.113.0/24", "icmp", "443"),
		strings.Replace(globalBoundaryConfig("203.0.113.0/24", "tcp", "443"), "      protocol:", "      protcol:", 1),
		strings.Replace(globalBoundaryConfig("203.0.113.0/24", "tcp", "443"), "    dns-nameservers: ['127.0.0.1:5300']\n", "", 1),
	} {
		if _, err := read(config); err == nil {
			t.Fatal("invalid global configuration accepted")
		}
	}
}

func TestGlobalBoundaryIntersectsPublicPrivateAndExplicitWorkloadRules(t *testing.T) {
	boundary := boundaryFixture()
	boundary.Allowed = append(boundary.Allowed, networkEndpoint{Destination: "8.0.0.0/6", Protocol: "tcp", Ports: "440-450"})
	requested, err := parseNetwork(nil, []string{"deny:egress@8.8.8.8/32:tcp:443", "allow:egress@public:tcp:443-460", "allow:egress@private:tcp:440-443"}, "deny")
	if err != nil {
		t.Fatal(err)
	}
	policy, err := boundary.restrict(requested)
	if err != nil {
		t.Fatal(err)
	}
	if policy.DefaultEgress != msb.PolicyActionDeny || policy.Rules[2].Action != msb.PolicyActionDeny {
		t.Fatal("default deny or explicit deny precedence changed")
	}
	for _, expected := range []msb.PolicyRule{
		{Action: msb.PolicyActionAllow, Direction: msb.PolicyDirectionEgress, Destination: "8.0.0.0/7", Protocol: msb.PolicyProtocolTCP, Port: "443-450"},
		{Action: msb.PolicyActionAllow, Direction: msb.PolicyDirectionEgress, Destination: "11.0.0.0/8", Protocol: msb.PolicyProtocolTCP, Port: "443-450"},
		{Action: msb.PolicyActionAllow, Direction: msb.PolicyDirectionEgress, Destination: "10.0.0.0/8", Protocol: msb.PolicyProtocolTCP, Port: "440-443"},
	} {
		if !slices.ContainsFunc(policy.Rules, func(rule msb.PolicyRule) bool {
			return rule.Action == expected.Action && rule.Direction == expected.Direction && rule.Destination == expected.Destination && rule.Protocol == expected.Protocol && rule.Port == expected.Port
		}) {
			t.Fatalf("missing exact intersection %#v in %#v", expected, policy.Rules)
		}
	}
	for _, rule := range policy.Rules {
		if rule.Destination == "8.0.0.0/6" || rule.Destination == "public" || rule.Destination == "*" {
			t.Fatal("a named group widened to the mixed configured CIDR")
		}
	}
	for _, destination := range []string{"host", "198.18.0.0/15", "100.64.0.0/10", "127.0.0.0/8"} {
		if result := profileEndpoints("public", networkEndpoint{Destination: destination}); len(result) != 0 {
			t.Fatalf("public profile contains host or local range: %#v", result)
		}
	}
	if candidates := profileEndpoints("private", networkEndpoint{Destination: "100.0.0.0/8"}); len(candidates) != 1 || candidates[0].Destination != "100.64.0.0/10" {
		t.Fatalf("SDK private shared-address intersection: %#v", candidates)
	}
}

func TestGlobalBoundaryDefaultAllowAndReadOnlyExport(t *testing.T) {
	boundary := boundaryFixture()
	boundary.global = []networkEndpoint{{Destination: "203.0.113.0/24", Protocol: "tcp", Ports: "443"}}
	boundary.Allowed = append(boundary.Allowed, boundary.global...)
	client := &Client{boundary: boundary}
	info := client.NetworkBoundary()
	if info.Fingerprint != boundary.fingerprint || len(info.AllowedEgress) != 3 || len(info.GlobalEgress) != 1 || len(info.DNSNameservers) != 1 {
		t.Fatalf("export %#v", info)
	}
	info.AllowedEgress[0].Destination = "0.0.0.0/0"
	info.GlobalEgress[0].Ports = "1-65535"
	info.DNSNameservers[0] = "8.8.8.8:53"
	policy, err := boundary.restrict(msb.NetworkPolicy.AllowAll())
	if err != nil || policy.DefaultEgress != msb.PolicyActionDeny {
		t.Fatal(err)
	}
	if client.NetworkBoundary().GlobalEgress[0].Ports != "443" || boundary.Nameservers[0] != "127.0.0.1:5300" {
		t.Fatal("consumer changed validated boundary")
	}
	for _, rule := range policy.Rules {
		if rule.Destination == "0.0.0.0/0" {
			t.Fatal("mutating exported data widened enforcement")
		}
	}
	if new(Client).NetworkBoundary().Fingerprint != "" {
		t.Fatal("disabled boundary advertised")
	}
}

func TestPublicProfileSplittingNeverIncludesExcludedAddress(t *testing.T) {
	endpoints := profileEndpoints("public", networkEndpoint{Destination: "0.0.0.0/0"})
	if len(endpoints) == 0 || len(endpoints) > 200 {
		t.Fatalf("unbounded or empty split: %d", len(endpoints))
	}
	for _, endpoint := range endpoints {
		prefix := netip.MustParsePrefix(endpoint.Destination)
		if !publicPrefix(prefix) {
			t.Fatalf("public split contains excluded range: %s", prefix)
		}
	}
}
