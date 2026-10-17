package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	msbruntime "github.com/floci-io/floci/tools/microsandbox-runtime"
)

func TestEnvironmentOptionsRequireCompleteSecurePeer(t *testing.T) {
	valid := config{environmentNetwork: "protected", isolationHelper: "helper", environmentDNS: "172.28.0.2", networkPolicy: true}
	if err := valid.validateEnvironment(); err != nil {
		t.Fatal(err)
	}
	if err := (config{}).validateEnvironment(); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*config){
		"network":     func(c *config) { c.environmentNetwork = "" },
		"helper":      func(c *config) { c.isolationHelper = "" },
		"dns":         func(c *config) { c.environmentDNS = "" },
		"loopback":    func(c *config) { c.environmentDNS = "127.0.0.1" },
		"ipv6":        func(c *config) { c.environmentDNS = "::1" },
		"raw sockets": func(c *config) { c.networkPolicy = false },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := valid
			change(&candidate)
			if candidate.validateEnvironment() == nil {
				t.Fatal("incomplete or unsafe protected peer accepted")
			}
		})
	}
}

func TestEnvironmentTransportRequiresExplicitNativeHostUDP(t *testing.T) {
	for _, entry := range []msbruntime.NetworkEgressEndpoint{
		{Destination: "host", Protocol: "udp", Ports: "45670"},
		{Destination: "host", Protocol: "udp", Ports: "45600-45700"},
	} {
		policy := msbruntime.NetworkBoundaryInfo{AllowedEgress: []msbruntime.NetworkEgressEndpoint{entry}}
		if !environmentTransportAllowed(policy, 45670) {
			t.Fatal("explicit transport refused")
		}
		if environmentTransportAllowed(policy, 45671) && entry.Ports == "45670" {
			t.Fatal("transport range widened")
		}
		policy.AllowedEgress[0].Protocol = "tcp"
		if environmentTransportAllowed(policy, 45670) {
			t.Fatal("TCP permission became UDP transport")
		}
		policy.AllowedEgress[0].Protocol = "udp"
		policy.AllowedEgress[0].Destination = "192.168.0.0/16"
		if environmentTransportAllowed(policy, 45670) {
			t.Fatal("address range became implicit SDK host permission")
		}
	}
}

func TestEnvironmentVerifiesOwnedTupleMembership(t *testing.T) {
	e := &environmentBoundary{identity: environmentIdentity{PeerIP: "172.28.0.3", HostIP: "192.168.0.10"}, ports: map[environmentPort]bool{{Protocol: "tcp", Port: 10250}: true, {Protocol: "udp", Port: 45670}: true}}
	correct := []environmentNFTTransport{
		{Source: e.identity.PeerIP, Destination: e.identity.HostIP, Protocol: "tcp", Port: 10250},
		{Source: e.identity.PeerIP, Destination: e.identity.HostIP, Protocol: "udp", Port: 45670},
		{Source: "172.28.0.4", Destination: "192.168.0.11", Protocol: "tcp", Port: 10250},
	}
	if err := e.verifyTransports(correct); err != nil {
		t.Fatal(err)
	}
	for name, rows := range map[string][]environmentNFTTransport{
		"missing":    correct[1:],
		"other host": append(append([]environmentNFTTransport{}, correct...), environmentNFTTransport{Source: e.identity.PeerIP, Destination: "192.168.0.11", Protocol: "tcp", Port: 10250}),
		"extra port": append(append([]environmentNFTTransport{}, correct...), environmentNFTTransport{Source: e.identity.PeerIP, Destination: e.identity.HostIP, Protocol: "tcp", Port: 22}),
	} {
		t.Run(name, func(t *testing.T) {
			if e.verifyTransports(rows) == nil {
				t.Fatal("changed own transport grants accepted")
			}
		})
	}
}

func TestEnvironmentIntentRetainsPendingRemovalPrivately(t *testing.T) {
	e := &environmentBoundary{runner: commandRunner{cfg: config{state: t.TempDir()}}, identity: environmentIdentity{Version: 1, PeerID: "owned"}, ports: map[environmentPort]bool{{Protocol: "tcp", Port: 10250}: true}, removed: map[environmentPort]bool{{Protocol: "udp", Port: 45670}: true}}
	if err := e.save(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(e.runner.cfg.state, "network-environment")
	data, err := readNetworkIdentityFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var record environmentRecord
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	if len(record.Ports) != 1 || len(record.Removed) != 1 || record.Removed[0].Port != 45670 {
		t.Fatalf("lost interrupted removal intent: %#v", record)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("network identity is not private: %v %v", info, err)
	}
	files, err := filepath.Glob(filepath.Join(e.runner.cfg.state, "*.json"))
	if err != nil || len(files) != 0 {
		t.Fatal("environment intent can be mistaken for a Pod journal")
	}
}

func TestEnvironmentPeerRulesRequireFirstHookAndExactSemantics(t *testing.T) {
	e := &environmentBoundary{runner: commandRunner{cfg: config{peerOwner: "owned", clusterCIDR: "10.42.0.0/16", serviceCIDR: "10.43.0.0/16"}}, identity: environmentIdentity{PeerIP: "172.28.0.3", HostIP: "192.168.0.10", Prefixes: []string{"172.28.0.0/16"}, Gateways: []string{"172.28.0.1"}}, ports: map[environmentPort]bool{{Protocol: "tcp", Port: 10250}: true}}
	for _, ipv6 := range []bool{false, true} {
		rules := e.peerRules(ipv6)
		output := ""
		for line := range strings.SplitSeq(rules, "\n") {
			if strings.HasPrefix(line, "-A ") {
				output += normalizeEnvironmentRule(line) + "\n"
			}
		}
		output += "-A POSTROUTING -j " + e.chain()
		if err := verifyEnvironmentPeerRules(output, e.chain(), rules); err != nil {
			t.Fatal(err)
		}
		if verifyEnvironmentPeerRules("-A POSTROUTING -j ACCEPT\n"+output, e.chain(), rules) == nil {
			t.Fatal("earlier bypass accepted")
		}
		if verifyEnvironmentPeerRules(strings.Replace(output, "-j DROP", "-j RETURN", 1), e.chain(), rules) == nil {
			t.Fatal("changed default denial accepted")
		}
	}
}
