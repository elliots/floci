package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestPeerVXLANReusedGuestIPKeepsDistinctConntrackTuples(t *testing.T) {
	n := &network{peerIP: "192.0.2.2", hostIP: "192.0.2.1"}
	first := &podEntry{link: "pvfirst", slot: 0, port: 40001, guestIP: "172.16.0.6"}
	second := &podEntry{link: "pvsecond", slot: 1, port: 40002, guestIP: first.guestIP}
	for _, entry := range []*podEntry{first, second} {
		link, rule := n.peerVXLANLink(entry), n.peerVXLANRule(entry)
		if !strings.Contains(link, fmt.Sprintf("srcport %d %d dstport 8472", peerVXLANSourcePort(entry), peerVXLANSourcePort(entry)+1)) ||
			!strings.Contains(rule, fmt.Sprintf("--sport %d --dport 8472", peerVXLANSourcePort(entry))) ||
			!strings.Contains(rule, fmt.Sprintf("--to-destination %s:%d", n.hostIP, entry.port)) {
			t.Fatal("Pod transport tuple is not unique", link, rule)
		}
		if !strings.Contains(n.clearPeerVXLANConntrack(entry), fmt.Sprintf("--sport %d --dport 8472", peerVXLANSourcePort(entry))) {
			t.Fatal("conntrack cleanup omitted owned source port")
		}
	}
	if n.peerVXLANRule(first) == n.peerVXLANRule(second) {
		t.Fatal("reused guest IP aliases different Pod routes")
	}
	if strings.Contains(n.peerVXLANRules(second), n.legacyPeerVXLANRule(first)) {
		t.Fatal("migration attempts to delete another Pod's legacy rule")
	}
}

func TestPeerVXLANMigrationRequiresExactJournalOwnership(t *testing.T) {
	n := &network{peerIP: "192.0.2.2"}
	e := &podEntry{link: "pvowned", slot: 9, port: 40009, guestIP: "172.16.0.6"}
	base := `[{"ifname":"pvowned","master":"pvkb0","mtu":1450,"linkinfo":{"info_kind":"vxlan","info_data":{"id":10009,"remote":"172.16.0.6","local":"192.0.2.2","link":"eth0","port_range":{"low":0,"high":0},"port":8472,"learning":false}}}]`
	for _, test := range []struct {
		name, data     string
		migrate, fails bool
	}{
		{"legacy", base, true, false},
		{"current", strings.Replace(base, `"low":0,"high":0`, `"low":20009,"high":20010`, 1), false, false},
		{"interrupted replacement", `[]`, true, false},
		{"other name", strings.Replace(base, "pvowned", "pvother", 1), false, true},
		{"other bridge", strings.Replace(base, "pvkb0", "other", 1), false, true},
		{"other VNI", strings.Replace(base, "10009", "10010", 1), false, true},
		{"other remote", strings.Replace(base, "172.16.0.6", "172.16.0.7", 1), false, true},
		{"other local", strings.Replace(base, "192.0.2.2", "192.0.2.3", 1), false, true},
		{"other receive port", strings.Replace(base, "8472", "8473", 1), false, true},
		{"other source port", strings.Replace(base, `"low":0,"high":0`, `"low":40008,"high":40008`, 1), false, true},
		{"other kind", strings.Replace(base, "vxlan", "bridge", 1), false, true},
		{"malformed", `{`, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			migrate, err := n.peerVXLANNeedsMigration([]byte(test.data), e)
			if migrate != test.migrate || (err != nil) != test.fails {
				t.Fatal("unexpected ownership result", migrate, err)
			}
		})
	}
}

func TestDockerPeerVXLANReusedGuestAddressAndMigration(t *testing.T) {
	if os.Getenv("FLOCI_TEST_DOCKER_VXLAN") != "1" {
		t.Skip("requires an explicitly enabled disposable privileged Docker network fixture")
	}
	name := fmt.Sprintf("floci-vxlan-reuse-%d", time.Now().UnixNano())
	runner := commandRunner{cfg: config{peer: name, peerOwner: name}}
	if _, err := runner.run(t.Context(), "docker", "run", "-d", "--name", name, "--label", ownerLabel+"="+name, "--privileged", "--entrypoint", "sh", "rancher/k3s:v1.35.0-k3s1", "-ec", "sleep 300"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := runner.run(ctx, "docker", "rm", "-f", name); err != nil {
			t.Error("remove owned network fixture", err)
		}
	})
	peerIP, err := runner.run(t.Context(), "docker", "inspect", "--format", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", name)
	if err != nil {
		t.Fatal(err)
	}
	hostIP, err := runner.run(t.Context(), "docker", "inspect", "--format", "{{range .NetworkSettings.Networks}}{{.Gateway}}{{end}}", name)
	if err != nil {
		t.Fatal(err)
	}
	n := &network{runner: runner, peerIP: peerIP, hostIP: hostIP}
	if _, err := runner.peer(t.Context(), "ip link add pvkb0 type bridge; ip addr add 10.42.200.1/22 dev pvkb0; ip link set pvkb0 up"); err != nil {
		t.Fatal(err)
	}
	entries := []*podEntry{{link: "pvfirst", slot: 0, port: 40001, guestIP: "172.16.0.6"}, {link: "pvsecond", slot: 1, port: 40002, guestIP: "172.16.0.6"}}
	for _, e := range entries {
		legacyLink := strings.Replace(n.peerVXLANLink(e), fmt.Sprintf("srcport %d %d ", peerVXLANSourcePort(e), peerVXLANSourcePort(e)+1), "", 1)
		if _, err := runner.peer(t.Context(), legacyLink+"; iptables -t nat -A "+n.legacyPeerVXLANRule(e)); err != nil {
			t.Fatal(err)
		}
	}
	for _, e := range entries {
		if err := n.reconcilePeerVXLAN(t.Context(), e); err != nil {
			diagnostics, _ := runner.command(t.Context(), "docker", "exec", name, "sh", "-exc", n.peerVXLANRules(e)).CombinedOutput()
			t.Log(string(diagnostics))
			t.Fatal(err)
		}
		inspection, err := runner.peer(t.Context(), "ip -d -j link show dev "+e.link)
		if err != nil {
			t.Fatal(err)
		}
		if migrate, err := n.peerVXLANNeedsMigration([]byte(inspection), e); err != nil || migrate {
			t.Fatal("migration not applied", err, inspection)
		}
	}
	if _, err := runner.peer(t.Context(), "ping -c 1 -W 1 10.42.200.2 >/dev/null 2>&1 || true"); err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		result, err := runner.peer(t.Context(), fmt.Sprintf("conntrack -L -p udp -s %s -d %s --sport %d --dport 8472 2>/dev/null", n.peerIP, e.guestIP, peerVXLANSourcePort(e)))
		if err != nil || !strings.Contains(result, fmt.Sprintf("sport=%d", e.port)) || !strings.Contains(result, fmt.Sprintf("dport=%d", peerVXLANSourcePort(e))) {
			diagnostics, _ := runner.peer(t.Context(), "ip -s link show pvkb0; ip -s link show pvfirst; ip route; iptables -t nat -nvL OUTPUT; conntrack -L -p udp 2>/dev/null")
			t.Log(diagnostics)
			t.Fatal("distinct peer VXLAN tuple not observed", e.port, err, result)
		}
	}
	if err := n.detach(t.Context(), entries[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.peer(t.Context(), "iptables -t nat -C "+n.peerVXLANRule(entries[1])); err != nil {
		t.Fatal("cleanup removed the surviving Pod rule", err)
	}
	if err := n.reconcilePeerVXLAN(t.Context(), entries[0]); err != nil {
		t.Fatal("interrupted replacement could not rebuild owned link", err)
	}
	data, err := runner.peer(t.Context(), "ip -d -j link show dev "+entries[0].link)
	var links []json.RawMessage
	if err != nil || json.Unmarshal([]byte(data), &links) != nil || len(links) != 1 {
		t.Fatal("replacement missing", err, data)
	}
}
