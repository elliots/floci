package microsandboxruntime

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"

	msb "github.com/superradcompany/microsandbox/sdk/go"
)

func TestRestoreRequiresCapturedPolicyAndDeniesDefaultSDKDNS(t *testing.T) {
	boundary := boundaryFixture()
	if _, err := boundary.restorePolicy(nil); err == nil {
		t.Fatal("missing captured rules accepted")
	}
	requested, err := parseNetwork(nil, []string{"allow:egress@172.24.0.0/20:tcp:443"}, "deny")
	if err != nil {
		t.Fatal(err)
	}
	boot, err := boundary.restrict(requested)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := boundary.restorePolicy(boot)
	if err != nil {
		t.Fatal(err)
	}
	if restored.DNS != nil || restored.DefaultEgress != msb.PolicyActionDeny || restored.DefaultIngress != msb.PolicyActionDeny {
		t.Fatal("restore widened defaults or passed unsupported DNS configuration")
	}
	first := restored.Rules[0]
	if first.Action != msb.PolicyActionDeny || first.Destination != "host" || first.Port != "53" || !slices.Contains(first.Protocols, msb.PolicyProtocolUDP) || !slices.Contains(first.Protocols, msb.PolicyProtocolTCP) {
		t.Fatal("SDK default DNS not blocked before other rules")
	}
	for _, rule := range restored.Rules[1:] {
		if rule.Action == msb.PolicyActionAllow && rule.Port != "5300" && (rule.Destination != "172.24.0.0/20" || rule.Port != "443") {
			t.Fatalf("captured workload restriction widened: %#v", rule)
		}
	}
}

func TestSnapshotNetworkRecordRejectsMissingIdentityLegacyDNSAndChangedCA(t *testing.T) {
	boundary := boundaryFixture()
	home := t.TempDir()
	policy, err := boundary.restrict(msb.NetworkPolicy.AllowAll())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := boundary.snapshotPolicy(home, "snapshot-id"); err == nil {
		t.Fatal("unrecorded snapshot accepted")
	}
	guest := restoredBoundary{DNSVersion: 1, Network: policy}
	if err := boundary.recordSnapshot(home, "snapshot-id", guest); err != nil {
		t.Fatal(err)
	}
	loaded, err := boundary.snapshotPolicy(home, "snapshot-id")
	if err != nil || loaded.DefaultEgress != msb.PolicyActionDeny {
		t.Fatalf("valid captured policy lost: %v", err)
	}
	boundary.caBytes = []byte("new public certificate bytes")
	if _, err := boundary.snapshotPolicy(home, "snapshot-id"); err == nil {
		t.Fatal("changed trust anchor accepted")
	}
	boundary.caBytes = nil
	path := snapshotBoundaryPath(home, "snapshot-id")
	var record snapshotBoundary
	if err := readNetworkRecord(path, &record); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*snapshotBoundary){
		func(r *snapshotBoundary) { r.ID = "different" },
		func(r *snapshotBoundary) { r.Policy = "different" },
		func(r *snapshotBoundary) { r.DNSVersion = 0 },
		func(r *snapshotBoundary) { r.Network = nil },
	} {
		changed := record
		mutate(&changed)
		if err := writeNetworkRecord(path, changed); err != nil {
			t.Fatal(err)
		}
		if _, err := boundary.snapshotPolicy(home, "snapshot-id"); err == nil {
			t.Fatal("invalid snapshot record accepted")
		}
	}
}

func TestNetworkRecordRejectsSymlinkAndPublicPermissions(t *testing.T) {
	home := t.TempDir()
	path := boundaryRecordPath(home, "id")
	if err := writeNetworkRecord(path, map[string]string{"id": "id"}); err != nil {
		t.Fatal(err)
	}
	var record map[string]string
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if readNetworkRecord(path, &record) == nil {
		t.Fatal("public record accepted")
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(home, "link.json")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if readNetworkRecord(link, &record) == nil {
		t.Fatal("symlink accepted")
	}
}

func TestGuestDNSUsesExplicitHostMappingAndNoImplicitResolver(t *testing.T) {
	boundary := boundaryFixture()
	boundary.Nameservers = append(boundary.Nameservers, "172.24.0.10:5301")
	data, err := boundary.guestDNSConfig()
	if err != nil {
		t.Fatal(err)
	}
	var configuration struct {
		DNS struct {
			Servers []string `json:"nameservers"`
		} `json:"dnsRouting"`
	}
	if err := json.Unmarshal(data, &configuration); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(configuration.DNS.Servers, []string{"host.microsandbox.internal:5300", "172.24.0.10:5301"}) {
		t.Fatalf("unexpected guest DNS config: %s", data)
	}
}
