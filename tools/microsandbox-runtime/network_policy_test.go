package microsandboxruntime

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	msb "github.com/superradcompany/microsandbox/sdk/go"
)

func boundaryFixture() *nativeNetworkBoundary {
	return &nativeNetworkBoundary{fingerprint: "fixture", Nameservers: []string{"127.0.0.1:5300"}, Allowed: []networkEndpoint{
		{Destination: "172.24.0.0/16", Protocol: "tcp", Ports: "80-443"},
		{Destination: "host", Protocol: "udp", Ports: "31000"},
	}}
}

func TestNetworkBoundaryIntersectsCIDRsProtocolsAndPorts(t *testing.T) {
	boundary := boundaryFixture()
	requested, err := parseNetwork(nil, []string{"allow:ingress@0.0.0.0/0:tcp:8080", "deny:egress@172.24.1.0/24:tcp:443", "allow:egress@172.24.0.0/20:tcp:400-500", "allow:egress@host:udp:30000-32000", "allow:egress@public"}, "deny")
	if err != nil {
		t.Fatal(err)
	}
	policy, err := boundary.restrict(requested)
	if err != nil {
		t.Fatal(err)
	}
	if policy.DefaultEgress != msb.PolicyActionDeny || policy.DefaultIngress != msb.PolicyActionDeny {
		t.Fatal("defaults widened")
	}
	if len(policy.Rules) != 5 {
		t.Fatalf("rules %#v", policy.Rules)
	}
	if !reflect.DeepEqual(policy.Rules[1], requested.Rules[0]) {
		t.Fatal("ingress changed")
	}
	if policy.Rules[2].Action != msb.PolicyActionDeny {
		t.Fatal("deny precedence lost")
	}
	if rule := policy.Rules[3]; rule.Destination != "172.24.0.0/20" || rule.Port != "400-443" || rule.Protocol != msb.PolicyProtocolTCP {
		t.Fatalf("intersection %#v", rule)
	}
	if rule := policy.Rules[4]; rule.Destination != "host" || rule.Port != "31000" || rule.Protocol != msb.PolicyProtocolUDP {
		t.Fatalf("host exception %#v", rule)
	}
	if !slices.Equal(policy.DNS.Nameservers, boundary.Nameservers) || policy.DNS.RebindProtection == nil || *policy.DNS.RebindProtection {
		t.Fatal("controlled DNS missing")
	}
}

func TestNetworkBoundaryDefaultAllowStillHasOnlyExplicitLocalEgress(t *testing.T) {
	policy, err := boundaryFixture().restrict(msb.NetworkPolicy.AllowAll())
	if err != nil {
		t.Fatal(err)
	}
	if policy.DefaultEgress != msb.PolicyActionDeny || len(policy.Rules) != 3 {
		t.Fatalf("unsafe default policy %#v", policy)
	}
	for _, rule := range policy.Rules {
		if rule.Destination == "*" || rule.Destination == "public" {
			t.Fatal("public egress allowed")
		}
	}
	if _, ok, err := intersectEndpoint(msb.PolicyRule{Destination: "private"}, networkEndpoint{Destination: "127.0.0.1/32", Protocol: "tcp", Ports: "443"}); err != nil || ok {
		t.Fatal("private selector widened to loopback")
	}
}

func TestBoundaryLaunchRewritesOnlyRuntimeOptions(t *testing.T) {
	boundary := boundaryFixture()
	client := &Client{boundary: boundary, core: fakeBackend(func(context.Context, command, io.Reader, io.Writer, io.Writer) (int, error) {
		t.Fatal("launch is CLI fallback")
		return 0, nil
	})}
	var got []string
	client.cli = func(_ context.Context, args []string, _, _ io.Writer) (int, error) { got = args; return 0, nil }
	_, err := client.Execute(t.Context(), []string{"run", "-d", "--name", "owned", "--net", "host,private,public", "alpine", "--", "sh", "-c", "literal --net public"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(got, " ")
	if strings.Contains(joined, "--net host,private,public") || !strings.Contains(joined, "--net-default-egress deny") || !strings.Contains(joined, "--dns-nameserver 127.0.0.1:5300") || !strings.Contains(joined, networkPolicyLabel+"=fixture") {
		t.Fatalf("unsafe launch %v", got)
	}
	if !slices.Equal(got[len(got)-4:], []string{"--", "sh", "-c", "literal --net public"}) {
		t.Fatal("guest command rewritten")
	}
}

func TestBoundaryIdentityRejectsOlderGuestsAndCheckpoints(t *testing.T) {
	boundary := boundaryFixture()
	for _, labels := range []map[string]string{nil, {networkPolicyLabel: "different"}} {
		if boundary.requireIdentity(labels) == nil {
			t.Fatal("old boundary adopted")
		}
	}
	if err := boundary.requireIdentity(map[string]string{networkPolicyLabel: "fixture"}); err != nil {
		t.Fatal(err)
	}
}

func TestBoundaryConfigurationRequiresExplicitLocalDestinationsAndDNS(t *testing.T) {
	good := "network:\n  isolation:\n    enabled: true\n  native:\n    dns-nameservers: ['127.0.0.1:5300']\n    allowed-egress:\n      - destination: 172.24.0.0/16\n        protocol: tcp\n        ports: '443'\n"
	file := filepath.Join(t.TempDir(), "network.yaml")
	if err := os.WriteFile(file, []byte(good), 0600); err != nil {
		t.Fatal(err)
	}
	boundary, err := readNetworkBoundary(file)
	if err != nil || len(boundary.fingerprint) != 64 {
		t.Fatal(err)
	}
	for _, bad := range []string{
		strings.Replace(good, "172.24.0.0/16", "0.0.0.0/0", 1),
		strings.Replace(good, "172.24.0.0/16", "8.8.8.8/32", 1),
		strings.Replace(good, "127.0.0.1:5300", "8.8.8.8:53", 1),
		strings.Replace(good, "ports: '443'", "ports: '0'", 1),
		strings.Replace(good, "allowed-egress:", "allow-egress:", 1),
		"network: {isolation: {enabled: true}}",
		good + "---\nnetwork: {}\n",
	} {
		if err := os.WriteFile(file, []byte(bad), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := readNetworkBoundary(file); err == nil {
			t.Fatalf("unsafe config accepted: %s", bad)
		}
	}
	if _, err := readNetworkBoundary(file + "-missing"); err == nil {
		t.Fatal("missing policy silently disabled")
	}
}

func TestBoundaryRejectsReservedLabelBeforeLaunch(t *testing.T) {
	c := &Client{boundary: boundaryFixture()}
	_, err := c.Execute(t.Context(), []string{"run", "-d", "--name", "owned", "--label", networkPolicyLabel + "=spoof", "alpine"}, nil)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
}

func TestRestoredBoundaryRequiresImmutableGuestAndCheckpointIdentity(t *testing.T) {
	boundary := boundaryFixture()
	record := restoredBoundary{ID: "vm-id", Name: "owned", Parent: "checkpoint-id", Policy: "fixture"}
	if err := boundary.verifyRestoredIdentity(record, "vm-id", "owned", "checkpoint-id"); err != nil {
		t.Fatal(err)
	}
	for _, identity := range [][3]string{{"different-id", "owned", "checkpoint-id"}, {"vm-id", "different-name", "checkpoint-id"}, {"vm-id", "owned", "different-parent"}, {"vm-id", "owned", ""}} {
		if boundary.verifyRestoredIdentity(record, identity[0], identity[1], identity[2]) == nil {
			t.Fatalf("accepted mismatched identity %v", identity)
		}
	}
	record.Policy = "old-policy"
	if boundary.verifyRestoredIdentity(record, "vm-id", "owned", "checkpoint-id") == nil {
		t.Fatal("accepted checkpoint under old policy")
	}
	path := boundaryRecordPath(t.TempDir(), "../../escaping-vm-id")
	if strings.Contains(filepath.Base(path), "escaping") {
		t.Fatal("guest identifier used as host path")
	}
	parent, err := snapshotParent(`{"snapshot_parent":"checkpoint-id","other":true}`)
	if err != nil || parent != "checkpoint-id" {
		t.Fatalf("parent %q: %v", parent, err)
	}
}

func TestBoundaryAcceptsOnlyParsedPublicCertificates(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1)}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificate := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if !publicCertificates(certificate) || !publicCertificates(append(slices.Clone(certificate), certificate...)) {
		t.Fatal("valid public certificates rejected")
	}
	for _, bad := range [][]byte{nil, []byte("garbage"), []byte("-----BEGIN CERTIFICATE-----\ninvalid\n-----END CERTIFICATE-----"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte("not x509")}), append(slices.Clone(certificate), []byte("PRIVATE KEY")...)} {
		if publicCertificates(bad) {
			t.Fatal("invalid certificate or private content accepted")
		}
	}
}
