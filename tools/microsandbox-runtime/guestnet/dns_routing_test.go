package guestnet

import (
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestDNSRoutingResolvesHostAgainOnReconfiguration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hosts")
	config := DNSRouting{Nameservers: []string{"host.microsandbox.internal:5300", "10.1.2.3:5301", "172.16.2.3:5302"}}
	for _, address := range []string{"192.168.127.1", "192.168.126.1"} {
		if err := os.WriteFile(path, []byte("127.0.0.1 localhost\n"+address+" host.microsandbox.internal # gateway\n"), 0600); err != nil {
			t.Fatal(err)
		}
		routes, err := planDNSRouting(config, path)
		if err != nil {
			t.Fatal(err)
		}
		if len(routes) != 3 || routes[0].Target.String() != address+":5300" || routes[2].Virtual.String() != "198.18.0.55" {
			t.Fatalf("unexpected routes: %v", routes)
		}
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := planDNSRouting(config, path); err == nil {
		t.Fatal("missing guest mapping must not fall back to host DNS")
	}
}

func TestDNSRoutingRejectsImplicitAndPublicResolvers(t *testing.T) {
	for _, nameservers := range [][]string{
		nil, {"10.0.0.1:53", "10.0.0.2:53", "10.0.0.3:53", "10.0.0.4:53"},
		{"8.8.8.8:53"}, {"127.0.0.1:53"}, {"169.254.1.2:53"}, {"0.0.0.0:53"},
		{"10.0.0.1"}, {"10.0.0.1:0"}, {"10.0.0.1:65536"}, {"10.0.0.1:dns"},
		{"localhost:53"}, {"resolver.example:53"}, {"[fd00::1]:53"},
		{"198.18.0.53:53"}, {"host.microsandbox.internal:53"},
		{"10.0.0.1:53"}, {"10.0.0.1:853"}, {"10.0.0.1:5353"}, {"10.0.0.1:5355"}, {"10.0.0.1:137"},
	} {
		t.Run(strings.Join(nameservers, ","), func(t *testing.T) {
			if _, err := planDNSRouting(DNSRouting{Nameservers: nameservers}, filepath.Join(t.TempDir(), "missing")); err == nil {
				t.Fatalf("accepted %v", nameservers)
			}
		})
	}
}

func TestDNSRoutingRejectsAmbiguousOrPublicHostMapping(t *testing.T) {
	for _, content := range []string{
		"8.8.8.8 host.microsandbox.internal\n",
		"192.168.1.1 host.microsandbox.internal\n192.168.2.1 host.microsandbox.internal\n",
		"::1 host.microsandbox.internal\n",
		"192.168.1.1 other-host\n",
		"192.168.1.1 host.microsandbox.internal\n" + strings.Repeat("# padding\n", 10000),
	} {
		path := filepath.Join(t.TempDir(), "hosts")
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := dnsHostAddress(path); err == nil {
			t.Fatal("accepted invalid guest mapping")
		}
	}
}

func TestDNSRoutingSelectsIPv4FromDualStackGuestHosts(t *testing.T) {
	ipv4 := "172.16.0.5 host.microsandbox.internal\n"
	ipv6 := "fd42:6d73:62:1::1 host.microsandbox.internal\n"
	for _, content := range []string{ipv4 + ipv6, ipv6 + ipv4} {
		path := filepath.Join(t.TempDir(), "hosts")
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		address, err := dnsHostAddress(path)
		if err != nil || address.String() != "172.16.0.5" {
			t.Fatalf("dual-stack hosts mapping: %v, %v", address, err)
		}
		if err := os.WriteFile(path, []byte(content+"invalid host.microsandbox.internal\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := dnsHostAddress(path); err == nil {
			t.Fatal("invalid address alongside valid mapping must be rejected")
		}
	}
}

func TestDNSRoutingPreservesSearchAndOptionsWithoutPublicFallback(t *testing.T) {
	routes, err := planDNSRouting(DNSRouting{Nameservers: []string{"10.1.2.3:5300", "10.1.2.4:5301"}}, "unused")
	if err != nil {
		t.Fatal(err)
	}
	existing := []byte("# guest configuration\nnameserver 8.8.8.8\n  nameserver 1.1.1.1 # old\nsearch svc.cluster.local cluster.local\noptions ndots:5 timeout:1\n")
	content, err := dnsResolverConfiguration(existing, routes)
	if err != nil {
		t.Fatal(err)
	}
	want := "# Configured by Floci guest DNS routing.\nnameserver 198.18.0.53\nnameserver 198.18.0.54\n# guest configuration\nsearch svc.cluster.local cluster.local\noptions ndots:5 timeout:1\n"
	if string(content) != want {
		t.Fatalf("resolver configuration = %q", content)
	}
	again, err := dnsResolverConfiguration(content, routes)
	if err != nil || !reflect.DeepEqual(content, again) {
		t.Fatalf("reconfiguration is not idempotent: %q, %v", again, err)
	}
	if _, err := dnsResolverConfiguration(make([]byte, 65537), routes); err == nil {
		t.Fatal("accepted oversized configuration")
	}
	if routes[0].Virtual.IsLoopback() || routes[0].Target != netip.MustParseAddrPort("10.1.2.3:5300") {
		t.Fatal("unexpected route addressing")
	}
}
