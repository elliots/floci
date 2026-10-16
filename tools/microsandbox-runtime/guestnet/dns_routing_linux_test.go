package guestnet

import (
	"io"
	"net"
	"net/netip"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/nftables"
)

// This mutates only the disposable test container's own network namespace.
// Never enable it on the host or in a shared workload guest.
func TestDNSRoutingKernel(t *testing.T) {
	if os.Getenv("FLOCI_GUESTNET_KERNEL_TEST") != "1" {
		t.Skip("requires a disposable Linux container with NET_ADMIN")
	}
	existing, err := os.ReadFile("/etc/resolv.conf")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.WriteFile("/etc/resolv.conf", existing, 0644); err != nil {
			t.Error(err)
		}
	})
	connection := &nftables.Conn{}
	unrelated := connection.AddTable(&nftables.Table{Name: "floci_test_keep", Family: nftables.TableFamilyIPv4})
	if err := connection.Flush(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		connection.DelTable(unrelated)
		connection.DelTable(&nftables.Table{Name: dnsTable, Family: nftables.TableFamilyIPv4})
		if err := connection.Flush(); err != nil {
			t.Error(err)
		}
	})
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatal(err)
	}
	var address netip.Addr
	for _, candidate := range addresses {
		prefix, err := netip.ParsePrefix(candidate.String())
		if err == nil && prefix.Addr().Is4() && prefix.Addr().IsPrivate() {
			address = prefix.Addr()
			break
		}
	}
	if !address.IsValid() {
		t.Fatal("test container requires a private IPv4 interface")
	}
	first := dnsEchoEndpoint(t, address, "first")
	second := dnsEchoEndpoint(t, address, "second")
	if err := ConfigureDNS(DNSRouting{Nameservers: []string{first, second}}); err != nil {
		t.Fatal(err)
	}
	udp, err := net.Dial("udp4", "198.18.0.53:53")
	if err != nil {
		t.Fatal(err)
	}
	defer udp.Close()
	checkDNSRoute(t, udp, "first")
	checkDNSRouteDial(t, "tcp4", "198.18.0.53:53", "first")
	checkDNSRouteDial(t, "udp4", "198.18.0.54:53", "second")
	checkDNSRouteDial(t, "tcp4", "198.18.0.54:53", "second")
	// Keep precisely the same UDP five-tuple. Merely replacing NAT rules would
	// leave this socket attached to the previous upstream in conntrack.
	if err := ConfigureDNS(DNSRouting{Nameservers: []string{second}}); err != nil {
		t.Fatal(err)
	}
	checkDNSRoute(t, udp, "second")
	checkDNSRouteDial(t, "tcp4", "198.18.0.53:53", "second")
	if _, err := connection.ListTable("floci_test_keep"); err != nil {
		t.Fatalf("unrelated table was removed: %v", err)
	}
	resolver, err := os.ReadFile("/etc/resolv.conf")
	if err != nil || strings.Count(string(resolver), "nameserver ") != 1 || !strings.Contains(string(resolver), "nameserver 198.18.0.53\n") {
		t.Fatalf("resolver was not replaced: %s, %v", resolver, err)
	}
}

func dnsEchoEndpoint(t *testing.T, address netip.Addr, response string) string {
	t.Helper()
	udp, err := net.ListenUDP("udp4", net.UDPAddrFromAddrPort(netip.AddrPortFrom(address, 0)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { udp.Close() })
	tcp, err := net.Listen("tcp4", udp.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tcp.Close() })
	go func() {
		buffer := make([]byte, 1024)
		for {
			_, remote, err := udp.ReadFrom(buffer)
			if err != nil {
				return
			}
			if _, err := udp.WriteTo([]byte(response), remote); err != nil {
				return
			}
		}
	}()
	go func() {
		for {
			client, err := tcp.Accept()
			if err != nil {
				return
			}
			client.SetDeadline(time.Now().Add(3 * time.Second))
			buffer := make([]byte, 1)
			if _, err := client.Read(buffer); err == nil {
				io.WriteString(client, response)
			}
			client.Close()
		}
	}()
	return udp.LocalAddr().String()
}

func checkDNSRouteDial(t *testing.T, protocol, address, want string) {
	t.Helper()
	connection, err := net.DialTimeout(protocol, address, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	checkDNSRoute(t, connection, want)
}

func checkDNSRoute(t *testing.T, connection net.Conn, want string) {
	t.Helper()
	if err := connection.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := connection.Write([]byte("?")); err != nil {
		t.Fatal(err)
	}
	response := make([]byte, len(want))
	if _, err := io.ReadFull(connection, response); err != nil {
		t.Fatal(err)
	}
	if string(response) != want {
		t.Fatalf("response=%q, want %q", response, want)
	}
}
