package hosttransport

import (
	"bytes"
	"net"
	"net/netip"
	"testing"
	"time"
)

func hostInterface(t *testing.T) netip.Addr {
	t.Helper()
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatal(err)
	}
	for _, address := range addresses {
		prefix, err := netip.ParsePrefix(address.String())
		if err == nil && unicastIPv4(prefix.Addr()) && !prefix.Addr().IsLoopback() {
			return prefix.Addr()
		}
	}
	t.Skip("requires a local IPv4 interface")
	return netip.Addr{}
}

func packet(vni uint32) []byte {
	data := make([]byte, 32)
	data[0], data[4], data[5], data[6] = 8, byte(vni>>16), byte(vni>>8), byte(vni)
	copy(data[8:], []byte("owned ethernet payload"))
	return data
}

func udpListener(t *testing.T, address netip.Addr, port int) *net.UDPConn {
	t.Helper()
	listener, err := net.ListenUDP("udp4", net.UDPAddrFromAddrPort(netip.AddrPortFrom(address, uint16(port))))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	return listener
}

func TestVXLANIngressAcceptsOnlyOwnedPeerTupleAndVNI(t *testing.T) {
	bind := hostInterface(t)
	backend := udpListener(t, netip.MustParseAddr("127.0.0.1"), 0)
	port := backend.LocalAddr().(*net.UDPAddr).Port
	peer := udpListener(t, bind, 0)
	wrongPort := udpListener(t, bind, 0)
	wrongIP := udpListener(t, netip.MustParseAddr("127.0.0.1"), 0)
	forwarder, err := ListenVXLAN(bind, bind, port, peer.LocalAddr().(*net.UDPAddr).Port, 10001)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(forwarder.Close)
	target := netip.AddrPortFrom(bind, uint16(port))
	for _, sender := range []*net.UDPConn{wrongPort, wrongIP} {
		if _, err := sender.WriteToUDPAddrPort(packet(10001), target); err != nil {
			t.Fatal(err)
		}
	}
	badFlags := packet(10001)
	badFlags[0] = 0
	reserved := packet(10001)
	reserved[7] = 1
	for _, data := range [][]byte{packet(10002), badFlags, reserved, {8, 0, 0, 0, 0, 0, 1, 0}} {
		if _, err := peer.WriteToUDPAddrPort(data, target); err != nil {
			t.Fatal(err)
		}
	}
	wanted := packet(10001)
	if _, err := peer.WriteToUDPAddrPort(wanted, target); err != nil {
		t.Fatal(err)
	}
	if err := backend.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 128)
	n, source, err := backend.ReadFromUDPAddrPort(buffer)
	if err != nil || !bytes.Equal(buffer[:n], wanted) || !source.Addr().IsLoopback() {
		t.Fatal("owned VXLAN packet did not reach private SDK endpoint", n, source, err)
	}
	if err := backend.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := backend.ReadFromUDPAddrPort(buffer); err == nil {
		t.Fatal("unowned datagram reached private SDK endpoint")
	}
	forwarder.Close()
	if forwarder.Ready() {
		t.Fatal("closed ingress still ready")
	}
	replacement := udpListener(t, bind, port)
	if replacement.LocalAddr().(*net.UDPAddr).AddrPort() != target {
		t.Fatal("frontend socket was not released")
	}
}

func TestVXLANIngressRejectsWildcardAndInvalidIdentity(t *testing.T) {
	bind, peer := netip.MustParseAddr("192.0.2.1"), netip.MustParseAddr("192.0.2.2")
	for _, invalid := range []netip.Addr{{}, netip.MustParseAddr("0.0.0.0"), netip.MustParseAddr("::1"), netip.MustParseAddr("224.0.0.1"), netip.MustParseAddr("255.255.255.255"), netip.MustParseAddr("127.0.0.1")} {
		if _, err := ListenVXLAN(invalid, peer, 40001, 20000, 10000); err == nil {
			t.Fatal("unsafe bind accepted", invalid)
		}
	}
	for _, source := range []int{0, 65536} {
		if _, err := ListenVXLAN(bind, peer, 40001, source, 10000); err == nil {
			t.Fatal("invalid source port accepted", source)
		}
	}
}
