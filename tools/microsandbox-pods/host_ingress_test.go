package main

import (
	"net"
	"net/netip"
	"testing"
)

func TestHostIngressClosesBeforeReuseAndFencesControllerShutdown(t *testing.T) {
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatal(err)
	}
	bind := ""
	for _, address := range addresses {
		prefix, err := netip.ParsePrefix(address.String())
		if err == nil && prefix.Addr().Is4() && !prefix.Addr().IsLoopback() {
			bind = prefix.Addr().String()
			break
		}
	}
	if bind == "" {
		t.Skip("requires a local IPv4 interface")
	}
	backend, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	n := &network{proxyBindIP: bind, peerIP: "192.0.2.2"}
	defer n.closeIngresses()
	e := &podEntry{vm: "owned", slot: 2, port: backend.LocalAddr().(*net.UDPAddr).Port}
	if err := n.ensureIngress(e); err != nil {
		t.Fatal(err)
	}
	first := n.ingress[e.vm]
	if err := n.ensureIngress(e); err != nil || n.ingress[e.vm] != first {
		t.Fatal("duplicate ingress", err)
	}
	n.closeIngress(e)
	if first.Ready() || len(n.ingress) != 0 {
		t.Fatal("stopped guest kept its ingress")
	}
	if err := n.ensureIngress(e); err != nil || n.ingress[e.vm] == first {
		t.Fatal("restored guest could not rebuild ingress", err)
	}
	n.closeIngresses()
	if err := n.ensureIngress(e); err == nil || len(n.ingress) != 0 {
		t.Fatal("late startup reopened ingress after controller shutdown")
	}
}
