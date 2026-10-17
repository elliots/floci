// Package hosttransport provides bounded host ingress for the owned network peer.
package hosttransport

import (
	"errors"
	"net"
	"net/netip"
	"sync"
)

// VXLANIngress forwards peer-to-guest datagrams to a private SDK publication.
// Guest return traffic uses the separately configured peer VXLAN route.
type VXLANIngress struct {
	listener *net.UDPConn
	upstream *net.UDPConn
	done     chan struct{}
	once     sync.Once
}

func ListenVXLAN(bind, peer netip.Addr, port, sourcePort int, vni uint32) (*VXLANIngress, error) {
	if !unicastIPv4(bind) || bind.IsLoopback() || !unicastIPv4(peer) || port < 1 || port > 65535 || sourcePort < 1 || sourcePort > 65535 || vni == 0 || vni > 0xffffff {
		return nil, errors.New("VXLAN ingress requires an explicit host interface, peer IPv4, port and VNI")
	}
	listener, err := net.ListenUDP("udp4", net.UDPAddrFromAddrPort(netip.AddrPortFrom(bind, uint16(port))))
	if err != nil {
		return nil, err
	}
	upstream, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		listener.Close()
		return nil, err
	}
	forwarder := &VXLANIngress{listener: listener, upstream: upstream, done: make(chan struct{})}
	go forwarder.forward(netip.AddrPortFrom(peer, uint16(sourcePort)), port, vni)
	return forwarder, nil
}

func unicastIPv4(address netip.Addr) bool {
	return address.Is4() && !address.IsUnspecified() && !address.IsMulticast() && address != netip.MustParseAddr("255.255.255.255")
}

func validVXLAN(packet []byte, vni uint32) bool {
	return len(packet) >= 22 && packet[0] == 8 && packet[1] == 0 && packet[2] == 0 && packet[3] == 0 && packet[7] == 0 &&
		uint32(packet[4])<<16|uint32(packet[5])<<8|uint32(packet[6]) == vni
}

func (f *VXLANIngress) forward(peer netip.AddrPort, port int, vni uint32) {
	defer close(f.done)
	defer f.closeSockets()
	target := netip.AddrPortFrom(netip.MustParseAddr("127.0.0.1"), uint16(port))
	packet := make([]byte, 65535)
	for {
		n, source, err := f.listener.ReadFromUDPAddrPort(packet)
		if err != nil {
			return
		}
		if source != peer || !validVXLAN(packet[:n], vni) {
			continue
		}
		if _, err := f.upstream.WriteToUDPAddrPort(packet[:n], target); err != nil {
			// UDP has no delivery acknowledgement. A stopped guest can discard
			// packets without allocating per-source sessions or retry queues.
			continue
		}
	}
}

func (f *VXLANIngress) Ready() bool {
	select {
	case <-f.done:
		return false
	default:
		return true
	}
}

func (f *VXLANIngress) closeSockets() {
	f.once.Do(func() {
		f.listener.Close()
		f.upstream.Close()
	})
}

func (f *VXLANIngress) Close() {
	f.closeSockets()
	<-f.done
}
