package main

import (
	"errors"
	"net/netip"

	"github.com/floci-io/floci/tools/microsandbox-pods/internal/hosttransport"
)

func (n *network) ensureIngress(e *podEntry) error {
	if n == nil || n.proxyBindIP == "" || n.proxyBindIP == "127.0.0.1" {
		return nil
	}
	n.ingressMu.Lock()
	defer n.ingressMu.Unlock()
	if n.ingressClosed {
		return errors.New("owned VXLAN ingress is shutting down")
	}
	if current := n.ingress[e.vm]; current != nil {
		if !current.Ready() {
			return errors.New("owned VXLAN ingress stopped unexpectedly")
		}
		return nil
	}
	bind, err := netip.ParseAddr(n.proxyBindIP)
	if err != nil {
		return err
	}
	peer, err := netip.ParseAddr(n.peerIP)
	if err != nil {
		return err
	}
	forwarder, err := hosttransport.ListenVXLAN(bind, peer, e.port, peerVXLANSourcePort(e), uint32(10000+e.slot))
	if err != nil {
		return err
	}
	if n.ingress == nil {
		n.ingress = make(map[string]*hosttransport.VXLANIngress)
	}
	n.ingress[e.vm] = forwarder
	return nil
}

func (n *network) closeIngress(e *podEntry) {
	if n == nil {
		return
	}
	n.ingressMu.Lock()
	defer n.ingressMu.Unlock()
	if current := n.ingress[e.vm]; current != nil {
		current.Close()
		delete(n.ingress, e.vm)
	}
}

func (n *network) closeIngresses() {
	if n == nil {
		return
	}
	n.ingressMu.Lock()
	defer n.ingressMu.Unlock()
	n.ingressClosed = true
	for name, current := range n.ingress {
		current.Close()
		delete(n.ingress, name)
	}
}
