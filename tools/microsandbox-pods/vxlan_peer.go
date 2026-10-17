package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

func peerVXLANSourcePort(e *podEntry) int { return 20000 + e.slot }

func (n *network) peerVXLANRule(e *podEntry) string {
	match := fmt.Sprintf(" --sport %d", peerVXLANSourcePort(e))
	return fmt.Sprintf("OUTPUT -p udp -s %s -d %s%s --dport 8472 -j DNAT --to-destination %s:%d", n.peerIP, e.guestIP, match, n.hostIP, e.port)
}

func (n *network) legacyPeerVXLANRule(e *podEntry) string {
	return fmt.Sprintf("OUTPUT -p udp -d %s --dport 8472 -j DNAT --to-destination %s:%d", e.guestIP, n.hostIP, e.port)
}

func (n *network) peerVXLANLink(e *podEntry) string {
	// Linux hashes into [low, high); equal bounds select its default ephemeral
	// range. The stable slot keeps this tuple unique when SDK guest IPs repeat.
	return fmt.Sprintf("ip link add %s type vxlan id %d local %s remote %s dev eth0 srcport %d %d dstport 8472 nolearning; "+
		"ip link set %s master %s; ip link set %s mtu 1450", e.link, 10000+e.slot, n.peerIP, e.guestIP, peerVXLANSourcePort(e), peerVXLANSourcePort(e)+1, e.link, bridgeName, e.link)
}

func (n *network) clearPeerVXLANConntrack(e *podEntry) string {
	filter := fmt.Sprintf("-p udp -s %s -d %s --sport %d --dport 8472", n.peerIP, e.guestIP, peerVXLANSourcePort(e))
	return "if ! conntrack -D " + filter + " >/dev/null 2>&1; then entries=$(conntrack -L " + filter + " 2>/dev/null); test -z \"$entries\"; fi"
}

func (n *network) peerVXLANRules(e *podEntry) string {
	rule, legacy := n.peerVXLANRule(e), n.legacyPeerVXLANRule(e)
	return "if ! iptables -t nat -C " + rule + " 2>/dev/null; then iptables -t nat -I " + rule + "; fi; " +
		"while iptables -t nat -C " + legacy + " 2>/dev/null; do iptables -t nat -D " + legacy + "; done; " + n.clearPeerVXLANConntrack(e)
}

func (n *network) reconcilePeerVXLAN(ctx context.Context, e *podEntry) error {
	if e.guestIP == "" {
		return nil
	}
	data, err := n.runner.peer(ctx, "if ip link show "+e.link+" >/dev/null 2>&1; then ip -d -j link show dev "+e.link+"; else printf '[]'; fi")
	if err != nil {
		return err
	}
	recreate, err := n.peerVXLANNeedsMigration([]byte(data), e)
	if err != nil {
		return err
	}
	script := ""
	if recreate {
		// The peer-side link owns no guest state. The kernel cannot change a
		// VXLAN source-port range in place; preserve its VNI and receive port.
		script = "if ip link show " + e.link + " >/dev/null 2>&1; then ip link del " + e.link + "; fi; " + n.peerVXLANLink(e) + "; "
	}
	_, err = n.runner.peer(ctx, script+"ip link set "+e.link+" down; "+n.peerVXLANRules(e)+"; ip link set "+e.link+" up")
	return err
}

func (n *network) peerVXLANNeedsMigration(data []byte, e *podEntry) (bool, error) {
	var links []struct {
		Name   string `json:"ifname"`
		Master string `json:"master"`
		MTU    int    `json:"mtu"`
		Info   struct {
			Kind string `json:"info_kind"`
			Data struct {
				ID                  int `json:"id"`
				Local, Remote, Link string
				Port                int                     `json:"port"`
				Range               struct{ Low, High int } `json:"port_range"`
				Learning            bool                    `json:"learning"`
			} `json:"info_data"`
		} `json:"linkinfo"`
	}
	if err := json.Unmarshal(data, &links); err != nil {
		return false, err
	}
	if len(links) == 0 {
		return true, nil
	}
	if len(links) != 1 {
		return false, errors.New("owned peer VXLAN identity is ambiguous")
	}
	link := links[0]
	config := link.Info.Data
	if link.Name != e.link || link.Master != bridgeName || link.MTU != 1450 || link.Info.Kind != "vxlan" ||
		config.ID != 10000+e.slot || config.Local != n.peerIP || config.Remote != e.guestIP || config.Link != "eth0" || config.Port != 8472 || config.Learning {
		return false, errors.New("peer VXLAN link differs from the owned Pod network journal")
	}
	if config.Range.Low == peerVXLANSourcePort(e) && config.Range.High == peerVXLANSourcePort(e)+1 {
		return false, nil
	}
	if config.Range.Low != 0 || config.Range.High != 0 {
		return false, errors.New("peer VXLAN source-port ownership changed")
	}
	return true, nil
}
