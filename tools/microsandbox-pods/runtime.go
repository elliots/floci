package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/floci-io/floci/tools/microsandbox-pods/internal/hosttransport"

	msbruntime "github.com/floci-io/floci/tools/microsandbox-runtime"
)

const guestImage = "rancher/k3s:v1.35.0-k3s1"
const ownerLabel = "io.floci.pod-probe"
const bridgeName = "pvkb0"
const ctr = "ctr --address /run/containerd/containerd.sock"

type config struct {
	networkPolicy                                           bool
	guestFSBinary                                           string
	relayBinary, proxyBackendCIDR                           string
	kubeconfig, nodeName, msb, home, state, peer, peerOwner string
	podCIDR, clusterCIDR, serviceCIDR, clusterDNS           string
	clusterName                                             string
	maxPods, maxActiveMemoryMiB                             int
	podWorkers                                              int
	schedulingCPU, schedulingMemory                         string
	kubeAPIQPS                                              float64
	kubeAPIBurst                                            int
	cleanupOnExit                                           bool
	controlListen, tokenFile                                string
	listen, nodeIP, cert, key, ca                           string
}

type commandRunner struct {
	cfg     config
	runtime nativeRuntime
}

var nativeMutations sync.Mutex

func (r commandRunner) command(ctx context.Context, binary string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = append(os.Environ(), "MSB_HOME="+r.cfg.home)
	return cmd
}

func (r commandRunner) run(ctx context.Context, binary string, args ...string) (string, error) {
	output, err := r.command(ctx, binary, args...).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s %s failed: %w", filepath.Base(binary), args[0], err)
	}
	return strings.TrimSpace(string(output)), nil
}

func (r commandRunner) vm(ctx context.Context, args ...string) (string, error) {
	if nativeMutation(args) {
		// Accepted HTTP wakes already have a bounded Pod lifetime context.
		// Other lifecycle callers retain their existing mutation fence behavior.
		if accepted, _ := ctx.Value(acceptedHTTPWakeKey{}).(bool); !accepted {
			ctx = context.WithoutCancel(ctx)
			nativeMutations.Lock()
		} else {
			for !nativeMutations.TryLock() {
				select {
				case <-ctx.Done():
					return "", ctx.Err()
				case <-time.After(25 * time.Millisecond):
				}
			}
		}
		defer nativeMutations.Unlock()
		if err := ctx.Err(); err != nil {
			return "", err
		}
		if err := r.checkPending(); err != nil {
			return "", err
		}
		marker := filepath.Join(r.cfg.state, "native-command.pending")
		record := r.commandIntent(ctx, args)
		if err := r.writeCommandRecord(record, true); err != nil {
			return "", err
		}
		output, err := r.nativeOutput(ctx, args, nil)
		if err != nil {
			if errors.Is(err, msbruntime.ErrCapacityUnavailable) {
				if proofErr := r.completeCommand(ctx, &record, true); proofErr != nil {
					return "", errors.Join(err, proofErr)
				}
				return "", errors.Join(err, os.Remove(marker), syncDirectory(r.cfg.state))
			}
			return "", err
		}
		if err := r.completeCommand(ctx, &record, false); err != nil {
			return "", err
		}
		if err := os.Remove(marker); err != nil {
			return "", err
		}
		return strings.TrimSpace(string(output)), syncDirectory(r.cfg.state)
	}
	output, err := r.nativeOutput(ctx, args, nil)
	return strings.TrimSpace(string(output)), err
}

func nativeMutation(args []string) bool {
	if len(args) == 0 {
		return false
	}
	switch args[0] {
	case "run", "create", "start", "pause", "stop", "resume", "rm":
		return true
	case "snap":
		return len(args) > 1 && (args[1] == "create" || args[1] == "restore" || args[1] == "rm")
	default:
		return false
	}
}

func (r commandRunner) checkPending() error {
	_, err := os.Stat(filepath.Join(r.cfg.state, "native-command.pending"))
	if err == nil {
		return fmt.Errorf("an uncertain native command is journaled; refuse lifecycle mutations until its outcome is reconciled")
	}
	if !os.IsNotExist(err) {
		return err
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func (r commandRunner) guest(ctx context.Context, name, script string) (string, error) {
	return r.vm(ctx, "exec", "--no-stdin", name, "--", "sh", "-ec", script)
}

func (r commandRunner) guestInput(ctx context.Context, name, script string, input []byte) error {
	if _, err := r.nativeOutput(ctx, []string{"exec", "--stream", name, "--", "sh", "-ec", script}, input); err != nil {
		return fmt.Errorf("write guest volume data: %w", err)
	}
	return nil
}

func (r commandRunner) peer(ctx context.Context, script string) (string, error) {
	return r.run(ctx, "docker", "exec", r.cfg.peer, "sh", "-ec", script)
}

func quote(value string) string { return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'" }
func joinCommand(args []string) string {
	quoted := make([]string, len(args))
	for i, value := range args {
		quoted[i] = quote(value)
	}
	return strings.Join(quoted, " ")
}

type network struct {
	ingressMu      sync.Mutex
	ingressClosed  bool
	ingress        map[string]*hosttransport.VXLANIngress
	backendPrefix  netip.Prefix
	runner         commandRunner
	prefix         netip.Prefix
	gateway        netip.Addr
	peerIP, hostIP string
	proxyBindIP    string
	peerPort       int
}

func newNetwork(ctx context.Context, runner commandRunner, reusing bool) (*network, error) {
	hostIP, err := peerHostAddress(runner.cfg.nodeIP)
	if err != nil {
		return nil, err
	}
	out, err := runner.run(ctx, "docker", "inspect", runner.cfg.peer)
	if err != nil {
		return nil, err
	}
	var containers []struct {
		Config          struct{ Labels map[string]string }
		NetworkSettings struct {
			Networks map[string]struct{ IPAddress string }
			Ports    map[string][]struct{ HostIP, HostPort string }
		}
	}
	if err := json.Unmarshal([]byte(out), &containers); err != nil {
		return nil, err
	}
	if len(containers) != 1 || containers[0].Config.Labels[ownerLabel] != runner.cfg.peerOwner {
		return nil, fmt.Errorf("network peer does not carry the expected ownership label")
	}
	peer := containers[0]
	ports := peer.NetworkSettings.Ports["8472/udp"]
	if len(ports) != 1 {
		return nil, fmt.Errorf("owned network peer must publish exactly one UDP 8472 binding")
	}
	port, err := strconv.Atoi(ports[0].HostPort)
	if err != nil {
		return nil, err
	}
	peerIP := ""
	for _, attached := range peer.NetworkSettings.Networks {
		if attached.IPAddress != "" {
			if peerIP != "" && peerIP != attached.IPAddress {
				return nil, errors.New("owned network peer has ambiguous IPv4 interfaces")
			}
			peerIP = attached.IPAddress
		}
	}
	if net.ParseIP(peerIP) == nil {
		return nil, fmt.Errorf("network peer has no IPv4 address")
	}
	prefix, err := netip.ParsePrefix(runner.cfg.podCIDR)
	if err != nil || !prefix.Addr().Is4() || prefix.Bits() != 22 {
		return nil, fmt.Errorf("qualification requires an IPv4 /22 pod CIDR")
	}
	backendPrefix, err := netip.ParsePrefix(runner.cfg.proxyBackendCIDR)
	if err != nil || !backendPrefix.Addr().Is4() || backendPrefix.Bits() != 22 || backendPrefix.Overlaps(prefix) {
		return nil, errors.New("proxy backend CIDR must be a distinct IPv4 /22")
	}
	clusterPrefix, err := netip.ParsePrefix(runner.cfg.clusterCIDR)
	if err != nil {
		return nil, err
	}
	servicePrefix, err := netip.ParsePrefix(runner.cfg.serviceCIDR)
	if err != nil {
		return nil, err
	}
	if !clusterPrefix.Contains(prefix.Addr()) || !clusterPrefix.Contains(backendPrefix.Addr()) || clusterPrefix.Bits() > 22 ||
		servicePrefix.Overlaps(prefix) || servicePrefix.Overlaps(backendPrefix) {
		return nil, errors.New("public and proxy backend prefixes must be reserved inside cluster CIDR and outside service CIDR")
	}
	gateway := prefix.Masked().Addr().Next()
	n := &network{runner: runner, prefix: prefix, backendPrefix: backendPrefix, gateway: gateway, peerIP: peerIP, hostIP: hostIP, peerPort: port}
	n.proxyBindIP, err = peerListenerAddress(ctx, runner, hostIP)
	if err != nil {
		return nil, err
	}
	if err := n.pinHostIdentity(ctx); err != nil {
		return nil, err
	}
	if reusing {
		if _, err := runner.peer(ctx, "ip link show "+bridgeName); err == nil {
			return n, nil
		}
		return nil, fmt.Errorf("the journaled network bridge is missing; refusing to recreate live pod routing")
	}
	hash := sha256.Sum256([]byte(runner.cfg.peerOwner))
	mac := fmt.Sprintf("02:%02x:%02x:%02x:%02x:%02x", hash[0], hash[1], hash[2], hash[3], hash[4])
	script := fmt.Sprintf("ip link add %s type bridge; ip link set %s address %s; ip addr add %s/22 dev %s; ip link set %s mtu 1450 up; "+
		"iptables -I FORWARD 1 -s %s -j ACCEPT; iptables -I FORWARD 1 -d %s -j ACCEPT; "+
		"iptables -t nat -I POSTROUTING 1 -s %s -d %s -m mark ! --mark 0x4000/0x4000 -j RETURN",
		bridgeName, bridgeName, mac, gateway, bridgeName, bridgeName, prefix, prefix, prefix, runner.cfg.clusterCIDR)
	if _, err := runner.peer(ctx, script); err != nil {
		return nil, err
	}
	return n, nil
}

func (n *network) address(slot int) string {
	return addressAt(n.prefix, slot)
}

func addressAt(prefix netip.Prefix, slot int) string {
	ip := prefix.Masked().Addr()
	for range slot + 2 {
		ip = ip.Next()
	}
	return ip.String()
}

func (n *network) attach(ctx context.Context, e *podEntry) error {
	if err := n.ensureIngress(e); err != nil {
		return err
	}
	guestIP, err := n.runner.guest(ctx, e.vm, "ip -4 -o addr show dev eth0 | awk '{print $4}' | cut -d/ -f1")
	if err != nil || net.ParseIP(guestIP) == nil {
		return fmt.Errorf("cannot find guest address: %w", err)
	}
	gateway, err := n.runner.guest(ctx, e.vm, "ip route show default | awk 'NR==1 {print $3}'")
	if err != nil || net.ParseIP(gateway) == nil {
		return fmt.Errorf("cannot find guest gateway: %w", err)
	}
	e.guestIP = guestIP
	podIP := e.ip
	podGateway := n.gateway
	if e.proxyAvailability {
		podIP = e.backendIP
		podGateway = n.backendPrefix.Masked().Addr().Next()
	}
	vni := 10000 + e.slot
	script := n.peerVXLANLink(e) + "; " + n.peerVXLANRules(e) + "; ip link set " + e.link + " up"
	if _, err := n.runner.peer(ctx, script); err != nil {
		return err
	}
	script = fmt.Sprintf("ip link add pod0 type vxlan id %d local %s remote %s dev eth0 dstport 8472 nolearning; "+
		"ip addr add %s/22 dev pod0; ip link set pod0 mtu 1450 up; "+
		"ip route add %s via %s dev pod0; ip route add %s via %s dev pod0; "+
		"ip route add 169.254.170.23/32 via %s dev pod0; "+
		"iptables -t nat -A OUTPUT -p udp -d %s --dport 8472 -j DNAT --to-destination %s:%d; "+
		"printf 'nameserver %s\\nsearch %s.svc.cluster.local svc.cluster.local cluster.local\\noptions ndots:5\\n' >/etc/pod-resolv.conf",
		vni, guestIP, n.peerIP, podIP, n.runner.cfg.clusterCIDR, podGateway,
		n.runner.cfg.serviceCIDR, podGateway, podGateway, n.peerIP, gateway, n.peerPort,
		n.runner.cfg.clusterDNS, e.pod.Namespace)
	_, err = n.runner.guest(ctx, e.vm, script)
	return err
}

func (n *network) detach(ctx context.Context, e *podEntry) error {
	if e.guestIP == "" {
		return nil
	}
	script := fmt.Sprintf("if ip link show %s >/dev/null 2>&1; then ip link del %s; fi; ", e.link, e.link)
	for _, rule := range []string{n.peerVXLANRule(e), n.legacyPeerVXLANRule(e)} {
		script += "while iptables -t nat -C " + rule + " 2>/dev/null; do iptables -t nat -D " + rule + "; done; "
	}
	_, err := n.runner.peer(ctx, script+n.clearPeerVXLANConntrack(e))
	return err
}

func (n *network) prepareProxy(ctx context.Context, e *podEntry) error {
	gateway := n.backendPrefix.Masked().Addr().Next()
	rules := []string{"FORWARD -s " + n.backendPrefix.String() + " -j ACCEPT", "FORWARD -d " + n.backendPrefix.String() + " -j ACCEPT"}
	script := fmt.Sprintf("ip addr replace %s/22 dev %s; ip addr replace %s/32 dev %s; ", gateway, bridgeName, e.ip, bridgeName)
	for _, rule := range rules {
		script += "if ! iptables -C " + rule + " 2>/dev/null; then iptables -A " + rule + "; fi; "
	}
	rule := fmt.Sprintf("POSTROUTING -s %s -d %s -m mark ! --mark 0x4000/0x4000 -j RETURN", n.backendPrefix, n.runner.cfg.clusterCIDR)
	script += "if ! iptables -t nat -C " + rule + " 2>/dev/null; then iptables -t nat -I " + rule + "; fi"
	_, err := n.runner.peer(ctx, script)
	return err
}

func (n *network) removeProxy(ctx context.Context, e *podEntry) error {
	script := fmt.Sprintf("if ip -4 addr show dev %s | grep -F 'inet %s/32 ' >/dev/null; then ip addr del %s/32 dev %s; fi", bridgeName, e.ip, e.ip, bridgeName)
	_, err := n.runner.peer(ctx, script)
	return err
}

func (n *network) close(ctx context.Context) error {
	script := "ip link del " + bridgeName + "; "
	for _, prefix := range []netip.Prefix{n.prefix, n.backendPrefix} {
		for _, rule := range []string{"FORWARD -s " + prefix.String() + " -j ACCEPT", "FORWARD -d " + prefix.String() + " -j ACCEPT"} {
			script += "if iptables -C " + rule + " 2>/dev/null; then iptables -D " + rule + "; fi; "
		}
		rule := fmt.Sprintf("POSTROUTING -s %s -d %s -m mark ! --mark 0x4000/0x4000 -j RETURN", prefix, n.runner.cfg.clusterCIDR)
		script += "if iptables -t nat -C " + rule + " 2>/dev/null; then iptables -t nat -D " + rule + "; fi; "
	}
	_, err := n.runner.peer(ctx, script)
	return err
}

func availableUDPPort() (int, error) {
	listener, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.LocalAddr().(*net.UDPAddr).Port, nil
}

func uniquePort(used map[int]bool, allocate func() (int, error)) (int, error) {
	for range 65536 {
		port, err := allocate()
		if err != nil {
			return 0, err
		}
		if !used[port] {
			used[port] = true
			return port, nil
		}
	}
	return 0, fmt.Errorf("cannot allocate an unclaimed native host port")
}

func boundedContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, 4*time.Minute)
}
