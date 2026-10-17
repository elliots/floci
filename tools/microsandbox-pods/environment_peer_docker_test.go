package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	msbruntime "github.com/floci-io/floci/tools/microsandbox-runtime"
)

// This fixture deliberately installs no outer Floci firewall. It proves that the
// peer's own boundary checks the final destination after Service DNAT and still
// distinguishes an application source from peer sockets before source NAT.
func TestDockerEnvironmentPeerEgressBeforeSourceNAT(t *testing.T) {
	if os.Getenv("FLOCI_TEST_DOCKER_ENVIRONMENT") != "1" {
		t.Skip("set FLOCI_TEST_DOCKER_ENVIRONMENT=1 for disposable privileged Docker peer fixtures")
	}
	f := newEnvironmentDockerFixture(t)
	peer := f.container(t, "peer", "--privileged", "--network", f.protected, "--entrypoint", "sh", guestImage,
		"-ec", "sleep 600")
	probe := f.container(t, "probe", "--privileged", "--network", "container:"+peer, f.image,
		"sh", "-ec", "socat TCP4-LISTEN:8084,fork,reuseaddr EXEC:cat & wait")
	f.probe = probe
	local := f.container(t, "local", "--network", f.protected, f.image, "sh", "-ec", environmentEchoServers)
	allowed := f.container(t, "allowed", "--network", f.outside, f.image, "sh", "-ec", environmentEchoServers)
	denied := f.container(t, "denied", "--network", f.outside, f.image, "sh", "-ec", environmentEchoServers)
	host := f.container(t, "host", "--network", "host", f.image, "sh", "-ec",
		"socat -d -d TCP4-LISTEN:0,fork,reuseaddr EXEC:cat 2>/tmp/tcp.log & "+
			"socat -d -d TCP4-LISTEN:0,fork,reuseaddr EXEC:cat 2>/tmp/other.log & "+
			"socat -d -d UDP4-RECVFROM:0,fork,reuseaddr EXEC:cat 2>/tmp/udp.log & "+
			"socat -d -d TCP4-LISTEN:0,fork,reuseaddr EXEC:cat 2>/tmp/global-tcp.log & "+
			"socat -d -d UDP4-RECVFROM:0,fork,reuseaddr EXEC:cat 2>/tmp/global-udp.log & "+
			"socat -d -d UDP4-RECVFROM:0,fork,reuseaddr EXEC:cat 2>/tmp/other-udp.log & wait")
	peerIP, _ := f.addresses(t, peer)
	localIP, _ := f.addresses(t, local)
	allowedIP, _ := f.addresses(t, allowed)
	deniedIP, deniedIPv6 := f.addresses(t, denied)
	hostTCP, hostUDP := f.listenerPort(t, host, "tcp"), f.listenerPort(t, host, "udp")
	otherHostTCP := f.listenerPort(t, host, "other")
	globalHostTCP, globalHostUDP := f.listenerPort(t, host, "global-tcp"), f.listenerPort(t, host, "global-udp")
	otherHostUDP := f.listenerPort(t, host, "other-udp")

	f.must(t, "exec", probe, "sh", "-ec", `
ip netns add app
ip link add fixture-peer type veth peer name fixture-app
ip link set fixture-app netns app
ip addr add 10.42.240.1/24 dev fixture-peer
ip -6 addr add fd42:240::1/64 dev fixture-peer nodad
ip link set fixture-peer up
ip netns exec app ip link set lo up
ip netns exec app ip addr add 10.42.240.2/24 dev fixture-app
ip netns exec app ip -6 addr add fd42:240::2/64 dev fixture-app nodad
ip netns exec app ip link set fixture-app up
ip netns exec app ip route add default via 10.42.240.1
ip netns exec app ip -6 route add default via fd42:240::1
sysctl -w net.ipv4.ip_forward=1 net.ipv6.conf.all.forwarding=1
`)
	f.must(t, "exec", peer, "sh", "-ec", `
iptables -t nat -A POSTROUTING -s 10.42.240.0/24 -o eth0 -j MASQUERADE
ip6tables -t nat -A POSTROUTING -s fd42:240::/64 -o eth0 -j MASQUERADE
iptables -I FORWARD 1 -j ACCEPT
ip6tables -I FORWARD 1 -j ACCEPT
iptables -I OUTPUT 1 -j ACCEPT
ip6tables -I OUTPUT 1 -j ACCEPT
`)
	for _, chain := range []string{"OUTPUT", "PREROUTING"} {
		for service, endpoint := range map[string]string{"10.43.9.10": deniedIP, "10.43.9.11": localIP} {
			f.must(t, "exec", peer, "iptables", "-t", "nat", "-A", chain,
				"-d", service+"/32", "-p", "tcp", "--dport", "8080", "-j", "DNAT", "--to-destination", endpoint+":8080")
		}
	}

	checks := []struct {
		name, address, protocol string
		port                    int
		allowed                 bool
	}{
		{"protected simulator", localIP, "tcp", 8080, true},
		{"explicit public IPv4", allowedIP, "tcp", 8080, true},
		{"unlisted listening port", allowedIP, "tcp", 8081, false},
		{"explicit UDP", allowedIP, "udp", 8082, true},
		{"unlisted UDP port", allowedIP, "udp", 8083, false},
		{"unlisted direct IPv4", deniedIP, "tcp", 8080, false},
		{"unlisted IPv6", deniedIPv6, "tcp", 8080, false},
		{"Service DNAT to outside", "10.43.9.10", "tcp", 8080, false},
		{"Service DNAT to simulator", "10.43.9.11", "tcp", 8080, true},
		{"unlisted host listener", f.gateway, "tcp", otherHostTCP, false},
		{"explicit gateway TCP", f.gateway, "tcp", globalHostTCP, true},
		{"explicit gateway UDP", f.gateway, "udp", globalHostUDP, true},
		{"unlisted gateway UDP", f.gateway, "udp", otherHostUDP, false},
	}
	// Positive controls prove every denied case reaches a real listening service
	// before the boundary is installed, including forwarded IPv6 and DNAT paths.
	for _, forwarded := range []bool{false, true} {
		for _, check := range checks {
			f.expect(t, probe, forwarded, check.address, check.protocol, check.port, true, "baseline "+check.name)
		}
		f.expect(t, probe, forwarded, f.gateway, "tcp", hostTCP, true, "baseline native TCP listener")
		f.expect(t, probe, forwarded, f.gateway, "udp", hostUDP, true, "baseline native UDP listener")
	}

	boundary := &environmentBoundary{
		runner: commandRunner{cfg: config{peer: peer, peerOwner: f.owner, home: t.TempDir(),
			clusterCIDR: "10.42.0.0/16", serviceCIDR: "10.43.0.0/16"}},
		identity: environmentIdentity{PeerID: peer, PeerIP: peerIP, HostIP: f.gateway,
			Prefixes: []string{f.prefix}, Gateways: []string{f.gateway}},
		policy: msbruntime.NetworkBoundaryInfo{GlobalEgress: []msbruntime.NetworkEgressEndpoint{
			{Destination: allowedIP + "/32", Protocol: "tcp", Ports: "8080"},
			{Destination: allowedIP + "/32", Protocol: "udp", Ports: "8082"},
			{Destination: f.gateway + "/32", Protocol: "tcp", Ports: strconv.Itoa(globalHostTCP)},
			{Destination: f.gateway + "/32", Protocol: "udp", Ports: strconv.Itoa(globalHostUDP)},
		}},
		ports: map[environmentPort]bool{{Protocol: "tcp", Port: hostTCP}: true, {Protocol: "udp", Port: hostUDP}: true},
	}
	if err := boundary.installPeer(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := boundary.verifyPeer(t.Context()); err != nil {
		f.diagnostics(t, peer, boundary.chain())
		t.Fatal("real iptables canonical output did not pass policy verification", err)
	}
	for _, forwarded := range []bool{false, true} {
		for _, check := range checks {
			f.expect(t, probe, forwarded, check.address, check.protocol, check.port, check.allowed, check.name)
		}
		f.expect(t, probe, forwarded, f.gateway, "tcp", hostTCP, !forwarded, "native TCP tuple is peer-only before SNAT")
		f.expect(t, probe, forwarded, f.gateway, "udp", hostUDP, !forwarded, "native UDP tuple is peer-only before SNAT")
	}
	f.expect(t, denied, false, peerIP, "tcp", 8084, true, "replies to inbound requests remain permitted")

	if err := boundary.installPeer(t.Context()); err != nil {
		t.Fatal("idempotent policy refresh", err)
	}
	if err := boundary.verifyPeer(t.Context()); err != nil {
		t.Fatal("policy refresh changed canonical rules", err)
	}
	f.expect(t, probe, true, localIP, "tcp", 8080, true, "simulator after refresh")
	f.expect(t, probe, true, f.gateway, "tcp", hostTCP, false, "peer tuple stays private after refresh")
	f.must(t, "exec", peer, "iptables", "-t", "mangle", "-I", "POSTROUTING", "1", "-j", "RETURN")
	if err := boundary.verifyPeer(t.Context()); err == nil {
		t.Error("verification accepted a rule before the mandatory POSTROUTING boundary")
	}
	f.must(t, "exec", peer, "iptables", "-t", "mangle", "-D", "POSTROUTING", "1")
	if err := boundary.verifyPeer(t.Context()); err != nil {
		t.Fatal("restoring the owned hook did not restore verification", err)
	}
}

const environmentEchoServers = "socat TCP6-LISTEN:8080,ipv6only=0,fork,reuseaddr EXEC:cat & " +
	"socat TCP6-LISTEN:8081,ipv6only=0,fork,reuseaddr EXEC:cat & " +
	"socat UDP4-RECVFROM:8082,fork,reuseaddr EXEC:cat & " +
	"socat UDP4-RECVFROM:8083,fork,reuseaddr EXEC:cat & wait"

type environmentDockerFixture struct {
	owner, image, protected, outside, prefix, gateway, probe string
	containers, networks                                     []string
}

func newEnvironmentDockerFixture(t *testing.T) *environmentDockerFixture {
	t.Helper()
	f := &environmentDockerFixture{owner: "peer-egress-" + strings.ToLower(rand.Text()[:12]), image: "floci/network-helper:local"}
	if image := os.Getenv("FLOCI_TEST_NETWORK_HELPER_IMAGE"); image != "" {
		f.image = image
	}
	t.Cleanup(func() { f.cleanup(t) })
	if output := f.must(t, "info", "--format", "{{.OSType}}"); output != "linux" {
		t.Fatal("fixture requires rootful Linux Docker", output)
	}
	for _, image := range []string{guestImage, f.image} {
		f.must(t, "image", "inspect", image, "--format", "{{.Id}}")
	}
	var random [6]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	unique := hex.EncodeToString(random[:])
	ipv6 := "fd" + unique[:2] + ":" + unique[2:6] + ":" + unique[6:10]
	for _, network := range []struct{ name, suffix, ipv4 string }{
		{"protected", ":1::/64", ""}, {"outside", ":2::/64", fmt.Sprintf("198.18.%d.0/24", random[5])},
	} {
		args := []string{"network", "create", "--label", "io.floci.peer-firewall-test=" + f.owner,
			"--ipv6", "--subnet", ipv6 + network.suffix}
		if network.ipv4 != "" {
			args = append(args, "--subnet", network.ipv4)
		}
		id := f.must(t, append(args, f.owner+"-"+network.name)...)
		f.networks = append(f.networks, id)
	}
	f.protected, f.outside = f.networks[0], f.networks[1]
	var inspected []environmentDockerNetwork
	if err := json.Unmarshal([]byte(f.must(t, "network", "inspect", f.protected)), &inspected); err != nil || len(inspected) != 1 {
		t.Fatal("cannot inspect fixture network", err)
	}
	for _, allocation := range inspected[0].IPAM.Config {
		if !strings.Contains(allocation.Subnet, ":") {
			f.prefix, f.gateway = allocation.Subnet, allocation.Gateway
		}
	}
	if f.prefix == "" || f.gateway == "" {
		t.Fatal("fixture network has no IPv4 allocation")
	}
	return f
}

func (f *environmentDockerFixture) container(t *testing.T, suffix string, args ...string) string {
	t.Helper()
	command := []string{"create", "--name", f.owner + "-" + suffix, "--label", "io.floci.peer-firewall-test=" + f.owner}
	id := f.must(t, append(command, args...)...)
	f.containers = append(f.containers, id)
	f.must(t, "start", id)
	return id
}

func (f *environmentDockerFixture) addresses(t *testing.T, container string) (string, string) {
	t.Helper()
	output := f.must(t, "inspect", "--format", "{{range .NetworkSettings.Networks}}{{.IPAddress}} {{.GlobalIPv6Address}}{{end}}", container)
	fields := strings.Fields(output)
	if len(fields) != 2 {
		t.Fatal("fixture lacks dual-stack addresses", output)
	}
	return fields[0], fields[1]
}

func (f *environmentDockerFixture) listenerPort(t *testing.T, container, name string) int {
	t.Helper()
	output := f.must(t, "exec", container, "cat", "/tmp/"+name+".log")
	for line := range strings.SplitSeq(output, "\n") {
		if strings.Contains(line, "listening on") || strings.Contains(line, "bound to") {
			port, err := strconv.Atoi(strings.TrimSpace(line[strings.LastIndexByte(line, ':')+1:]))
			if err == nil && port > 0 && port <= 65535 {
				return port
			}
		}
	}
	t.Fatal("fixture listener did not bind", output)
	return 0
}

func (f *environmentDockerFixture) expect(t *testing.T, container string, forwarded bool, address, protocol string, port int, allowed bool, name string) {
	t.Helper()
	args := []string{"exec", container}
	if forwarded {
		args = append(args, "ip", "netns", "exec", "app")
	}
	transport := "TCP4:"
	if protocol == "udp" {
		transport = "UDP4-DATAGRAM:"
	} else if strings.Contains(address, ":") {
		transport, address = "TCP6:", "["+address+"]"
	}
	destination := transport + address + ":" + strconv.Itoa(port)
	if protocol == "tcp" {
		destination += ",connect-timeout=1"
	}
	args = append(args, "sh", "-ec", `printf probe | socat -T1 - "$1"`, "sh", destination)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	output, err := environmentDockerCommand(ctx, args...)
	reached := strings.Contains(output, "probe") && err == nil
	if reached != allowed {
		t.Errorf("%s forwarded=%v allowed=%v: output=%q error=%v", name, forwarded, allowed, output, err)
	}
}

func (f *environmentDockerFixture) must(t *testing.T, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	output, err := environmentDockerCommand(ctx, args...)
	if err != nil {
		t.Fatalf("docker %v: %v\n%s", args, err, output)
	}
	return strings.TrimSpace(output)
}

func environmentDockerCommand(ctx context.Context, args ...string) (string, error) {
	output, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	return string(output), err
}

func (f *environmentDockerFixture) diagnostics(t *testing.T, peer, chain string) {
	t.Helper()
	for _, binary := range []string{"iptables", "ip6tables"} {
		t.Log(f.must(t, "exec", peer, binary, "-t", "mangle", "-S", chain))
		t.Log(f.must(t, "exec", peer, binary, "-t", "mangle", "-S", "POSTROUTING"))
	}
}

func (f *environmentDockerFixture) cleanup(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 60*time.Second)
	defer cancel()
	if f.probe != "" {
		if output, err := environmentDockerCommand(ctx, "exec", f.probe, "sh", "-ec",
			"if ip netns list | grep -q '^app'; then ip netns del app; fi"); err != nil {
			t.Error("remove owned application namespace", err, output)
		}
	}
	for i := len(f.containers) - 1; i >= 0; i-- {
		if output, err := environmentDockerCommand(ctx, "rm", "-f", f.containers[i]); err != nil {
			t.Error("remove owned fixture container", err, output)
		}
	}
	for _, network := range f.networks {
		if output, err := environmentDockerCommand(ctx, "network", "rm", network); err != nil {
			t.Error("remove owned fixture network", err, output)
		}
	}
	for _, args := range [][]string{{"ps", "-aq"}, {"network", "ls", "-q"}} {
		output, err := environmentDockerCommand(ctx, append(args, "--filter", "label=io.floci.peer-firewall-test="+f.owner)...)
		if err != nil || strings.TrimSpace(output) != "" {
			t.Error("owned fixture resources remain", args, err, output)
		}
	}
}
