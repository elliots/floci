package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	msbruntime "github.com/floci-io/floci/tools/microsandbox-runtime"
)

// One boundary is shared by all Pods using this peer. The SDK protects direct
// guest traffic; the peer protects traffic after Kubernetes Service DNAT.
type environmentBoundary struct {
	runner   commandRunner
	policy   msbruntime.NetworkBoundaryInfo
	identity environmentIdentity
	mu       sync.Mutex
	ports    map[environmentPort]bool
	removed  map[environmentPort]bool
	dirty    bool
	healthy  atomic.Bool
}

type environmentPort struct {
	Protocol string `json:"protocol"`
	Port     int    `json:"port"`
}

type environmentIdentity struct {
	Version     int      `json:"version"`
	PeerID      string   `json:"peerID"`
	PeerIP      string   `json:"peerIP"`
	PeerPort    int      `json:"peerPort"`
	HostIP      string   `json:"hostIP"`
	NetworkID   string   `json:"networkID"`
	HelperID    string   `json:"helperID"`
	Owner       string   `json:"owner"`
	Table       string   `json:"table"`
	Fingerprint string   `json:"fingerprint"`
	DNS         string   `json:"dns"`
	Bridge      string   `json:"bridge"`
	Prefixes    []string `json:"prefixes"`
	Gateways    []string `json:"gateways"`
}

type environmentRecord struct {
	Identity environmentIdentity `json:"identity"`
	Ports    []environmentPort   `json:"ports,omitempty"`
	Removed  []environmentPort   `json:"removed,omitempty"`
}

type environmentContract struct {
	transports  []environmentNFTTransport
	Version     int    `json:"version"`
	Owner       string `json:"owner"`
	Table       string `json:"table"`
	Fingerprint string `json:"fingerprint"`
	Bridges     []struct {
		Name      string `json:"name"`
		NetworkID string `json:"networkId"`
		Bridge    string `json:"bridge"`
	} `json:"bridges"`
}

type environmentContainer struct {
	ID     string `json:"Id"`
	Config struct {
		Labels map[string]string
		Env    []string
	}
	HostConfig struct {
		NetworkMode string
		DNS         []string `json:"Dns"`
	}
	State           struct{ Running bool }
	NetworkSettings struct {
		Ports    map[string][]struct{ HostIP, HostPort string }
		Networks map[string]struct {
			NetworkID string
			IPAddress string
		}
	}
}

type environmentDockerNetwork struct {
	ID           string `json:"Id"`
	Name, Driver string
	Options      map[string]string
	IPAM         struct {
		Config []struct{ Subnet, Gateway string }
	}
	Containers map[string]struct{ IPv4Address string }
}

func (c config) validateEnvironment() error {
	if c.environmentNetwork == "" && c.isolationHelper == "" && c.environmentDNS == "" {
		return nil
	}
	dns, err := netip.ParseAddr(c.environmentDNS)
	if c.environmentNetwork == "" || c.isolationHelper == "" || err != nil || !dns.Is4() || dns.IsUnspecified() || dns.IsLoopback() || dns.IsMulticast() {
		return errors.New("environment-network, isolation-helper and a unicast environment-dns IPv4 address are required together")
	}
	if !c.networkPolicy {
		return errors.New("environment isolation requires --network-policy=true and safe capabilities on every native Pod")
	}
	return nil
}

func environmentInspect[T any](ctx context.Context, runner commandRunner, kind, id string) (T, error) {
	var zero T
	args := []string{"inspect", id}
	if kind != "container" {
		args = []string{kind, "inspect", id}
	}
	output, err := runner.run(ctx, "docker", args...)
	if err != nil {
		return zero, err
	}
	var values []T
	if err := json.Unmarshal([]byte(output), &values); err != nil || len(values) != 1 {
		return zero, errors.New("invalid Docker environment inventory")
	}
	return values[0], nil
}

func inspectEnvironment(ctx context.Context, runner commandRunner, policy msbruntime.NetworkBoundaryInfo) (environmentIdentity, environmentContract, error) {
	var identity environmentIdentity
	var contract environmentContract
	peer, err := environmentInspect[environmentContainer](ctx, runner, "container", runner.cfg.peer)
	if err != nil {
		return identity, contract, err
	}
	helper, err := environmentInspect[environmentContainer](ctx, runner, "container", runner.cfg.isolationHelper)
	if err != nil {
		return identity, contract, err
	}
	output, err := runner.run(ctx, "docker", "exec", helper.ID, "cat", "/etc/floci/network-policy.json")
	if err != nil {
		return identity, contract, err
	}
	if err := json.Unmarshal([]byte(output), &contract); err != nil {
		return identity, contract, errors.New("invalid Floci isolation helper contract")
	}
	if contract.Version != 1 || contract.Owner == "" || !environmentTableName.MatchString(contract.Table) ||
		contract.Fingerprint != policy.Fingerprint || len(contract.Fingerprint) != 64 || len(contract.Bridges) == 0 ||
		helper.Config.Labels["floci.network.owner"] != contract.Owner || helper.Config.Labels["floci.network.table"] != contract.Table ||
		!helper.State.Running || helper.HostConfig.NetworkMode != "host" {
		return identity, contract, errors.New("Floci helper ownership, policy fingerprint or running state differs")
	}
	network, err := environmentInspect[environmentDockerNetwork](ctx, runner, "network", runner.cfg.environmentNetwork)
	if err != nil {
		return identity, contract, err
	}
	if peer.Config.Labels[ownerLabel] != runner.cfg.peerOwner || !peer.State.Running || len(peer.NetworkSettings.Networks) != 1 ||
		!slices.Equal(peer.HostConfig.DNS, []string{runner.cfg.environmentDNS}) ||
		!slices.Contains(peer.Config.Env, "K3S_RESOLV_CONF=/etc/floci-resolv.conf") {
		return identity, contract, errors.New("protected peer ownership, sole attachment or resolver configuration differs")
	}
	attached, ok := peer.NetworkSettings.Networks[network.Name]
	if !ok || attached.NetworkID != network.ID {
		return identity, contract, errors.New("peer is not attached exclusively to the protected environment")
	}
	peerIP, err := netip.ParseAddr(attached.IPAddress)
	if err != nil || !peerIP.Is4() || peerIP.IsUnspecified() {
		return identity, contract, errors.New("protected peer has no IPv4 address")
	}
	bindings := peer.NetworkSettings.Ports["8472/udp"]
	if len(bindings) != 1 {
		return identity, contract, errors.New("protected peer requires exactly one published UDP transport port")
	}
	peerPort, err := strconv.Atoi(bindings[0].HostPort)
	if err != nil || !environmentTransportAllowed(policy, peerPort) {
		return identity, contract, errors.New("network.native.allowed-egress must allow the protected peer's published UDP port on host")
	}
	dnsMember := false
	for _, endpoint := range network.Containers {
		prefix, err := netip.ParsePrefix(endpoint.IPv4Address)
		if err == nil && prefix.Addr().String() == runner.cfg.environmentDNS {
			dnsMember = true
		}
	}
	if !dnsMember || runner.cfg.environmentDNS == peerIP.String() {
		return identity, contract, errors.New("environment DNS is not a separate endpoint on the protected network")
	}
	host, err := peerHostAddress(runner.cfg.nodeIP)
	if err != nil {
		return identity, contract, err
	}
	identity = environmentIdentity{Version: 1, PeerID: peer.ID, PeerIP: peerIP.String(), PeerPort: peerPort, HostIP: host, NetworkID: network.ID,
		HelperID: helper.ID, Owner: contract.Owner, Table: contract.Table, Fingerprint: contract.Fingerprint, DNS: runner.cfg.environmentDNS}
	seen := map[string]bool{}
	for _, protected := range contract.Bridges {
		if seen[protected.NetworkID] || !environmentBridgeName.MatchString(protected.Bridge) {
			return identity, contract, errors.New("ambiguous protected bridge contract")
		}
		seen[protected.NetworkID] = true
		current := network
		if protected.NetworkID != network.ID {
			current, err = environmentInspect[environmentDockerNetwork](ctx, runner, "network", protected.NetworkID)
			if err != nil {
				return identity, contract, err
			}
		}
		bridge := current.Options["com.docker.network.bridge.name"]
		if bridge == "" && len(current.ID) >= 12 {
			bridge = "br-" + current.ID[:12]
		}
		if current.ID != protected.NetworkID || current.Name != protected.Name || current.Driver != "bridge" || current.Name == "bridge" || bridge != protected.Bridge {
			return identity, contract, errors.New("protected bridge no longer matches its Docker identity")
		}
		if current.ID == network.ID {
			identity.Bridge = bridge
		}
		if current.ID != network.ID {
			continue
		}
		for _, allocation := range current.IPAM.Config {
			prefix, err := netip.ParsePrefix(allocation.Subnet)
			if err != nil {
				return identity, contract, errors.New("invalid protected bridge subnet")
			}
			if !prefix.Addr().Is4() {
				continue
			}
			if prefix.Bits() == 0 || prefix != prefix.Masked() {
				return identity, contract, errors.New("invalid protected IPv4 subnet")
			}
			for _, reserved := range []string{runner.cfg.clusterCIDR, runner.cfg.serviceCIDR} {
				cluster, err := netip.ParsePrefix(reserved)
				if err != nil || cluster.Overlaps(prefix) {
					return identity, contract, errors.New("protected Docker subnets overlap Kubernetes addresses")
				}
			}
			identity.Prefixes = append(identity.Prefixes, prefix.String())
			if allocation.Gateway != "" {
				gateway, err := netip.ParseAddr(allocation.Gateway)
				if err != nil || !prefix.Contains(gateway) {
					return identity, contract, errors.New("invalid protected bridge gateway")
				}
				identity.Gateways = append(identity.Gateways, gateway.String())
			}
		}
	}
	if identity.Bridge == "" || len(identity.Prefixes) == 0 {
		return identity, contract, errors.New("peer network has no protected IPv4 bridge")
	}
	slices.Sort(identity.Prefixes)
	identity.Prefixes = slices.Compact(identity.Prefixes)
	slices.Sort(identity.Gateways)
	identity.Gateways = slices.Compact(identity.Gateways)
	resolver, err := runner.peer(ctx, "cat /etc/floci-resolv.conf")
	if err != nil || !environmentResolverMatches(resolver, identity.DNS) {
		return identity, contract, errors.New("peer's staged DNS resolver differs")
	}
	output, err = runner.run(ctx, "docker", "exec", helper.ID, "nft", "-j", "list", "table", "inet", contract.Table)
	if err != nil {
		return identity, contract, err
	}
	bridges := make([]string, len(contract.Bridges))
	for i, bridge := range contract.Bridges {
		bridges[i] = bridge.Bridge
	}
	if err := verifyEnvironmentNFT([]byte(output), contract.Table, bridges, policy.GlobalEgress); err != nil {
		return identity, contract, err
	}
	contract.transports, err = environmentNFTTransports([]byte(output))
	if err != nil {
		return identity, contract, err
	}
	return identity, contract, nil
}

func environmentResolverMatches(resolver, address string) bool {
	nameservers := 0
	for line := range strings.SplitSeq(resolver, "\n") {
		line, _, _ = strings.Cut(line, "#")
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		switch fields[0] {
		case "nameserver":
			if len(fields) != 2 || fields[1] != address {
				return false
			}
			nameservers++
		case "options", "search", "domain", "sortlist":
			// Resolver timing and search settings do not authorize a new server.
		default:
			return false
		}
	}
	return nameservers == 1
}

func newEnvironmentBoundary(ctx context.Context, runner commandRunner, policy msbruntime.NetworkBoundaryInfo) (*environmentBoundary, error) {
	identity, _, err := inspectEnvironment(ctx, runner, policy)
	if err != nil {
		return nil, err
	}
	e := &environmentBoundary{runner: runner, policy: policy, identity: identity, ports: map[environmentPort]bool{}, removed: map[environmentPort]bool{}}
	old, err := readNetworkIdentityFile(filepath.Join(runner.cfg.state, "network-environment"))
	if err == nil {
		var record environmentRecord
		if json.Unmarshal(old, &record) != nil || !reflect.DeepEqual(record.Identity, identity) {
			return nil, errors.New("stored environment identity differs; recreate the managed cluster before changing its isolation boundary")
		}
		for _, port := range append(record.Ports, record.Removed...) {
			if !validEnvironmentPort(port) {
				return nil, errors.New("invalid stored environment transport grant")
			}
			e.ports[port] = true
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	// On recovery, close old allowances before reopening only journaled listeners.
	// The peer's independent default-deny rule closes the crash window if the
	// outer helper disappears while stale tuple entries are being removed.
	oldPorts := e.portList()
	clear(e.ports)
	if err := e.installPeer(ctx); err != nil {
		return nil, err
	}
	if err := e.outerPorts(ctx, oldPorts, false); err != nil {
		return nil, err
	}
	if err := e.save(); err != nil {
		return nil, err
	}
	_, port, err := net.SplitHostPort(runner.cfg.listen)
	if err != nil {
		return nil, err
	}
	number, err := strconv.Atoi(port)
	if err != nil {
		return nil, err
	}
	if err := e.changePorts(ctx, []environmentPort{{Protocol: "tcp", Port: number}}, true); err != nil {
		return nil, err
	}
	e.healthy.Store(true)
	return e, nil
}

func validEnvironmentPort(port environmentPort) bool {
	return (port.Protocol == "tcp" || port.Protocol == "udp") && port.Port > 0 && port.Port <= 65535
}

func (e *environmentBoundary) portList() []environmentPort {
	result := make([]environmentPort, 0, len(e.ports))
	for port := range e.ports {
		result = append(result, port)
	}
	slices.SortFunc(result, func(a, b environmentPort) int {
		if a.Protocol != b.Protocol {
			return strings.Compare(a.Protocol, b.Protocol)
		}
		return a.Port - b.Port
	})
	return result
}

func (e *environmentBoundary) save() error {
	file, err := os.CreateTemp(e.runner.cfg.state, ".network-environment-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	if err := json.NewEncoder(file).Encode(environmentRecord{Identity: e.identity, Ports: e.portList(), Removed: e.removedPorts()}); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), filepath.Join(e.runner.cfg.state, "network-environment")); err != nil {
		return err
	}
	return syncDirectory(e.runner.cfg.state)
}

func (e *environmentBoundary) verify(ctx context.Context) error {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	identity, contract, err := inspectEnvironment(ctx, e.runner, e.policy)
	if err == nil && !reflect.DeepEqual(identity, e.identity) {
		err = errors.New("protected environment identity changed")
	}
	if err == nil && e.dirty {
		err = e.commitPorts(ctx)
		if err == nil {
			identity, contract, err = inspectEnvironment(ctx, e.runner, e.policy)
		}
		if err == nil && !reflect.DeepEqual(identity, e.identity) {
			err = errors.New("protected environment identity changed during transport recovery")
		}
	}
	if err == nil {
		err = e.verifyTransports(contract.transports)
	}
	if err == nil {
		err = e.verifyPeer(ctx)
	}
	e.healthy.Store(err == nil)
	return err
}

func (e *environmentBoundary) verifyTransports(transports []environmentNFTTransport) error {
	observed := map[environmentPort]bool{}
	for _, transport := range transports {
		if transport.Source != e.identity.PeerIP {
			continue
		}
		port := environmentPort{Protocol: transport.Protocol, Port: transport.Port}
		if transport.Destination != e.identity.HostIP || !e.ports[port] {
			return errors.New("unowned transport grant exists for the protected peer")
		}
		observed[port] = true
	}
	if len(observed) != len(e.ports) {
		return errors.New("protected peer transport grant is missing")
	}
	return nil
}

func (e *environmentBoundary) outerPorts(ctx context.Context, ports []environmentPort, add bool) error {
	if len(ports) == 0 {
		return nil
	}
	var rules strings.Builder
	action := "destroy"
	if add {
		action = "add"
	}
	for _, port := range ports {
		if !validEnvironmentPort(port) {
			return errors.New("invalid environment transport grant")
		}
		fmt.Fprintf(&rules, "%s element inet %s native_%s { %s . %s . %d }\n", action, e.identity.Table, port.Protocol, e.identity.PeerIP, e.identity.HostIP, port.Port)
	}
	command := e.runner.command(ctx, "docker", "exec", "-i", e.identity.HelperID, "nft", "-f", "-")
	command.Stdin = strings.NewReader(rules.String())
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("update owned environment transport grants: %w (%s)", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func (e *environmentBoundary) changePorts(ctx context.Context, ports []environmentPort, add bool) error {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	var changed []environmentPort
	for _, port := range ports {
		if !validEnvironmentPort(port) {
			return errors.New("invalid environment transport grant")
		}
		if e.ports[port] != add {
			changed = append(changed, port)
		}
	}
	if len(changed) == 0 && !e.dirty {
		if !e.healthy.Load() {
			return errors.New("protected environment network is unavailable")
		}
		return nil
	}
	if e.removed == nil {
		e.removed = map[environmentPort]bool{}
	}
	for _, port := range changed {
		if add {
			e.ports[port] = true
			delete(e.removed, port)
		} else {
			delete(e.ports, port)
			e.removed[port] = true
		}
	}
	e.dirty = true
	if err := e.commitPorts(ctx); err != nil {
		e.healthy.Store(false)
		return err
	}
	return nil
}

func (e *environmentBoundary) removedPorts() []environmentPort {
	result := make([]environmentPort, 0, len(e.removed))
	for port := range e.removed {
		result = append(result, port)
	}
	slices.SortFunc(result, func(a, b environmentPort) int {
		if a.Protocol != b.Protocol {
			return strings.Compare(a.Protocol, b.Protocol)
		}
		return a.Port - b.Port
	})
	return result
}

// A failed transaction remains dirty and is retried only after live identity
// verification. Both additions and pending removals survive a controller crash.
func (e *environmentBoundary) commitPorts(ctx context.Context) error {
	if err := e.save(); err != nil {
		return err
	}
	if err := e.installPeer(ctx); err != nil {
		return err
	}
	if err := e.outerPorts(ctx, e.portList(), true); err != nil {
		return err
	}
	if err := e.outerPorts(ctx, e.removedPorts(), false); err != nil {
		return err
	}
	clear(e.removed)
	if err := e.save(); err != nil {
		return err
	}
	e.dirty = false
	return nil
}

func environmentTransportAllowed(policy msbruntime.NetworkBoundaryInfo, port int) bool {
	if port < 1 || port > 65535 {
		return false
	}
	for _, endpoint := range policy.AllowedEgress {
		if endpoint.Destination != "host" || endpoint.Protocol != "udp" {
			continue
		}
		low, high, ranged := strings.Cut(endpoint.Ports, "-")
		first, err := strconv.Atoi(low)
		if err != nil {
			continue
		}
		last := first
		if ranged {
			last, err = strconv.Atoi(high)
			if err != nil {
				continue
			}
		}
		if port >= first && port <= last {
			return true
		}
	}
	return false
}
