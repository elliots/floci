package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	msbruntime "github.com/floci-io/floci/tools/microsandbox-runtime"
)

func TestEnvironmentRecoveryFailedGrantPersistsIntentAndVerifiedRetryConverges(t *testing.T) {
	f, boundary := newEnvironmentRecoveryFixture(t)
	added := environmentPort{Protocol: "udp", Port: 47001}
	f.change(t, func(state *environmentRecoveryDocker) { state.Fail = "grant" })
	if err := boundary.changePorts(t.Context(), []environmentPort{added}, true); err == nil {
		t.Fatal("injected outer grant failure was ignored")
	}
	if !boundary.dirty || boundary.healthy.Load() {
		t.Fatal("interrupted transaction did not fence availability")
	}
	record := f.record(t)
	if !slices.Equal(record.Ports, []environmentPort{f.kubelet, added}) || len(record.Removed) != 0 {
		t.Fatalf("addition intent was not durable: %#v", record)
	}
	state := f.read(t)
	if !strings.Contains(state.Peer4, "--dport 47001") || slices.Contains(state.Transports, f.transport(added)) {
		t.Fatal("fixture did not fail after peer install and before outer grant")
	}
	f.assertIntentBeforeMutation(t, added, false)

	// A retry must inspect live identity before dispatching the saved transaction.
	f.change(t, func(state *environmentRecoveryDocker) {
		state.Fail = ""
		state.Responses["contract"] = strings.ReplaceAll(state.Responses["contract"], f.identity.Fingerprint, strings.Repeat("b", 64))
		state.Calls = nil
	})
	if err := boundary.verify(t.Context()); err == nil || boundary.healthy.Load() {
		t.Fatal("dirty recovery accepted a changed live environment")
	}
	for _, call := range f.read(t).Calls {
		if call.Intent != nil {
			t.Fatal("recovery mutated a boundary before validating its identity", call.Operation)
		}
	}
	f.change(t, func(state *environmentRecoveryDocker) {
		state.Responses["contract"] = strings.ReplaceAll(state.Responses["contract"], strings.Repeat("b", 64), f.identity.Fingerprint)
		state.Calls = nil
	})
	if err := boundary.verify(t.Context()); err != nil {
		t.Fatal("verified retry did not converge", err)
	}
	if boundary.dirty || !boundary.healthy.Load() {
		t.Fatal("successful verified retry did not restore availability")
	}
	f.assertTransports(t, f.kubelet, added)
	if !slices.Equal(f.record(t).Ports, []environmentPort{f.kubelet, added}) {
		t.Fatal("completed intent differs from installed transports")
	}
	if calls := f.read(t).Calls; len(calls) == 0 || calls[0].Operation != "inspect-peer" {
		t.Fatal("retry did not begin with live peer inspection", calls)
	}
}

func TestEnvironmentRecoveryFailedRemovalSurvivesRepeatedControllerRestart(t *testing.T) {
	f, boundary := newEnvironmentRecoveryFixture(t)
	removed := environmentPort{Protocol: "udp", Port: 47002}
	if err := boundary.changePorts(t.Context(), []environmentPort{removed}, true); err != nil {
		t.Fatal(err)
	}
	f.change(t, func(state *environmentRecoveryDocker) { state.Fail, state.Calls = "remove", nil })
	if err := boundary.changePorts(t.Context(), []environmentPort{removed}, false); err == nil {
		t.Fatal("injected outer removal failure was ignored")
	}
	if !boundary.dirty || boundary.healthy.Load() || !boundary.removed[removed] {
		t.Fatal("failed removal lost pending intent or remained available")
	}
	record := f.record(t)
	if !slices.Equal(record.Ports, []environmentPort{f.kubelet}) || !slices.Equal(record.Removed, []environmentPort{removed}) {
		t.Fatalf("pending removal not recorded: %#v", record)
	}
	state := f.read(t)
	if strings.Contains(state.Peer4, "--dport 47002") || !slices.Contains(state.Transports, f.transport(removed)) {
		t.Fatal("fixture did not close the peer before failing outer removal")
	}
	f.assertIntentBeforeMutation(t, removed, true)

	if restarted, err := newEnvironmentBoundary(t.Context(), f.runner, f.policy); err == nil || restarted != nil {
		t.Fatal("restart ignored an unreconciled old grant", err)
	}
	if after := f.record(t); !reflect.DeepEqual(record, after) {
		t.Fatal("failed restart overwrote durable pending removal", after)
	}
	if rules := f.read(t).Peer4; strings.Contains(rules, "--dport 47002") || strings.Contains(rules, "--dport 10250") {
		t.Fatal("recovery reopened a listener before stale outer grants were removed")
	}
	f.change(t, func(state *environmentRecoveryDocker) { state.Fail = "" })
	restarted, err := newEnvironmentBoundary(t.Context(), f.runner, f.policy)
	if err != nil {
		t.Fatal("controller restart could not finish pending removal", err)
	}
	if err := restarted.verify(t.Context()); err != nil || !restarted.healthy.Load() || restarted.dirty {
		t.Fatal("recovered controller did not verify its final boundary", err)
	}
	f.assertTransports(t, f.kubelet)
	if record := f.record(t); !slices.Equal(record.Ports, []environmentPort{f.kubelet}) || len(record.Removed) != 0 {
		t.Fatal("recovery resurrected a removed listener", record)
	}
}

func TestEnvironmentRecoveryMissingLiveTupleCannotReportHealthy(t *testing.T) {
	f, boundary := newEnvironmentRecoveryFixture(t)
	f.change(t, func(state *environmentRecoveryDocker) {
		state.Transports = slices.DeleteFunc(state.Transports, func(row environmentNFTTransport) bool {
			return row == f.transport(f.kubelet)
		})
		state.Calls = nil
	})
	if err := boundary.verify(t.Context()); err == nil || boundary.healthy.Load() {
		t.Fatal("missing live tuple was accepted as a healthy environment")
	}
	if err := boundary.changePorts(t.Context(), []environmentPort{f.kubelet}, true); err == nil {
		t.Fatal("an unchanged request silently healed a missing external grant")
	}
	for _, call := range f.read(t).Calls {
		if call.Intent != nil {
			t.Fatal("live tuple verification replayed a completed transaction", call.Operation)
		}
	}
	f.change(t, func(state *environmentRecoveryDocker) {
		state.Transports = append(state.Transports, f.transport(f.kubelet))
	})
	if err := boundary.verify(t.Context()); err != nil || !boundary.healthy.Load() {
		t.Fatal("restored exact tuple did not pass verification", err)
	}
}

func TestEnvironmentRecoveryUnchangedDirtyRequestVerifiesIdentityBeforeRetry(t *testing.T) {
	f, boundary := newEnvironmentRecoveryFixture(t)
	added := environmentPort{Protocol: "tcp", Port: 47003}
	f.change(t, func(state *environmentRecoveryDocker) { state.Fail = "grant" })
	if err := boundary.changePorts(t.Context(), []environmentPort{added}, true); err == nil {
		t.Fatal("fixture did not interrupt the first grant")
	}
	before := f.record(t)
	f.change(t, func(state *environmentRecoveryDocker) {
		state.Fail, state.Calls = "", nil
		state.Responses["contract"] = strings.ReplaceAll(state.Responses["contract"], f.identity.Fingerprint, strings.Repeat("b", 64))
	})
	if err := boundary.changePorts(t.Context(), []environmentPort{added}, true); err == nil {
		t.Fatal("unchanged dirty request mutated a changed live environment")
	}
	if !boundary.dirty || boundary.healthy.Load() || !reflect.DeepEqual(before, f.record(t)) {
		t.Fatal("rejected retry changed the pending transaction or availability")
	}
	for _, call := range f.read(t).Calls {
		if call.Intent != nil {
			t.Fatal("dirty request dispatched a mutation before identity verification", call.Operation)
		}
	}
	f.change(t, func(state *environmentRecoveryDocker) {
		state.Responses["contract"] = strings.ReplaceAll(state.Responses["contract"], strings.Repeat("b", 64), f.identity.Fingerprint)
		state.Calls = nil
	})
	if err := boundary.changePorts(t.Context(), []environmentPort{added}, true); err != nil {
		t.Fatal("unchanged dirty request did not recover after restoring identity", err)
	}
	if boundary.dirty {
		t.Fatal("successful identity-checked transaction stayed dirty")
	}
	if calls := f.read(t).Calls; len(calls) == 0 || calls[0].Operation != "inspect-peer" {
		t.Fatal("dirty request did not inspect identity before replay", calls)
	}
	if err := boundary.verify(t.Context()); err != nil || !boundary.healthy.Load() {
		t.Fatal("reconciled exact grants did not verify", err)
	}
	f.assertTransports(t, f.kubelet, added)
}

type environmentRecoveryFixture struct {
	file     string
	runner   commandRunner
	policy   msbruntime.NetworkBoundaryInfo
	identity environmentIdentity
	kubelet  environmentPort
}

type environmentRecoveryCall struct {
	Operation string
	Intent    *environmentRecord
}

type environmentRecoveryDocker struct {
	StateDirectory string
	Responses      map[string]string
	Transports     []environmentNFTTransport
	Peer4, Peer6   string
	Fail           string
	Calls          []environmentRecoveryCall
}

func newEnvironmentRecoveryFixture(t *testing.T) (*environmentRecoveryFixture, *environmentBoundary) {
	t.Helper()
	directory := t.TempDir()
	identity := environmentIdentity{Version: 1, PeerID: "peer-fixture", PeerIP: "172.28.0.3", PeerPort: 45670,
		HostIP: "192.168.0.10", NetworkID: "network-fixture", HelperID: "helper-fixture", Owner: "environment/4566",
		Table: "floci_iso_sample", Fingerprint: strings.Repeat("a", 64), DNS: "172.28.0.2", Bridge: "br-one",
		Prefixes: []string{"172.28.0.0/16"}, Gateways: []string{"172.28.0.1"}}
	f := &environmentRecoveryFixture{file: filepath.Join(directory, "docker-state"), identity: identity,
		kubelet: environmentPort{Protocol: "tcp", Port: 10250},
		runner: commandRunner{cfg: config{state: directory, home: directory, peer: identity.PeerID, peerOwner: "owned-peer",
			environmentNetwork: identity.NetworkID, isolationHelper: identity.HelperID, environmentDNS: identity.DNS,
			nodeIP: identity.HostIP, listen: "0.0.0.0:10250", clusterCIDR: "10.42.0.0/16", serviceCIDR: "10.43.0.0/16"}},
		policy: msbruntime.NetworkBoundaryInfo{Fingerprint: identity.Fingerprint, GlobalEgress: environmentNFTGlobals(),
			AllowedEgress: []msbruntime.NetworkEgressEndpoint{{Destination: "host", Protocol: "udp", Ports: "45670"}}}}
	encode := func(value any) string {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	state := environmentRecoveryDocker{StateDirectory: directory, Responses: map[string]string{
		"inspect-peer": encode([]any{map[string]any{"Id": identity.PeerID, "State": map[string]any{"Running": true},
			"Config":     map[string]any{"Labels": map[string]string{ownerLabel: "owned-peer"}, "Env": []string{"K3S_RESOLV_CONF=/etc/floci-resolv.conf"}},
			"HostConfig": map[string]any{"Dns": []string{identity.DNS}}, "NetworkSettings": map[string]any{
				"Networks": map[string]any{"protected": map[string]any{"NetworkID": identity.NetworkID, "IPAddress": identity.PeerIP}},
				"Ports":    map[string]any{"8472/udp": []any{map[string]string{"HostIP": "0.0.0.0", "HostPort": "45670"}}}}}}),
		"inspect-helper": encode([]any{map[string]any{"Id": identity.HelperID, "State": map[string]any{"Running": true},
			"Config":     map[string]any{"Labels": map[string]string{"floci.network.owner": identity.Owner, "floci.network.table": identity.Table}},
			"HostConfig": map[string]any{"NetworkMode": "host"}}}),
		"inspect-network": encode([]any{map[string]any{"Id": identity.NetworkID, "Name": "protected", "Driver": "bridge",
			"Options":    map[string]string{"com.docker.network.bridge.name": identity.Bridge},
			"IPAM":       map[string]any{"Config": []any{map[string]string{"Subnet": identity.Prefixes[0], "Gateway": identity.Gateways[0]}}},
			"Containers": map[string]any{"dns": map[string]string{"IPv4Address": identity.DNS + "/16"}}}}),
		"contract": encode(map[string]any{"version": 1, "owner": identity.Owner, "table": identity.Table, "fingerprint": identity.Fingerprint,
			"bridges": []any{map[string]string{"name": "protected", "networkId": identity.NetworkID, "bridge": identity.Bridge}}}),
		"resolver": "nameserver " + identity.DNS + "\noptions ndots:0\n",
	}, Transports: []environmentNFTTransport{{Source: "172.28.0.99", Destination: identity.HostIP, Protocol: "tcp", Port: 22000}}}
	f.write(t, state)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	shim := "#!/bin/sh\nexec " + quote(executable) + " -test.run '^TestEnvironmentRecoveryDockerProcess$' -- \"$@\"\n"
	if err := os.WriteFile(filepath.Join(directory, "docker"), []byte(shim), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("FLOCI_ENVIRONMENT_RECOVERY_HELPER", f.file)
	t.Setenv("GORACE", "atexit_sleep_ms=0")
	boundary, err := newEnvironmentBoundary(t.Context(), f.runner, f.policy)
	if err != nil {
		t.Fatal("fixture boundary did not initialize", err)
	}
	if err := boundary.verify(t.Context()); err != nil {
		t.Fatal("fixture boundary did not verify", err)
	}
	f.change(t, func(state *environmentRecoveryDocker) { state.Calls = nil })
	return f, boundary
}

func (f *environmentRecoveryFixture) transport(port environmentPort) environmentNFTTransport {
	return environmentNFTTransport{Source: f.identity.PeerIP, Destination: f.identity.HostIP, Protocol: port.Protocol, Port: port.Port}
}

func (f *environmentRecoveryFixture) read(t *testing.T) environmentRecoveryDocker {
	t.Helper()
	data, err := os.ReadFile(f.file)
	if err != nil {
		t.Fatal(err)
	}
	var state environmentRecoveryDocker
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatal(err)
	}
	return state
}

func (f *environmentRecoveryFixture) write(t *testing.T, state environmentRecoveryDocker) {
	t.Helper()
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f.file, data, 0600); err != nil {
		t.Fatal(err)
	}
}

func (f *environmentRecoveryFixture) change(t *testing.T, change func(*environmentRecoveryDocker)) {
	t.Helper()
	state := f.read(t)
	change(&state)
	f.write(t, state)
}

func (f *environmentRecoveryFixture) record(t *testing.T) environmentRecord {
	t.Helper()
	path := filepath.Join(f.runner.cfg.state, "network-environment")
	data, err := readNetworkIdentityFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var record environmentRecord
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0600 {
		t.Fatal("durable transport intent is not private", info, err)
	}
	return record
}

func (f *environmentRecoveryFixture) assertIntentBeforeMutation(t *testing.T, port environmentPort, removing bool) {
	t.Helper()
	seen := map[string]bool{}
	for _, call := range f.read(t).Calls {
		if call.Intent == nil {
			continue
		}
		seen[call.Operation] = true
		if removing != slices.Contains(call.Intent.Removed, port) || removing == slices.Contains(call.Intent.Ports, port) {
			t.Fatal("external mutation preceded durable transaction intent", call)
		}
	}
	operation := "grant"
	if removing {
		operation = "remove"
	}
	if !seen["peer4"] || !seen["peer6"] || !seen[operation] {
		t.Fatal("failure did not reach the intended transaction boundary", seen)
	}
}

func (f *environmentRecoveryFixture) assertTransports(t *testing.T, ports ...environmentPort) {
	t.Helper()
	want := []environmentNFTTransport{{Source: "172.28.0.99", Destination: f.identity.HostIP, Protocol: "tcp", Port: 22000}}
	for _, port := range ports {
		want = append(want, f.transport(port))
	}
	actual := f.read(t).Transports
	if len(actual) != len(want) {
		t.Fatalf("transport inventory differs: got %#v want %#v", actual, want)
	}
	for _, tuple := range want {
		if !slices.Contains(actual, tuple) {
			t.Fatal("reconciliation changed or dropped an owned/foreign tuple", tuple, actual)
		}
	}
}

// The private PATH shim re-executes this test binary; it never invokes Docker or
// touches any system firewall. Each mutation records the journal it observed.
func TestEnvironmentRecoveryDockerProcess(t *testing.T) {
	file := os.Getenv("FLOCI_ENVIRONMENT_RECOVERY_HELPER")
	separator := slices.Index(os.Args, "--")
	if file == "" || separator < 0 {
		return
	}
	f := &environmentRecoveryFixture{file: file}
	state := f.read(t)
	output, err := state.dispatch(t, os.Args[separator+1:])
	f.write(t, state)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(17)
	}
	fmt.Print(output)
	os.Exit(0)
}

func (state *environmentRecoveryDocker) dispatch(t *testing.T, args []string) (string, error) {
	operation := ""
	switch {
	case slices.Equal(args, []string{"inspect", "peer-fixture"}):
		operation = "inspect-peer"
	case slices.Equal(args, []string{"inspect", "helper-fixture"}):
		operation = "inspect-helper"
	case slices.Equal(args, []string{"network", "inspect", "network-fixture"}):
		operation = "inspect-network"
	case slices.Equal(args, []string{"exec", "helper-fixture", "cat", "/etc/floci/network-policy.json"}):
		operation = "contract"
	case slices.Equal(args, []string{"exec", "peer-fixture", "sh", "-ec", "cat /etc/floci-resolv.conf"}):
		operation = "resolver"
	case slices.Equal(args, []string{"exec", "helper-fixture", "nft", "-j", "list", "table", "inet", "floci_iso_sample"}):
		state.Calls = append(state.Calls, environmentRecoveryCall{Operation: "read-nft"})
		return string(recoveryNFT(t, state.Transports)), nil
	case len(args) == 6 && args[0] == "exec" && args[1] == "-i" && args[2] == "helper-fixture" && args[3] == "nft":
		return state.mutate(t, "outer")
	case len(args) == 6 && args[0] == "exec" && args[1] == "-i" && args[2] == "peer-fixture" && args[3] == "sh":
		if strings.HasPrefix(args[5], "ip6tables-restore") {
			return state.mutate(t, "peer6")
		}
		return state.mutate(t, "peer4")
	case len(args) == 5 && args[0] == "exec" && args[1] == "peer-fixture" && args[2] == "sh":
		if strings.HasPrefix(args[4], "ip6tables -w 5") {
			return state.Peer6, nil
		}
		if strings.HasPrefix(args[4], "iptables -w 5") {
			return state.Peer4, nil
		}
	}
	if output, ok := state.Responses[operation]; ok {
		state.Calls = append(state.Calls, environmentRecoveryCall{Operation: operation})
		return output, nil
	}
	return "", fmt.Errorf("unexpected fake Docker command %q", args)
}

func (state *environmentRecoveryDocker) mutate(t *testing.T, operation string) (string, error) {
	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		return "", err
	}
	if operation == "outer" {
		operation = "grant"
		if strings.HasPrefix(string(input), "destroy ") {
			operation = "remove"
		}
	}
	data, err := os.ReadFile(filepath.Join(state.StateDirectory, "network-environment"))
	var intent *environmentRecord
	if err == nil {
		intent = new(environmentRecord)
		if err := json.Unmarshal(data, intent); err != nil {
			return "", err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	state.Calls = append(state.Calls, environmentRecoveryCall{Operation: operation, Intent: intent})
	if state.Fail == operation {
		return "", fmt.Errorf("injected %s failure", operation)
	}
	if operation == "peer4" || operation == "peer6" {
		var output strings.Builder
		chain := ""
		for line := range strings.SplitSeq(string(input), "\n") {
			if strings.HasPrefix(line, "-A ") {
				chain = strings.Fields(line)[1]
				fmt.Fprintln(&output, normalizeEnvironmentRule(line))
			}
		}
		fmt.Fprintln(&output, "-A POSTROUTING -j "+chain)
		if operation == "peer4" {
			state.Peer4 = output.String()
		} else {
			state.Peer6 = output.String()
		}
		return "", nil
	}
	for line := range strings.SplitSeq(strings.TrimSpace(string(input)), "\n") {
		parts := strings.Fields(line)
		if len(parts) != 12 {
			return "", fmt.Errorf("unexpected tuple update %q", line)
		}
		port, err := strconv.Atoi(parts[10])
		if err != nil {
			return "", err
		}
		tuple := environmentNFTTransport{Source: parts[6], Destination: parts[8], Protocol: strings.TrimPrefix(parts[4], "native_"), Port: port}
		if operation == "remove" {
			state.Transports = slices.DeleteFunc(state.Transports, func(row environmentNFTTransport) bool { return row == tuple })
		} else if !slices.Contains(state.Transports, tuple) {
			state.Transports = append(state.Transports, tuple)
		}
	}
	return "", nil
}

func recoveryNFT(t *testing.T, transports []environmentNFTTransport) []byte {
	t.Helper()
	return mutateEnvironmentNFT(t, func(objects []any) []any {
		for _, protocol := range []string{"tcp", "udp"} {
			entry := environmentNFTEntry(objects, "set", "native_"+protocol)
			delete(entry, "elem")
			var elements []any
			for _, tuple := range transports {
				if tuple.Protocol == protocol {
					elements = append(elements, map[string]any{"concat": []any{tuple.Source, tuple.Destination, tuple.Port}})
				}
			}
			if len(elements) > 0 {
				entry["elem"] = elements
			}
		}
		for _, object := range objects {
			rule, ok := object.(map[string]any)["rule"].(map[string]any)
			if !ok {
				continue
			}
			for _, expression := range rule["expr"].([]any) {
				match, ok := expression.(map[string]any)["match"].(map[string]any)
				if !ok {
					continue
				}
				if meta, ok := match["left"].(map[string]any)["meta"].(map[string]any); ok && (meta["key"] == "iifname" || meta["key"] == "oifname") {
					match["right"] = "br-one"
				}
			}
		}
		return objects
	})
}
