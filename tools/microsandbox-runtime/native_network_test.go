//go:build native

package microsandboxruntime_test

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	runtime "github.com/floci-io/floci/tools/microsandbox-runtime"
)

// Run explicitly against a dedicated native runtime home. Normal unit tests do
// not create VMs; -tags native makes missing prerequisites a test failure.
func TestNativeNetworkBoundary(t *testing.T) {
	config := runtime.Config{Executable: os.Getenv("FLOCI_NATIVE_TEST_MSB"), Home: os.Getenv("FLOCI_NATIVE_TEST_HOME"),
		GuestAgentPath: os.Getenv("FLOCI_MICROSANDBOX_GUEST_AGENT_PATH")}
	if config.Executable == "" || config.Home == "" || config.GuestAgentPath == "" {
		t.Fatal("native qualification requires FLOCI_NATIVE_TEST_MSB, FLOCI_NATIVE_TEST_HOME and FLOCI_MICROSANDBOX_GUEST_AGENT_PATH")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	var forbiddenHits, dnsQueries atomic.Int32
	allowed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, "internal-ok") }))
	defer allowed.Close()
	forbidden := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		forbiddenHits.Add(1)
		fmt.Fprint(w, "forbidden")
	}))
	defer forbidden.Close()
	dns, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer dns.Close()
	go func() {
		buffer := make([]byte, 4096)
		for {
			n, peer, err := dns.ReadFrom(buffer)
			if err != nil {
				return
			}
			if n < 12 {
				continue
			}
			dnsQueries.Add(1)
			reply := append([]byte(nil), buffer[:n]...)
			reply[2], reply[3] = 0x81, 0x83 // Controlled NXDOMAIN, no public recursion.
			clear(reply[6:12])
			_, _ = dns.WriteTo(reply, peer)
		}
	}()
	_, allowedPort, _ := net.SplitHostPort(strings.TrimPrefix(allowed.URL, "http://"))
	_, forbiddenPort, _ := net.SplitHostPort(strings.TrimPrefix(forbidden.URL, "http://"))
	policy := fmt.Sprintf("network:\n  isolation: {enabled: true}\n  native:\n    dns-nameservers: [%q]\n    allowed-egress:\n      - {destination: host, protocol: tcp, ports: %q}\n", dns.LocalAddr().String(), allowedPort)
	config.NetworkConfigFile = filepath.Join(t.TempDir(), "network.yaml")
	if err := os.WriteFile(config.NetworkConfigFile, []byte(policy), 0600); err != nil {
		t.Fatal(err)
	}
	client, err := runtime.New(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.VerifyNetworkBoundary(ctx); err != nil {
		t.Fatal("dedicated runtime home contains incompatible active guests:", err)
	}
	vm := "floci-network-test-" + strings.ToLower(rand.Text()[:10])
	t.Logf("owned guest: %s", vm)
	checkpoints := []string{vm + ":first", vm + ":second"}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		inventory, err := client.Execute(cleanup, []string{"ls"}, nil)
		if err != nil {
			t.Error("cannot inspect owned guest for cleanup:", err)
			return
		}
		var guests []struct{ Name string }
		if err := json.Unmarshal([]byte(inventory.Stdout), &guests); err != nil {
			t.Error(err)
			return
		}
		for _, guest := range guests {
			if guest.Name == vm {
				if _, err := client.Execute(cleanup, []string{"rm", "--force", vm}, nil); err != nil {
					t.Error("cannot remove owned guest:", err)
				}
			}
		}
		inventory, err = client.Execute(cleanup, []string{"snap", "ls"}, nil)
		if err != nil {
			t.Error("cannot inspect owned checkpoints:", err)
			return
		}
		var snapshots []struct{ Name, Group string }
		if err := json.Unmarshal([]byte(inventory.Stdout), &snapshots); err != nil {
			t.Error(err)
			return
		}
		present := map[string]bool{}
		for _, snapshot := range snapshots {
			if snapshot.Group == vm && (snapshot.Name == "first" || snapshot.Name == "second") {
				present[vm+":"+snapshot.Name] = true
			}
		}
		// The SDK refuses to remove a current head while other members exist.
		for _, checkpoint := range checkpoints {
			if present[checkpoint] {
				if _, err := client.Execute(cleanup, []string{"snap", "rm", "--force", checkpoint}, nil); err != nil {
					t.Errorf("cannot remove owned checkpoint %s: %v (%v)", checkpoint, err, errors.Unwrap(err))
				}
			}
		}
	})
	execute := func(args ...string) runtime.Result {
		t.Helper()
		result, err := client.Execute(ctx, args, nil)
		if err != nil || result.ExitCode != 0 {
			t.Fatalf("%s failed: %v, exit=%d, stderr=%s", args[0], err, result.ExitCode, result.Stderr)
		}
		return result
	}
	execute("run", "-d", "--name", vm, "--cpus", "1", "--memory", "384M", "--root-disk", "flat:2G", "--net", "all",
		"--entrypoint", "/bin/sh", "rancher/k3s:v1.35.0-k3s1", "--", "-c", "exec sleep 2147483647")
	check := func() {
		t.Helper()
		before := dnsQueries.Load()
		query := "fresh-" + strings.ToLower(rand.Text()) + ".vendor.test"
		script := "gateway=$(ip route show default | awk 'NR==1 {print $3}'); " +
			"wget -q -T 3 -O- http://$gateway:" + allowedPort + "; " +
			"if wget -q -T 1 -O- http://$gateway:" + forbiddenPort + "; then exit 11; fi; " +
			"if wget -q -T 1 -O- http://1.1.1.1:80; then exit 12; fi; " +
			"if nslookup " + query + " >/tmp/dns-result 2>&1; then exit 13; fi; " +
			"if pidof guestnet; then exit 14; fi; " +
			"test ! -e /var/lib/floci/network-dns/guestnet; test ! -e /var/lib/floci/network-dns/config.json; cat /tmp/dns-result"
		result := execute("exec", vm, "--", "sh", "-ec", script)
		if !strings.Contains(result.Stdout, "internal-ok") || forbiddenHits.Load() != 0 || dnsQueries.Load() <= before {
			t.Fatalf("network boundary failed: %s, forbidden=%d DNS=%d/%d", result.Stdout, forbiddenHits.Load(), dnsQueries.Load(), before)
		}
	}
	check()
	for index, checkpoint := range checkpoints {
		execute("pause", vm)
		execute("snap", "create", strings.TrimPrefix(checkpoint, vm+":"), "--sandbox", vm, "--full", "--integrity")
		execute("stop", "--force", vm)
		execute("rm", vm)
		execute("snap", "verify", checkpoint)
		restore := []string{"snap", "restore", checkpoint, "--name", vm}
		if index == 1 {
			restore = append(restore, "--net", "all")
		}
		execute(restore...)
		check()
		before := dnsQueries.Load()
		result, err := client.Execute(ctx, []string{"exec", vm, "--", "timeout", "8", "nslookup", "example.com", "1.1.1.1"}, nil)
		if err != nil || result.ExitCode == 0 || dnsQueries.Load() != before {
			t.Fatalf("SDK DNS interceptor escaped restored deny policy: %v, %#v", err, result)
		}
	}
	execute("stop", "--force", vm)
	execute("start", vm)
	check()
	if err := os.WriteFile(config.NetworkConfigFile, []byte(policy+"# changed\n"), 0600); err != nil {
		t.Fatal(err)
	}
	changed, err := runtime.New(config)
	if err != nil {
		t.Fatal(err)
	}
	if changed.VerifyNetworkBoundary(ctx) == nil {
		t.Fatal("changed policy adopted an existing guest")
	}
	if _, err := changed.Execute(ctx, []string{"exec", vm, "--", "true"}, nil); err == nil {
		t.Fatal("changed policy executed in an existing guest")
	}
	if err := os.WriteFile(config.NetworkConfigFile, []byte(policy), 0600); err != nil {
		t.Fatal(err)
	}
}
