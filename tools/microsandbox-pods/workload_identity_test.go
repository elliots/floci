package main

import (
	"context"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"

	msbruntime "github.com/floci-io/floci/tools/microsandbox-runtime"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPodIdentityRouteUsesOwnedPeerForStrictAndProxyPods(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "docker"), []byte("#!/bin/sh\n[ \"$1\" = exec ] && [ \"$2\" = owned-peer ]\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	for _, proxy := range []bool{false, true} {
		var commands []string
		runner := commandRunner{cfg: config{peer: "owned-peer", clusterCIDR: "10.42.0.0/16", serviceCIDR: "10.43.0.0/16", clusterDNS: "10.43.0.10"},
			runtime: stubNativeRuntime{execute: func(_ context.Context, args []string, _ []byte) (msbruntime.Result, error) {
				script := args[len(args)-1]
				commands = append(commands, script)
				if strings.Contains(script, "ip -4 -o addr") {
					return msbruntime.Result{Stdout: "192.168.127.2"}, nil
				}
				if strings.Contains(script, "ip route show default") {
					return msbruntime.Result{Stdout: "192.168.127.1"}, nil
				}
				return msbruntime.Result{}, nil
			}},
		}
		net := network{runner: runner, gateway: netip.MustParseAddr("10.42.64.1"), backendPrefix: netip.MustParsePrefix("10.42.68.0/22"), peerIP: "172.18.0.2", hostIP: "172.18.0.1", peerPort: 40000}
		entry := &podEntry{vm: "owned-vm", ip: "10.42.64.2", backendIP: "10.42.68.2", link: "owned-link", proxyAvailability: proxy,
			pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "payments"}}}
		if err := net.attach(t.Context(), entry); err != nil {
			t.Fatal(err)
		}
		gateway := "10.42.64.1"
		if proxy {
			gateway = "10.42.68.1"
		}
		attached := commands[len(commands)-1]
		if !strings.Contains(attached, "ip route add 169.254.170.23/32 via "+gateway+" dev pod0;") {
			t.Fatalf("Pod Identity did not use the owned peer: %s", attached)
		}
		if strings.Contains(attached, "169.254.169.254") || strings.Contains(attached, "169.254.0.0/16") {
			t.Fatal("Pod Identity routing must not expose node instance credentials or all link-local services")
		}
	}
}
