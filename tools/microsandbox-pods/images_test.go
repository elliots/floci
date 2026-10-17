package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	msbruntime "github.com/floci-io/floci/tools/microsandbox-runtime"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestImagePullPoliciesRespectCompleteGuestCache(t *testing.T) {
	for _, test := range []struct {
		name   string
		policy corev1.PullPolicy
		cached bool
		pull   bool
		fail   bool
	}{
		{"cached IfNotPresent", corev1.PullIfNotPresent, true, false, false},
		{"uncached IfNotPresent", corev1.PullIfNotPresent, false, true, false},
		{"cached Never", corev1.PullNever, true, false, false},
		{"uncached Never", corev1.PullNever, false, false, true},
		{"cached Always", corev1.PullAlways, true, true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			p, e := probeFixture(t)
			container := e.pod.Spec.Containers[0]
			container.ImagePullPolicy = test.policy
			log := filepath.Join(p.runner.cfg.state, "calls")
			cache := ""
			if test.cached {
				cache = normalizeImage(container.Image)
			}
			script := "#!/bin/sh\nprintf '%s\\n' \"$*\" >>" + quote(log) + "\ncase \"$*\" in *'images check'*) printf '%s\\n' " + quote(cache) + ";; esac\n"
			if err := os.WriteFile(p.runner.cfg.msb, []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			err := p.ensureApplicationImage(t.Context(), e, container)
			if (err != nil) != test.fail {
				t.Fatalf("image decision: %v", err)
			}
			data, err := os.ReadFile(log)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(data), "images pull") != test.pull {
				t.Fatalf("unexpected registry access: %s", data)
			}
		})
	}
}

func TestImageDefaultsMatchTagAndDigestSemantics(t *testing.T) {
	for image, want := range map[string]corev1.PullPolicy{
		"busybox": corev1.PullAlways, "busybox:latest": corev1.PullAlways,
		"registry.test:5000/example": corev1.PullAlways,
		"busybox:1.37.0":             corev1.PullIfNotPresent, "example@sha256:abc": corev1.PullIfNotPresent,
	} {
		if actual := effectivePullPolicy(corev1.Container{Image: image}); actual != want {
			t.Fatalf("%s: %s", image, actual)
		}
	}
}

func TestRegistryPullUsesOnlyConfiguredReservedEnvironmentCA(t *testing.T) {
	for _, ca := range []string{"", reservedCAPath, "/private/host/signing.key"} {
		for _, authenticated := range []bool{false, true} {
			t.Run(ca+"/authenticated="+fmt.Sprint(authenticated), func(t *testing.T) {
				p, e := probeFixture(t)
				container := e.pod.Spec.Containers[0]
				container.Image = "registry.test/app:v1"
				container.ImagePullPolicy = corev1.PullAlways
				p.client = fake.NewClientset()
				if authenticated {
					e.pod.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "pull"}}
					p.client = fake.NewClientset(registrySecret(t, e.pod.Namespace, "pull", map[string]dockerAuthEntry{
						"registry.test": {Username: "user", Password: "secret"},
					}))
				}
				calls := 0
				p.runner.runtime = boundaryRuntime{caPath: ca, stubNativeRuntime: stubNativeRuntime{
					execute: func(_ context.Context, args []string, input []byte) (msbruntime.Result, error) {
						calls++
						script := args[len(args)-1]
						trusted := ca == reservedCAPath
						if strings.Contains(script, "--tlscacert "+quote(reservedCAPath)) != trusted ||
							strings.Contains(script, "--local") != (trusted || authenticated) {
							t.Fatalf("incorrect isolated image pull: %s", script)
						}
						for _, unsafe := range []string{"--skip-verify", "--plain-http", "/private/host", "user:secret", "update-ca-certificates"} {
							if strings.Contains(script, unsafe) {
								t.Fatalf("pull changed trust or exposed credentials: %s", unsafe)
							}
						}
						if authenticated && string(input) != base64.StdEncoding.EncodeToString([]byte("user:secret"))+"\n" {
							t.Fatal("registry secret did not use the existing stdin transport")
						}
						return msbruntime.Result{}, nil
					},
				}}
				if err := p.ensureApplicationImage(t.Context(), e, container); err != nil {
					t.Fatal(err)
				}
				if calls != 1 {
					t.Fatalf("unexpected guest operations: %d", calls)
				}
			})
		}
	}
}
