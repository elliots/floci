package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
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
