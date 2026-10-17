package main

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

type boundaryRuntime struct {
	stubNativeRuntime
	verify      func(context.Context) error
	fingerprint string
	caPath      string
}

func (r boundaryRuntime) VerifyNetworkBoundary(ctx context.Context) error { return r.verify(ctx) }
func (r boundaryRuntime) NetworkPolicyFingerprint() string                { return r.fingerprint }
func (r boundaryRuntime) GuestCAPath() string                             { return r.caPath }

func TestRuntimeBoundaryVerificationAndPublishedIdentity(t *testing.T) {
	blocked := errors.New("older active guest is not isolated")
	runner := commandRunner{runtime: boundaryRuntime{fingerprint: "policy-hash", caPath: reservedCAPath,
		verify: func(ctx context.Context) error { return blocked }}}
	if err := runner.verifyNetworkBoundary(t.Context()); !errors.Is(err, blocked) {
		t.Fatalf("boundary refusal did not propagate: %v", err)
	}
	if runner.networkFingerprint() != "policy-hash" || runner.guestCAPath() != reservedCAPath {
		t.Fatal("runtime boundary identity or CA path was lost")
	}
	runner.runtime = boundaryRuntime{caPath: "/private/host/key.pem"}
	if runner.guestCAPath() != "" {
		t.Fatal("unexpected SDK path became a hostPath exception")
	}
}

func TestPodBoundaryRefusesIsolationWithoutProtectedPeer(t *testing.T) {
	verified := false
	runner := commandRunner{runtime: boundaryRuntime{fingerprint: "valid-sdk-policy", verify: func(ctx context.Context) error {
		verified = true
		return nil
	}}}
	if err := runner.verifyNetworkBoundary(t.Context()); err == nil || !strings.Contains(err.Error(), "protected Kubernetes network peer") {
		t.Fatalf("SDK fingerprint advertised complete Pod isolation: %v", err)
	}
	if !verified {
		t.Fatal("existing SDK boundary ownership was not checked")
	}
	runner.runtime = boundaryRuntime{verify: func(ctx context.Context) error { return nil }}
	if err := runner.verifyNetworkBoundary(t.Context()); err != nil {
		t.Fatalf("ordinary native networking was refused: %v", err)
	}
}

func caPod() *corev1.Pod {
	pod := validPod()
	pod.Spec.Volumes = []corev1.Volume{{Name: "floci-network-ca", VolumeSource: corev1.VolumeSource{
		HostPath: &corev1.HostPathVolumeSource{Path: reservedCAPath, Type: new(corev1.HostPathFile)},
	}}}
	pod.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "floci-network-ca", MountPath: reservedCAPath, ReadOnly: true}}
	return pod
}

func TestNetworkCAIsTheOnlyReadOnlyGuestHostPathException(t *testing.T) {
	pod := caPod()
	if err := validatePod(pod); err == nil {
		t.Fatal("CA hostPath accepted without a configured native boundary")
	}
	if err := validatePodWithCA(pod, reservedCAPath); err != nil {
		t.Fatal(err)
	}
	want := []string{"--mount", "type=bind,src=" + reservedCAPath + ",dst=" + reservedCAPath + ",options=rbind:rprivate:ro"}
	if got := volumeMountArgs(pod); !slices.Equal(got, want) {
		t.Fatalf("CA did not bind the preboot guest copy read-only: %v", got)
	}
	setup := volumeSetupScript(pod.Spec.Volumes[0], vmMemoryMiB)
	if !strings.Contains(setup, "test -f") || !strings.Contains(setup, "test ! -L") || !strings.Contains(setup, "chmod 0444") {
		t.Fatalf("CA setup did not validate its public file: %s", setup)
	}
	for name, mutate := range map[string]func(*corev1.Pod){
		"host private key": func(pod *corev1.Pod) { pod.Spec.Volumes[0].HostPath.Path = "/etc/floci/certs/ca.key" },
		"host directory":   func(pod *corev1.Pod) { pod.Spec.Volumes[0].HostPath.Path = "/etc/floci/certs" },
		"directory type":   func(pod *corev1.Pod) { pod.Spec.Volumes[0].HostPath.Type = new(corev1.HostPathDirectory) },
		"missing type":     func(pod *corev1.Pod) { pod.Spec.Volumes[0].HostPath.Type = nil },
		"alternate source": func(pod *corev1.Pod) { pod.Spec.Volumes[0].Secret = &corev1.SecretVolumeSource{SecretName: "secret"} },
		"writable":         func(pod *corev1.Pod) { pod.Spec.Containers[0].VolumeMounts[0].ReadOnly = false },
		"different destination": func(pod *corev1.Pod) {
			pod.Spec.Containers[0].VolumeMounts[0].MountPath = "/other/ca.crt"
		},
		"subpath": func(pod *corev1.Pod) { pod.Spec.Containers[0].VolumeMounts[0].SubPath = "certificate" },
	} {
		t.Run(name, func(t *testing.T) {
			copy := pod.DeepCopy()
			mutate(copy)
			if err := validatePodWithCA(copy, reservedCAPath); err == nil {
				t.Fatal("unsafe CA mount was accepted")
			}
		})
	}
}
