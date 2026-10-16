package main

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestPodGroupPoliciesValidateAndReachRuntime(t *testing.T) {
	for _, policy := range []corev1.SupplementalGroupsPolicy{corev1.SupplementalGroupsPolicyMerge, corev1.SupplementalGroupsPolicyStrict} {
		pod := &corev1.Pod{Spec: corev1.PodSpec{SecurityContext: &corev1.PodSecurityContext{
			FSGroup: new(int64(3000)), SupplementalGroups: []int64{4000, 3000}, SupplementalGroupsPolicy: new(policy), FSGroupChangePolicy: new(corev1.FSGroupChangeOnRootMismatch),
		}, Containers: []corev1.Container{{Name: "app"}}}}
		if err := validateSecurityContexts(pod); err != nil {
			t.Fatal(err)
		}
		if !requiresGuestFS(pod) {
			t.Fatal("group helper not required")
		}
		wrapper := containerGroupWrapper(pod, "/private/container.groups")
		for _, part := range []string{"--groups '3000,4000'", "--policy '" + string(policy) + "'", "--state '/private/container.groups'", `-- "$@"`} {
			if !strings.Contains(wrapper, part) {
				t.Fatalf("wrapper misses %s: %s", part, wrapper)
			}
		}
	}
}

func TestEmptyDirOwnershipRunsOnlyDuringInitialization(t *testing.T) {
	pod := &corev1.Pod{Spec: corev1.PodSpec{SecurityContext: &corev1.PodSecurityContext{FSGroup: new(int64(3000)), FSGroupChangePolicy: new(corev1.FSGroupChangeOnRootMismatch)}}}
	for _, medium := range []corev1.StorageMedium{corev1.StorageMediumDefault, corev1.StorageMediumMemory} {
		volume := corev1.Volume{Name: "data", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: medium}}}
		script := volumeSetupWithOwnership(pod, volume, 768)
		ownership := strings.Index(script, "'ownership'")
		if ownership < 0 || ownership > strings.LastIndex(script, "fi") {
			t.Fatalf("ownership repeats after initialization: %s", script)
		}
		if strings.Contains(script, "on-root-mismatch") {
			t.Fatal("fsGroupChangePolicy must not affect emptyDir")
		}
	}
}

func TestProjectionGetsGroupReadBeforeAtomicPublication(t *testing.T) {
	pod := &corev1.Pod{Spec: corev1.PodSpec{SecurityContext: &corev1.PodSecurityContext{FSGroup: new(int64(3000))}}}
	script := projectionScriptWithOwnership(pod, "/volume", "..floci-new", map[string]volumeFile{"secret": {Data: []byte("value"), Mode: 0400}})
	if strings.Index(script, "'ownership'") < strings.Index(script, "tar -xf") || strings.Index(script, "'ownership'") > strings.Index(script, "mv -Tf") || !strings.Contains(script, "'--read-only'") {
		t.Fatalf("permissions not applied before publication: %s", script)
	}
	ca := corev1.Volume{Name: "ca", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: reservedCAPath}}}
	if strings.Contains(volumeSetupWithOwnership(pod, ca, 768), "ownership") {
		t.Fatal("reserved public CA must not receive pod ownership")
	}
}
