package main

import (
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestSecurityIdentityOverridesAndImageDefaults(t *testing.T) {
	scenarios := []struct {
		name      string
		pod       *corev1.PodSecurityContext
		container *corev1.SecurityContext
		imageUser string
		wantUser  string
	}{
		{name: "image username unchanged", imageUser: "app:staff"},
		{name: "image UID unchanged", imageUser: "1001:2001"},
		{name: "empty contexts unchanged", pod: &corev1.PodSecurityContext{}, container: &corev1.SecurityContext{}, imageUser: "app"},
		{name: "pod UID", pod: &corev1.PodSecurityContext{RunAsUser: new(int64(1000))}, imageUser: "app:staff", wantUser: "1000"},
		{name: "pod UID and GID", pod: &corev1.PodSecurityContext{RunAsUser: new(int64(1000)), RunAsGroup: new(int64(2000))}, wantUser: "1000:2000"},
		{name: "container overrides UID only", pod: &corev1.PodSecurityContext{RunAsUser: new(int64(1000)), RunAsGroup: new(int64(2000))}, container: &corev1.SecurityContext{RunAsUser: new(int64(1001))}, wantUser: "1001:2000"},
		{name: "container overrides GID only", pod: &corev1.PodSecurityContext{RunAsUser: new(int64(1000)), RunAsGroup: new(int64(2000))}, container: &corev1.SecurityContext{RunAsGroup: new(int64(2001))}, wantUser: "1000:2001"},
		{name: "explicit root overrides pod", pod: &corev1.PodSecurityContext{RunAsUser: new(int64(1000))}, container: &corev1.SecurityContext{RunAsUser: new(int64(0))}, wantUser: "0"},
		{name: "GID with default root", container: &corev1.SecurityContext{RunAsGroup: new(int64(2000))}, wantUser: "0:2000"},
		{name: "GID replaces image group", container: &corev1.SecurityContext{RunAsGroup: new(int64(2000))}, imageUser: "app:staff", wantUser: "app:2000"},
		{name: "UID ignores image group", container: &corev1.SecurityContext{RunAsUser: new(int64(1000))}, imageUser: "app:staff", wantUser: "1000"},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			pod := validPod()
			pod.Spec.SecurityContext = scenario.pod
			pod.Spec.Containers[0].SecurityContext = scenario.container
			before := pod.DeepCopy()
			args, err := securityArgs(pod, pod.Spec.Containers[0], scenario.imageUser)
			if err != nil {
				t.Fatal(err)
			}
			if got := securityFlagValue(args, "--user"); got != scenario.wantUser {
				t.Fatalf("user = %q, want %q, args %v", got, scenario.wantUser, args)
			}
			if !slices.Contains(args, "--allow-new-privs") {
				t.Fatal("Kubernetes default privilege escalation was not preserved")
			}
			if !reflect.DeepEqual(pod, before) {
				t.Fatal("security context was mutated")
			}
		})
	}
}

func TestSecurityRunAsNonRoot(t *testing.T) {
	for _, imageUser := range []string{"", "0", "0:1000", "root", "app", "app:1000", "-1", "2147483648", "999999999999999999999999"} {
		t.Run("reject "+imageUser, func(t *testing.T) {
			pod := validPod()
			pod.Spec.SecurityContext = &corev1.PodSecurityContext{RunAsNonRoot: new(true)}
			if _, err := securityArgs(pod, pod.Spec.Containers[0], imageUser); err == nil {
				t.Fatalf("runAsNonRoot accepted unverified/root image USER %q", imageUser)
			}
		})
	}
	for _, imageUser := range []string{"1", "1000:2000", "2147483647", "1000:staff"} {
		t.Run("accept "+imageUser, func(t *testing.T) {
			pod := validPod()
			pod.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{RunAsNonRoot: new(true)}
			args, err := securityArgs(pod, pod.Spec.Containers[0], imageUser)
			if err != nil {
				t.Fatal(err)
			}
			if slices.Contains(args, "--user") {
				t.Fatal("image user was overridden unnecessarily")
			}
		})
	}
	pod := validPod()
	pod.Spec.SecurityContext = &corev1.PodSecurityContext{RunAsUser: new(int64(0)), RunAsNonRoot: new(true)}
	if err := validateSecurityContexts(pod); err == nil {
		t.Fatal("explicit root/non-root conflict accepted")
	}
	pod.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{RunAsUser: new(int64(1000))}
	if _, err := securityArgs(pod, pod.Spec.Containers[0], "named"); err != nil {
		t.Fatalf("explicit numeric UID did not override image or pod: %v", err)
	}
	pod.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{RunAsNonRoot: new(false)}
	if _, err := securityArgs(pod, pod.Spec.Containers[0], "root"); err != nil {
		t.Fatalf("explicit false did not override pod non-root requirement: %v", err)
	}
}

func TestSecurityRootFilesystemAndPrivilegeEscalation(t *testing.T) {
	for _, readOnly := range []bool{false, true} {
		for _, escalation := range []bool{false, true} {
			pod := validPod()
			pod.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{
				Privileged: new(false), ProcMount: new(corev1.DefaultProcMount),
				ReadOnlyRootFilesystem: new(readOnly), AllowPrivilegeEscalation: new(escalation),
			}
			args, err := securityArgs(pod, pod.Spec.Containers[0], "")
			if err != nil {
				t.Fatal(err)
			}
			if slices.Contains(args, "--read-only") != readOnly || slices.Contains(args, "--allow-new-privs") != escalation {
				t.Fatalf("readOnly=%v escalation=%v generated %v", readOnly, escalation, args)
			}
		}
	}
}

func TestSecurityCapabilitiesMatchContainerdCRIOrder(t *testing.T) {
	scenarios := []struct {
		name     string
		add      []corev1.Capability
		drop     []corev1.Capability
		want     []string
		startAll bool
	}{
		{name: "drop all", drop: []corev1.Capability{"ALL"}, startAll: true},
		{name: "drop all then add", add: []corev1.Capability{"NET_BIND_SERVICE"}, drop: []corev1.Capability{"ALL"}, want: []string{"NET_BIND_SERVICE"}, startAll: true},
		{name: "explicit drop wins", add: []corev1.Capability{"NET_BIND_SERVICE"}, drop: []corev1.Capability{"ALL", "NET_BIND_SERVICE"}, startAll: true},
		{name: "drop all wins over add all", add: []corev1.Capability{"ALL"}, drop: []corev1.Capability{"ALL"}, startAll: true},
		{name: "explicit add survives both all", add: []corev1.Capability{"ALL", "NET_BIND_SERVICE"}, drop: []corev1.Capability{"ALL"}, want: []string{"NET_BIND_SERVICE"}},
		{name: "case normalized and deduplicated", add: []corev1.Capability{"net_bind_service", "NET_BIND_SERVICE"}, want: []string{"NET_BIND_SERVICE"}},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			args, err := securityCapabilityArgs(&corev1.Capabilities{Add: scenario.add, Drop: scenario.drop})
			if err != nil {
				t.Fatal(err)
			}
			effective := make(map[string]bool)
			if scenario.startAll {
				for _, name := range linuxCapabilities {
					effective[name] = true
				}
			}
			// ctr applies every add before every drop, regardless of argv order.
			for _, operation := range []string{"--cap-add", "--cap-drop"} {
				for i := 0; i < len(args); i += 2 {
					if args[i] == operation {
						name := strings.TrimPrefix(args[i+1], "CAP_")
						if operation == "--cap-add" {
							effective[name] = true
						} else {
							delete(effective, name)
						}
					}
				}
			}
			if len(effective) != len(scenario.want) {
				t.Fatalf("effective capabilities %v, want %v", effective, scenario.want)
			}
			for _, name := range scenario.want {
				if !effective[name] {
					t.Fatalf("missing capability %s in %v", name, effective)
				}
			}
		})
	}
	args, err := securityCapabilityArgs(&corev1.Capabilities{Add: []corev1.Capability{"ALL"}, Drop: []corev1.Capability{"SYS_ADMIN"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(args) != 2*(len(linuxCapabilities)+1) || !slices.Contains(args, "CAP_SYS_ADMIN") {
		t.Fatalf("add ALL with named drop not expanded: %v", args)
	}
}

func TestSecurityRejectsUnsupportedSettings(t *testing.T) {
	scenarios := map[string]func(*corev1.Pod){
		"pod negative UID":        func(p *corev1.Pod) { p.Spec.SecurityContext.RunAsUser = new(int64(-1)) },
		"pod oversized GID":       func(p *corev1.Pod) { p.Spec.SecurityContext.RunAsGroup = new(int64(math.MaxInt32 + 1)) },
		"container negative GID":  func(p *corev1.Pod) { p.Spec.Containers[0].SecurityContext.RunAsGroup = new(int64(-1)) },
		"container oversized UID": func(p *corev1.Pod) { p.Spec.Containers[0].SecurityContext.RunAsUser = new(int64(math.MaxInt32 + 1)) },
		"fsGroup":                 func(p *corev1.Pod) { p.Spec.SecurityContext.FSGroup = new(int64(-1)) },
		"fsGroup policy": func(p *corev1.Pod) {
			p.Spec.SecurityContext.FSGroupChangePolicy = new(corev1.PodFSGroupChangePolicy("Unknown"))
		},
		"supplemental groups": func(p *corev1.Pod) { p.Spec.SecurityContext.SupplementalGroups = []int64{math.MaxInt32 + 1} },
		"supplemental groups policy": func(p *corev1.Pod) {
			p.Spec.SecurityContext.SupplementalGroupsPolicy = new(corev1.SupplementalGroupsPolicy("Unknown"))
		},
		"sysctls": func(p *corev1.Pod) {
			p.Spec.SecurityContext.Sysctls = []corev1.Sysctl{{Name: "net.ipv4.ip_forward", Value: "1"}}
		},
		"pod SELinux": func(p *corev1.Pod) { p.Spec.SecurityContext.SELinuxOptions = &corev1.SELinuxOptions{} },
		"pod Windows": func(p *corev1.Pod) { p.Spec.SecurityContext.WindowsOptions = &corev1.WindowsSecurityContextOptions{} },
		"pod localhost seccomp": func(p *corev1.Pod) {
			p.Spec.SecurityContext.SeccompProfile = &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeLocalhost}
		},
		"pod AppArmor": func(p *corev1.Pod) {
			p.Spec.SecurityContext.AppArmorProfile = &corev1.AppArmorProfile{Type: corev1.AppArmorProfileTypeRuntimeDefault}
		},
		"privileged":        func(p *corev1.Pod) { p.Spec.Containers[0].SecurityContext.Privileged = new(true) },
		"unmasked proc":     func(p *corev1.Pod) { p.Spec.Containers[0].SecurityContext.ProcMount = new(corev1.UnmaskedProcMount) },
		"container SELinux": func(p *corev1.Pod) { p.Spec.Containers[0].SecurityContext.SELinuxOptions = &corev1.SELinuxOptions{} },
		"container Windows": func(p *corev1.Pod) {
			p.Spec.Containers[0].SecurityContext.WindowsOptions = &corev1.WindowsSecurityContextOptions{}
		},
		"container localhost seccomp": func(p *corev1.Pod) {
			p.Spec.Containers[0].SecurityContext.SeccompProfile = &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeLocalhost}
		},
		"container AppArmor": func(p *corev1.Pod) {
			p.Spec.Containers[0].SecurityContext.AppArmorProfile = &corev1.AppArmorProfile{Type: corev1.AppArmorProfileTypeUnconfined}
		},
		"unknown capability": func(p *corev1.Pod) {
			p.Spec.Containers[0].SecurityContext.Capabilities = &corev1.Capabilities{Add: []corev1.Capability{"MADE_UP"}}
		},
		"prefixed capability": func(p *corev1.Pod) {
			p.Spec.Containers[0].SecurityContext.Capabilities = &corev1.Capabilities{Drop: []corev1.Capability{"CAP_CHOWN"}}
		},
	}
	for name, mutate := range scenarios {
		t.Run(name, func(t *testing.T) {
			pod := validPod()
			pod.Spec.SecurityContext = &corev1.PodSecurityContext{}
			pod.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{}
			mutate(pod)
			if err := validateSecurityContexts(pod); err == nil {
				t.Fatal("unsupported or invalid security setting was accepted")
			}
		})
	}
}

func TestSecuritySeccompUsesContainerOverrideThenPodDefault(t *testing.T) {
	profiles := []struct {
		name    string
		profile *corev1.SeccompProfile
	}{
		{name: "omitted"},
		{name: "RuntimeDefault", profile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
		{name: "Unconfined", profile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeUnconfined}},
	}
	for _, podProfile := range profiles {
		for _, containerProfile := range profiles {
			t.Run(podProfile.name+"/"+containerProfile.name, func(t *testing.T) {
				pod := validPod()
				if podProfile.profile != nil {
					pod.Spec.SecurityContext = &corev1.PodSecurityContext{SeccompProfile: podProfile.profile}
				}
				if containerProfile.profile != nil {
					pod.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{SeccompProfile: containerProfile.profile}
				}
				before := pod.DeepCopy()
				args, err := securityArgs(pod, pod.Spec.Containers[0], "")
				if err != nil {
					t.Fatal(err)
				}
				effective := podProfile.profile
				if containerProfile.profile != nil {
					effective = containerProfile.profile
				}
				wantSeccomp := effective != nil && effective.Type == corev1.SeccompProfileTypeRuntimeDefault
				if slices.Contains(args, "--seccomp") != wantSeccomp {
					t.Fatalf("seccomp args = %v, want RuntimeDefault=%v", args, wantSeccomp)
				}
				if slices.Contains(args, "--seccomp-profile") {
					t.Fatal("built-in seccomp selection must not load an arbitrary file")
				}
				if !reflect.DeepEqual(pod, before) {
					t.Fatal("seccomp selection mutated pod security contexts")
				}
			})
		}
	}
	pod := validPod()
	pod.Spec.SecurityContext = &corev1.PodSecurityContext{SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}}
	pod.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{}
	args, err := securityArgs(pod, pod.Spec.Containers[0], "")
	if err != nil || !slices.Contains(args, "--seccomp") {
		t.Fatalf("empty container securityContext must inherit pod profile: %v (%v)", args, err)
	}
}

func TestSecuritySeccompRejectsUnknownTypesAndLocalPaths(t *testing.T) {
	for _, profile := range []*corev1.SeccompProfile{
		{},
		{Type: "runtime/default"},
		{Type: "Unknown"},
		{Type: corev1.SeccompProfileTypeLocalhost},
		{Type: corev1.SeccompProfileTypeLocalhost, LocalhostProfile: new("profiles/custom.json")},
		{Type: corev1.SeccompProfileTypeRuntimeDefault, LocalhostProfile: new("")},
		{Type: corev1.SeccompProfileTypeRuntimeDefault, LocalhostProfile: new("/etc/seccomp/profile.json")},
		{Type: corev1.SeccompProfileTypeUnconfined, LocalhostProfile: new("../../outside.json")},
	} {
		for _, level := range []string{"pod", "container"} {
			t.Run(level+"/"+string(profile.Type), func(t *testing.T) {
				pod := validPod()
				if level == "pod" {
					pod.Spec.SecurityContext = &corev1.PodSecurityContext{SeccompProfile: profile}
				} else {
					pod.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{SeccompProfile: profile}
				}
				if err := validateSecurityContexts(pod); err == nil {
					t.Fatalf("unsupported seccomp profile accepted: %#v", profile)
				}
			})
		}
	}
}

func securityFlagValue(args []string, flag string) string {
	for i, arg := range args {
		if arg == flag && i+1 < len(args) {
			return args[i+1]
		}
	}
	return ""
}
