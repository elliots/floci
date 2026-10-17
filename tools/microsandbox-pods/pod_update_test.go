package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestMetadataUpdateAcceptsResolvedEnvironmentReordering(t *testing.T) {
	entry := availabilityEntry(t)
	entry.proxyAvailability = false
	entry.pod.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "B", Value: "second"}, {Name: "A", Value: "first"}}
	p := &provider{pods: map[string]*podEntry{"default/app": entry}, runner: commandRunner{cfg: config{state: t.TempDir()}}}
	updated := entry.pod.DeepCopy()
	updated.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "A", Value: "first"}, {Name: "B", Value: "second"}}
	updated.Labels = map[string]string{"proxy-target": "true"}
	updated.Annotations = map[string]string{"example.com/change": "metadata"}
	updated.ResourceVersion = "123"
	if err := p.UpdatePod(t.Context(), updated); err != nil {
		t.Fatal(err)
	}
	if entry.pod.Labels["proxy-target"] != "true" || entry.pod.Annotations["example.com/change"] != "metadata" ||
		entry.pod.ResourceVersion != "123" || entry.pod.Spec.Containers[0].Env[0].Name != "B" || entry.state != "COLD" || entry.generation != 9 {
		t.Fatal("metadata update changed the runtime spec or failed to retain new metadata")
	}
	updated.Labels["proxy-target"] = "caller-mutation"
	if entry.pod.Labels["proxy-target"] != "true" {
		t.Fatal("caller can mutate stored metadata")
	}
	data, err := os.ReadFile(filepath.Join(p.runner.cfg.state, string(entry.pod.UID)+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var record journalRecord
	if err := json.Unmarshal(data, &record); err != nil || record.Pod.Labels["proxy-target"] != "true" {
		t.Fatalf("metadata was not persisted: %v", err)
	}
}

func TestProxyProtocolChangeRequiresPodReplacement(t *testing.T) {
	entry := availabilityEntry(t)
	entry.pod.Annotations = map[string]string{"microsandbox.dev/availability": "proxy"}
	p := &provider{pods: map[string]*podEntry{"default/app": entry}}
	updated := entry.pod.DeepCopy()
	updated.Annotations["microsandbox.dev/proxy-protocol"] = "tcp"
	if err := p.UpdatePod(t.Context(), updated); err == nil {
		t.Fatal("proxy protocol was changed without replacing its listener and pod")
	}
	if proxyProtocol(entry.pod) != "http" {
		t.Fatal("rejected update changed the retained proxy protocol")
	}
}

func TestMetadataUpdateKeepsOriginalResolvedEnvironment(t *testing.T) {
	entry := availabilityEntry(t)
	entry.proxyAvailability = false
	declared := entry.pod.DeepCopy()
	declared.Spec.EnableServiceLinks = new(false)
	declared.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "SETTING", ValueFrom: &corev1.EnvVarSource{
		ConfigMapKeyRef: &corev1.ConfigMapKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "settings"}, Key: "value"}}}}
	declared.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "secret"}}}}
	entry.declaredSpec = declared.Spec.DeepCopy()
	entry.pod.Spec = *declared.Spec.DeepCopy()
	entry.pod.Spec.Containers[0].EnvFrom = nil
	entry.pod.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "SETTING", Value: "original"}, {Name: "SECRET", Value: "original-secret"}}
	oldRuntimeSpec := entry.pod.Spec.DeepCopy()
	p := &provider{client: fake.NewClientset(declared), pods: map[string]*podEntry{"default/app": entry},
		runner: commandRunner{cfg: config{state: t.TempDir()}}}
	updated := entry.pod.DeepCopy()
	updated.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "SETTING", Value: "refreshed"}, {Name: "SECRET", Value: "refreshed-secret"},
		{Name: "NEW_SERVICE_HOST", Value: "10.43.0.7"}}
	updated.Labels = map[string]string{"proxy-target": "true"}
	if err := p.UpdatePod(t.Context(), updated); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(&entry.pod.Spec, oldRuntimeSpec) || !reflect.DeepEqual(entry.declaredSpec, &declared.Spec) {
		t.Fatal("metadata reconciliation changed the immutable runtime or declared environment")
	}
	data, err := os.ReadFile(filepath.Join(p.runner.cfg.state, string(entry.pod.UID)+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var record journalRecord
	if err := json.Unmarshal(data, &record); err != nil || !reflect.DeepEqual(record.DeclaredSpec, &declared.Spec) {
		t.Fatalf("declared environment was not journaled: %v", err)
	}
	if err := validatePod(entry.pod); err != nil {
		t.Fatalf("enableServiceLinks false was rejected: %v", err)
	}
}

func TestDeclaredSpecRejectsRealApplicationChanges(t *testing.T) {
	for _, scenario := range []string{"image", "command", "env literal", "env source", "service links"} {
		t.Run(scenario, func(t *testing.T) {
			entry := availabilityEntry(t)
			entry.proxyAvailability = false
			entry.pod.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "SETTING", Value: "original"}}
			entry.declaredSpec = entry.pod.Spec.DeepCopy()
			declared := entry.pod.DeepCopy()
			switch scenario {
			case "image":
				declared.Spec.Containers[0].Image = "changed:image"
			case "command":
				declared.Spec.Containers[0].Command = []string{"false"}
			case "env literal":
				declared.Spec.Containers[0].Env[0].Value = "changed"
			case "env source":
				declared.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "SETTING", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}}}
			case "service links":
				declared.Spec.EnableServiceLinks = new(false)
			}
			p := &provider{client: fake.NewClientset(declared), pods: map[string]*podEntry{"default/app": entry}}
			if err := p.UpdatePod(t.Context(), declared); err == nil {
				t.Fatal("changed declared spec was accepted")
			}
		})
	}
}

func TestLegacyDeclaredSpecMigrationOnlyIgnoresResolvedEnvironment(t *testing.T) {
	for _, scenario := range []string{"resolved environment", "changed image", "changed UID", "changed ports"} {
		t.Run(scenario, func(t *testing.T) {
			entry := availabilityEntry(t)
			entry.pod.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "CONFIG", Value: "old-value"}, {Name: "OLD_SERVICE_HOST", Value: "10.43.0.9"}}
			declared := entry.pod.DeepCopy()
			declared.Spec.Containers[0].Env = nil
			declared.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "config"}}}}
			switch scenario {
			case "changed image":
				declared.Spec.Containers[0].Image = "changed:image"
			case "changed UID":
				declared.UID = "changed"
			case "changed ports":
				declared.Spec.Containers[0].Ports = []corev1.ContainerPort{{ContainerPort: 9090}}
			}
			err := reconcileDeclaredSpec(entry, declared)
			if scenario == "resolved environment" {
				if err != nil || !reflect.DeepEqual(entry.declaredSpec, &declared.Spec) || entry.pod.Spec.Containers[0].Env[0].Value != "old-value" {
					t.Fatalf("legacy environment migration failed: %v", err)
				}
			} else if err == nil || entry.declaredSpec != nil {
				t.Fatal("legacy migration adopted a different application identity")
			}
		})
	}
}

func TestDeclaredSpecCaptureRejectsStalePodIdentity(t *testing.T) {
	pod := validPod()
	current := pod.DeepCopy()
	current.UID = "replacement"
	p := &provider{client: fake.NewClientset(current)}
	if _, err := p.readDeclaredPod(t.Context(), pod); err == nil {
		t.Fatal("capture adopted a replacement Pod")
	}
	current.UID = pod.UID
	current.DeletionTimestamp = new(metav1.Now())
	p.client = fake.NewClientset(current)
	if _, err := p.readDeclaredPod(t.Context(), pod); err == nil {
		t.Fatal("capture adopted a deleting Pod")
	}
}

func TestMetadataUpdateRejectsActualSpecAndRuntimePolicyChanges(t *testing.T) {
	for _, scenario := range []string{"image", "command", "env value", "env addition", "duplicate env", "UID", "availability", "idle owner", "root disk"} {
		t.Run(scenario, func(t *testing.T) {
			entry := availabilityEntry(t)
			entry.proxyAvailability = false
			entry.pod.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "A", Value: "original"}}
			p := &provider{pods: map[string]*podEntry{"default/app": entry}, runner: commandRunner{cfg: config{state: t.TempDir()}}}
			updated := entry.pod.DeepCopy()
			updated.Labels = map[string]string{"uncommitted": "true"}
			switch scenario {
			case "image":
				updated.Spec.Containers[0].Image = "busybox:changed"
			case "command":
				updated.Spec.Containers[0].Command = []string{"false"}
			case "env value":
				updated.Spec.Containers[0].Env[0].Value = "changed"
			case "env addition":
				updated.Spec.Containers[0].Env = append(updated.Spec.Containers[0].Env, corev1.EnvVar{Name: "B", Value: "new"})
			case "duplicate env":
				updated.Spec.Containers[0].Env = append(updated.Spec.Containers[0].Env, updated.Spec.Containers[0].Env[0])
			case "UID":
				updated.UID = "different"
			case "availability":
				updated.Annotations = map[string]string{"microsandbox.dev/availability": "proxy"}
			case "root disk":
				updated.Annotations = map[string]string{rootDiskAnnotation: "8Gi"}
			case "idle owner":
				updated.Annotations = map[string]string{"microsandbox.dev/idle-owner": "external"}
			}
			if err := p.UpdatePod(t.Context(), updated); err == nil {
				t.Fatal("unsupported in-place change accepted")
			}
			if len(entry.pod.Labels) != 0 || entry.pod.Spec.Containers[0].Env[0].Value != "original" {
				t.Fatal("rejected update changed stored Pod")
			}
			files, err := os.ReadDir(p.runner.cfg.state)
			if err != nil || len(files) != 0 {
				t.Fatalf("rejected update changed persisted state: %v", err)
			}
		})
	}
}
