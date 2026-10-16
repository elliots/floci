package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	msbruntime "github.com/floci-io/floci/tools/microsandbox-runtime"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func defaultAccountVolume() corev1.Volume {
	return corev1.Volume{Name: "kube-api-access", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{Sources: []corev1.VolumeProjection{
		{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token", ExpirationSeconds: new(int64(3607))}},
		{ConfigMap: &corev1.ConfigMapProjection{LocalObjectReference: corev1.LocalObjectReference{Name: "kube-root-ca.crt"}, Items: []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}}}},
		{DownwardAPI: &corev1.DownwardAPIProjection{Items: []corev1.DownwardAPIVolumeFile{{Path: "namespace", FieldRef: &corev1.ObjectFieldSelector{APIVersion: "v1", FieldPath: "metadata.namespace"}}}}},
	}}}}
}

func projectedFixture(t *testing.T) (*provider, *podEntry, *tokenFixture) {
	t.Helper()
	f := newTokenFixture(t)
	p, e := probeFixture(t)
	e.vm = "owned-token-pod"
	e.pod = f.pod
	e.tokens.now = func() time.Time { return f.now }
	p.client = f.client
	e.pod.Spec.Volumes = []corev1.Volume{defaultAccountVolume()}
	e.pod.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "kube-api-access", MountPath: "/var/run/secrets/kubernetes.io/serviceaccount", ReadOnly: true}}
	_, err := f.client.CoreV1().ConfigMaps(e.pod.Namespace).Create(t.Context(), &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "kube-root-ca.crt", Namespace: e.pod.Namespace}, Data: map[string]string{"ca.crt": "public-kubernetes-ca"}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return p, e, f
}

func TestDefaultAutomountProjectionUsesReadOnlyTmpfsAndPodIdentity(t *testing.T) {
	p, e, f := projectedFixture(t)
	if err := validatePod(e.pod); err != nil {
		t.Fatal(err)
	}
	files, err := p.resolvePodProjection(t.Context(), e, e.pod.Spec.Volumes[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 3 || string(files["namespace"].Data) != e.pod.Namespace || string(files["ca.crt"].Data) != "public-kubernetes-ca" || len(files["token"].Data) == 0 || len(f.requests) != 1 {
		t.Fatal("default automount token/CA/namespace projection incomplete")
	}
	if !strings.Contains(volumeSetupScript(e.pod.Spec.Volumes[0], 768), "mount -t tmpfs") || !strings.Contains(strings.Join(volumeMountArgs(e.pod), " "), "rbind:rprivate:ro") {
		t.Fatal("identity data was not a read-only memory projection")
	}
}

func TestCombinedProjectionMetadataAndOrderedSources(t *testing.T) {
	p, e, _ := projectedFixture(t)
	e.pod.Labels = map[string]string{"z": "last", "a": "first\nquoted\""}
	source := e.pod.Spec.Volumes[0].Projected
	source.DefaultMode = new(int32(0o400))
	source.Sources = append(source.Sources, corev1.VolumeProjection{DownwardAPI: &corev1.DownwardAPIProjection{Items: []corev1.DownwardAPIVolumeFile{
		{Path: "labels", FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.labels"}},
		{Path: "nested/uid", FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.uid"}, Mode: new(int32(0o440))},
		{Path: "ca.crt", FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.labels['z']"}},
	}}})
	files, err := p.resolvePodProjection(t.Context(), e, e.pod.Spec.Volumes[0])
	if err != nil {
		t.Fatal(err)
	}
	if string(files["labels"].Data) != "a=\"first\\nquoted\\\"\"\nz=\"last\"" || string(files["nested/uid"].Data) != string(e.pod.UID) || files["nested/uid"].Mode != 0o440 || files["token"].Mode != 0o400 || string(files["ca.crt"].Data) != "last" {
		t.Fatal("projected metadata, mode or source ordering changed")
	}
	e.pod.Labels["z"] = "updated"
	fresh, err := p.resolvePodProjection(t.Context(), e, e.pod.Spec.Volumes[0])
	if err != nil || string(fresh["ca.crt"].Data) != "updated" {
		t.Fatal("DownwardAPI did not refresh current Pod metadata")
	}
}

func TestCombinedProjectionRejectsUnsafeAndUnsupportedFields(t *testing.T) {
	for name, mutate := range map[string]func(*corev1.Volume){
		"ambiguous": func(v *corev1.Volume) { v.Projected.Sources[0].ConfigMap = &corev1.ConfigMapProjection{} },
		"empty":     func(v *corev1.Volume) { v.Projected.Sources = nil },
		"trust bundle": func(v *corev1.Volume) {
			v.Projected.Sources = []corev1.VolumeProjection{{ClusterTrustBundle: &corev1.ClusterTrustBundleProjection{}}}
		},
		"token traversal": func(v *corev1.Volume) { v.Projected.Sources[0].ServiceAccountToken.Path = "../token" },
		"short lifetime":  func(v *corev1.Volume) { v.Projected.Sources[0].ServiceAccountToken.ExpirationSeconds = new(int64(599)) },
		"token owner":     func(v *corev1.Volume) { v.Projected.Sources[0].ServiceAccountToken.User = new(int64(123)) },
		"mode":            func(v *corev1.Volume) { v.Projected.DefaultMode = new(int32(0o4755)) },
		"field api":       func(v *corev1.Volume) { v.Projected.Sources[2].DownwardAPI.Items[0].FieldRef.APIVersion = "v2" },
		"field host": func(v *corev1.Volume) {
			v.Projected.Sources[2].DownwardAPI.Items[0].FieldRef.FieldPath = "status.hostIP"
		},
		"field resource": func(v *corev1.Volume) {
			v.Projected.Sources[2].DownwardAPI.Items[0].ResourceFieldRef = &corev1.ResourceFieldSelector{Resource: "limits.cpu"}
		},
	} {
		t.Run(name, func(t *testing.T) {
			v := defaultAccountVolume()
			mutate(&v)
			if err := validateVolume(v, 768); err == nil {
				t.Fatal("invalid projected volume accepted")
			}
		})
	}
}

func TestProjectedTokenTransportAndJournalNeverContainCredentials(t *testing.T) {
	p, e, f := projectedFixture(t)
	var arguments []string
	var payload []byte
	p.runner.runtime = stubNativeRuntime{execute: func(ctx context.Context, args []string, input []byte) (msbruntime.Result, error) {
		arguments = append(arguments, args...)
		payload = bytes.Clone(input)
		return msbruntime.Result{}, nil
	}}
	if err := p.refreshVolumes(t.Context(), e, true); err != nil {
		t.Fatal(err)
	}
	var token string
	for _, entry := range e.tokens.entries {
		token = entry.token
	}
	if token == "" || !bytes.Contains(payload, []byte(token)) || strings.Contains(strings.Join(arguments, " "), token) {
		t.Fatal("token was not transferred exclusively over stdin")
	}
	if err := p.journal(e); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(p.runner.cfg.state, string(e.pod.UID)+".json"))
	if err != nil || bytes.Contains(data, []byte(token)) {
		t.Fatal("Pod journal contains token bytes")
	}
	first := bytes.Clone(payload)
	f.now = f.now.Add(49 * time.Minute)
	if err := p.refreshVolumes(t.Context(), e, false); err != nil || bytes.Equal(payload, first) {
		t.Fatal("awake token rotation did not update atomic projection")
	}
	f.failure = errors.New("private-credential-response")
	f.now = f.now.Add(time.Hour)
	if err := p.refreshVolumes(t.Context(), e, false); err != nil {
		t.Fatal(err)
	}
	if err := p.preflightWakeTokens(t.Context(), e); err == nil || strings.Contains(err.Error(), "private-credential-response") {
		t.Fatal("wake did not reject unavailable identity before restore or leaked error")
	}
	if err := p.refreshVolumeProjections(t.Context(), e, false, true); err == nil {
		t.Fatal("wake admitted stale identity despite failed required refresh")
	}
}

func TestCombinedProjectionRejectsDirectoryFileCollision(t *testing.T) {
	p, e, _ := projectedFixture(t)
	e.pod.Spec.Volumes[0].Projected.Sources[2].DownwardAPI.Items[0].Path = "token/nested"
	if _, err := p.resolvePodProjection(t.Context(), e, e.pod.Spec.Volumes[0]); err == nil {
		t.Fatal("projected file was also accepted as directory")
	}
}

func TestInitialIdentityRefreshesAfterLongImagePull(t *testing.T) {
	p, e, f := projectedFixture(t)
	e.started = time.Time{}
	projections, err := p.readVolumeSources(t.Context(), e)
	if err != nil {
		t.Fatal(err)
	}
	e.initialVolumes = projections
	old := bytes.Clone(projections["kube-api-access"]["token"].Data)
	f.now = f.now.Add(2 * time.Hour)
	var payload []byte
	p.runner.runtime = stubNativeRuntime{execute: func(_ context.Context, _ []string, input []byte) (msbruntime.Result, error) {
		if len(input) > 0 {
			payload = bytes.Clone(input)
		}
		return msbruntime.Result{}, nil
	}}
	if err := p.prepareVolumes(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	if len(f.requests) != 2 || bytes.Contains(payload, old) || e.initialVolumes != nil {
		t.Fatal("initial image pull retained expired identity or preflight credentials")
	}
}

func TestSleepingIdentityFailureDoesNotRestoreOrConsumeGeneration(t *testing.T) {
	p, e, f := projectedFixture(t)
	e.state, e.generation, e.snapshots = "SLEEPING", 7, []string{"owned:snapshot"}
	e.freeze = frozenFixture()
	f.failure = errors.New("private-response")
	var commands [][]string
	p.runner.runtime = stubNativeRuntime{execute: func(_ context.Context, args []string, _ []byte) (msbruntime.Result, error) {
		commands = append(commands, args)
		var value any
		switch args[0] {
		case "ls":
			value = []map[string]string{{"name": e.vm}}
		case "inspect":
			value = map[string]any{"status": "Stopped", "config": map[string]any{"labels": map[string]string{ownerLabel: p.runner.cfg.peerOwner, "kubernetes.io/pod-uid": string(e.pod.UID)}}}
		default:
			t.Fatalf("native mutation or snapshot access before identity preflight: %s", args[0])
		}
		data, err := json.Marshal(value)
		return msbruntime.Result{Stdout: string(data)}, err
	}}
	if err := p.resume(t.Context(), e); err == nil || strings.Contains(err.Error(), "private-response") {
		t.Fatal("identity failure did not block wake safely")
	}
	if len(f.requests) != 1 || len(commands) != 2 || e.state != "SLEEPING" || e.generation != 7 || e.allocated {
		t.Fatal("failed identity preflight changed sleeping runtime identity")
	}
}

func TestRecoveredIdentityRequiresFreshProjectionOnlyForAwakePods(t *testing.T) {
	for _, state := range []string{"COLD", "SLEEPING", "INITIALIZING", "RUNNING", "READY"} {
		t.Run(state, func(t *testing.T) {
			p, e, f := projectedFixture(t)
			e.state = state
			f.failure = errors.New("unavailable")
			err := p.refreshRecoveredTokens(t.Context(), e)
			awake := state != "COLD" && state != "SLEEPING"
			if (err != nil) != awake || (len(f.requests) != 0) != awake {
				t.Fatal("recovery admitted stale awake identity or issued tokens for a sleeping Pod")
			}
		})
	}
}
