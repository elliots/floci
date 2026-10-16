package main

import (
	"archive/tar"
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestProjectionPreservesBinaryContentModesAndSafePaths(t *testing.T) {
	files := map[string]volumeFile{
		"nested/binary": {Data: []byte{0, 255, '\r', '\n', 128}, Mode: 0o400},
		".hidden":       {Data: []byte("hidden"), Mode: 0},
	}
	data, err := projectionArchive(files)
	if err != nil {
		t.Fatal(err)
	}
	reader := tar.NewReader(bytes.NewReader(data))
	seen := make(map[string]bool)
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if header.Typeflag == tar.TypeDir {
			continue
		}
		content, err := io.ReadAll(reader)
		if err != nil || !bytes.Equal(content, files[header.Name].Data) || header.Mode != int64(files[header.Name].Mode) {
			t.Fatalf("projection changed file %s: %v", header.Name, err)
		}
		seen[header.Name] = true
	}
	if len(seen) != len(files) {
		t.Fatal("missing projected file")
	}
	for _, name := range []string{"../escape", "/absolute", "..data", "a/../b"} {
		if _, err := projectionArchive(map[string]volumeFile{name: {Mode: 0o644}}); err == nil {
			t.Fatalf("accepted unsafe archive path %s", name)
		}
	}
}

func TestProjectionUpdatesUseStdinAndRetainLastGoodData(t *testing.T) {
	p, e := probeFixture(t)
	e.vm = "owned-pod"
	e.pod.Namespace = "test"
	e.pod.Spec.Volumes = []corev1.Volume{{Name: "secret", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "credentials"}}}}
	e.pod.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "secret", MountPath: "/credentials"}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "test"}, Data: map[string][]byte{"password": []byte("never-put-secret-in-argv")}}
	client := fake.NewClientset(secret)
	p.client = client
	arguments, payload := filepath.Join(p.runner.cfg.state, "argv"), filepath.Join(p.runner.cfg.state, "payload")
	stub := "#!/bin/sh\nprintf '%s\\n' \"$*\" >>" + quote(arguments) + "\ncat >" + quote(payload) + "\n"
	if err := os.WriteFile(p.runner.cfg.msb, []byte(stub), 0700); err != nil {
		t.Fatal(err)
	}
	if err := p.refreshVolumes(t.Context(), e, true); err != nil {
		t.Fatal(err)
	}
	argv, err := os.ReadFile(arguments)
	if err != nil || bytes.Contains(argv, secret.Data["password"]) || !bytes.Contains(argv, []byte("--stream")) {
		t.Fatalf("secret transfer was not private: %v", err)
	}
	initial, err := os.ReadFile(payload)
	if err != nil || !bytes.Contains(initial, secret.Data["password"]) {
		t.Fatal("secret missing from stdin payload")
	}
	if err := p.refreshVolumes(t.Context(), e, true); err != nil {
		t.Fatal(err)
	}
	unchanged, _ := os.ReadFile(arguments)
	if !bytes.Equal(argv, unchanged) {
		t.Fatal("unchanged projection was rewritten")
	}
	secret.Data["password"] = []byte("rotated-credential")
	if _, err := client.CoreV1().Secrets("test").Update(t.Context(), secret, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := p.refreshVolumes(t.Context(), e, false); err != nil {
		t.Fatal(err)
	}
	updated, _ := os.ReadFile(payload)
	if bytes.Equal(initial, updated) || !bytes.Contains(updated, secret.Data["password"]) {
		t.Fatal("secret rotation not applied")
	}
	if err := client.CoreV1().Secrets("test").Delete(t.Context(), secret.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := p.refreshVolumes(t.Context(), e, false); err != nil {
		t.Fatal(err)
	}
	retained, _ := os.ReadFile(payload)
	if !bytes.Equal(updated, retained) {
		t.Fatal("missing required source destroyed last good data")
	}
	if err := p.refreshVolumes(t.Context(), e, true); err == nil {
		t.Fatal("initial required source absence accepted")
	}
}

func TestMissingVolumeSourcesWaitWithoutCreatingAGuest(t *testing.T) {
	p, e := probeFixture(t)
	p.client = fake.NewClientset()
	e.pod.Spec.Volumes = []corev1.Volume{{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "missing"}}}}}
	if _, err := p.readVolumeSources(t.Context(), e); err != nil {
		t.Fatal("unused missing volume blocked startup")
	}
	e.pod.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "config", MountPath: "/config"}}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := p.waitForVolumeSources(ctx, e); err == nil {
		t.Fatal("missing source did not wait")
	}
	if e.pod.Status.Reason != "FailedMount" || e.created || e.allocated {
		t.Fatal("missing source was allowed to allocate a VM")
	}
}

func TestVolumeMountsEnforceProjectionReadOnly(t *testing.T) {
	pod := validPod()
	pod.Spec.Volumes = []corev1.Volume{
		{Name: "data", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		{Name: "secret", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "credentials"}}},
	}
	pod.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "data", MountPath: "/data"}, {Name: "secret", MountPath: "/secret"}}
	args := volumeMountArgs(pod)
	if !strings.HasSuffix(args[1], ":rw") || !strings.HasSuffix(args[3], ":ro") {
		t.Fatalf("incorrect mount permissions: %v", args)
	}
}

func TestProjectionAtomicReplacementAndKeyRemoval(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("guest writer requires Linux mv; covered by native VM qualification on macOS")
	}
	root := t.TempDir()
	write := func(generation string, files map[string]volumeFile) {
		t.Helper()
		data, err := projectionArchive(files)
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.CommandContext(t.Context(), "sh", "-ec", projectionScript(root, "..floci-"+generation, files))
		cmd.Stdin = bytes.NewReader(data)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("write projection: %v: %s", err, output)
		}
	}
	write("first", map[string]volumeFile{"nested/key": {Data: []byte("old"), Mode: 0o640}, ".removed": {Mode: 0o400}})
	open, err := os.Open(filepath.Join(root, "nested/key"))
	if err != nil {
		t.Fatal(err)
	}
	defer open.Close()
	write("second", map[string]volumeFile{"nested/key": {Data: []byte("new"), Mode: 0o600}, "-option": {Mode: 0o444}})
	old, _ := io.ReadAll(open)
	current, _ := os.ReadFile(filepath.Join(root, "nested/key"))
	info, _ := os.Stat(filepath.Join(root, "nested/key"))
	if string(old) != "old" || string(current) != "new" || info.Mode().Perm() != 0o600 {
		t.Fatal("projection rewrote an open file or lost new bytes/mode")
	}
	if _, err := os.Lstat(filepath.Join(root, ".removed")); !os.IsNotExist(err) {
		t.Fatal("removed key retained a dangling link")
	}
	write("empty", map[string]volumeFile{})
	if _, err := os.Lstat(filepath.Join(root, "nested")); !os.IsNotExist(err) {
		t.Fatal("empty optional projection retained old keys")
	}
}

func TestStrictStartupRechecksNetworkPolicyAfterWaitingForVolumes(t *testing.T) {
	p, e := probeFixture(t)
	e.state, e.started = "WAITINGVOLUMES", time.Time{}
	e.lifetime, e.cancel = context.WithTimeout(t.Context(), 15*time.Second)
	defer e.cancel()
	e.pod.Labels = map[string]string{"role": "isolated"}
	e.pod.Status = corev1.PodStatus{Phase: corev1.PodPending, ContainerStatuses: []corev1.ContainerStatus{{Name: "app"}}}
	e.pod.Spec.Volumes = []corev1.Volume{{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "settings"}}}}}
	e.pod.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "config", MountPath: "/config"}}
	client := fake.NewClientset(e.pod.DeepCopy())
	p.client = client
	p.starts = make(chan struct{}, 1)
	p.runner.cfg.maxActiveMemoryMiB = vmMemoryMiB
	if err := p.checkCurrentPod(t.Context(), e); err != nil {
		t.Fatalf("initial Pod admission failed: %v", err)
	}
	calls := filepath.Join(p.runner.cfg.state, "runtime-calls")
	if err := os.WriteFile(p.runner.cfg.msb, []byte("#!/bin/sh\nprintf '%s\\n' \"$*\" >>"+quote(calls)+"\nexit 1\n"), 0700); err != nil {
		t.Fatal(err)
	}
	waited := false
	p.notify = func(pod *corev1.Pod) {
		if pod.Status.Reason != "FailedMount" || waited {
			return
		}
		waited = true
		policy := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: "deny", Namespace: pod.Namespace},
			Spec: networkingv1.NetworkPolicySpec{PodSelector: metav1.LabelSelector{MatchLabels: pod.Labels}}}
		if _, err := client.NetworkingV1().NetworkPolicies(pod.Namespace).Create(t.Context(), policy, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		source := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "settings", Namespace: pod.Namespace}, Data: map[string]string{"key": "value"}}
		if _, err := client.CoreV1().ConfigMaps(pod.Namespace).Create(t.Context(), source, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	err := p.startStrictPod(e)
	if !waited || err == nil || !strings.Contains(err.Error(), "NetworkPolicy") {
		t.Fatalf("startup ignored a policy added while waiting for its source: waited=%t error=%v", waited, err)
	}
	if e.created || e.allocated || p.activeMemoryMiB != 0 || len(p.starts) != 0 {
		t.Fatal("policy rejection allocated a VM, memory or a start permit")
	}
	if _, err := os.Stat(calls); !os.IsNotExist(err) {
		t.Fatalf("policy rejection invoked the runtime: %v", err)
	}
}

func TestInitialProjectionSurvivesSourceDeletionAfterPreflight(t *testing.T) {
	for _, kind := range []string{"ConfigMap", "Secret"} {
		t.Run(kind, func(t *testing.T) {
			p, e := probeFixture(t)
			e.started = time.Time{}
			e.vm = "owned-pod"
			client := fake.NewClientset()
			p.client = client
			data := []byte{0, 255, '\r', '\n', 128}
			metadata := metav1.ObjectMeta{Name: "settings", Namespace: e.pod.Namespace}
			volume := corev1.Volume{Name: "config"}
			if kind == "ConfigMap" {
				volume.ConfigMap = &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: metadata.Name}, DefaultMode: new(int32(0o400))}
				if _, err := client.CoreV1().ConfigMaps(metadata.Namespace).Create(t.Context(), &corev1.ConfigMap{ObjectMeta: metadata, BinaryData: map[string][]byte{"binary": data}}, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			} else {
				volume.Secret = &corev1.SecretVolumeSource{SecretName: metadata.Name, DefaultMode: new(int32(0o400))}
				if _, err := client.CoreV1().Secrets(metadata.Namespace).Create(t.Context(), &corev1.Secret{ObjectMeta: metadata, Data: map[string][]byte{"binary": data}}, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			e.pod.Spec.Volumes = []corev1.Volume{volume}
			e.pod.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: volume.Name, MountPath: "/config"}}
			if err := p.waitForVolumeSources(t.Context(), e); err != nil {
				t.Fatal(err)
			}
			if kind == "ConfigMap" {
				if err := client.CoreV1().ConfigMaps(metadata.Namespace).Delete(t.Context(), metadata.Name, metav1.DeleteOptions{}); err != nil {
					t.Fatal(err)
				}
			} else if err := client.CoreV1().Secrets(metadata.Namespace).Delete(t.Context(), metadata.Name, metav1.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
			client.ClearActions()
			payload := filepath.Join(p.runner.cfg.state, "payload")
			stub := "#!/bin/sh\ncase \" $* \" in *\" --stream \"*) cat >" + quote(payload) + ";; esac\n"
			if err := os.WriteFile(p.runner.cfg.msb, []byte(stub), 0700); err != nil {
				t.Fatal(err)
			}
			e.op.Lock()
			err := p.prepareVolumes(t.Context(), e)
			e.op.Unlock()
			if err != nil {
				t.Fatalf("initial projection depended on a deleted source: %v", err)
			}
			if actions := client.Actions(); len(actions) != 0 {
				t.Fatalf("initial projection refetched its source: %v", actions)
			}
			archive, err := os.ReadFile(payload)
			if err != nil {
				t.Fatal(err)
			}
			reader := tar.NewReader(bytes.NewReader(archive))
			header, err := reader.Next()
			if err != nil {
				t.Fatal(err)
			}
			content, err := io.ReadAll(reader)
			if err != nil || header.Name != "binary" || header.Mode != 0o400 || !bytes.Equal(content, data) {
				t.Fatalf("initial projection lost its captured bytes or mode: header=%+v content=%v error=%v", header, content, err)
			}
			if e.initialVolumes != nil {
				t.Fatal("initial source bytes remained retained after projection")
			}
		})
	}
}
