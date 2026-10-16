package main

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func volumeSpecPod() *corev1.Pod {
	pod := validPod()
	pod.Spec.Volumes = []corev1.Volume{
		{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "settings"}}}},
		{Name: "secret", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "credentials"}}},
		{Name: "scratch", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		{Name: "memory", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory, SizeLimit: new(resource.MustParse("32Mi"))}}},
	}
	pod.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{
		{Name: "config", MountPath: "/etc/config"}, {Name: "secret", MountPath: "/etc/credentials"},
		{Name: "scratch", MountPath: "/scratch"}, {Name: "memory", MountPath: "/cache"},
	}
	return pod
}

func TestValidateVolumesAcceptsSupportedSourcesAndDefaultOptions(t *testing.T) {
	for _, pod := range []*corev1.Pod{validPod(), volumeSpecPod()} {
		if err := validateVolumes(pod); err != nil {
			t.Fatal(err)
		}
	}
	pod := volumeSpecPod()
	pod.Spec.Containers[0].VolumeMounts[0].MountPropagation = new(corev1.MountPropagationNone)
	pod.Spec.Containers[0].VolumeMounts[0].RecursiveReadOnly = new(corev1.RecursiveReadOnlyDisabled)
	pod.Spec.Volumes[0].ConfigMap.Items = []corev1.KeyToPath{{Key: "config", Path: "nested/app.conf", Mode: new(int32(0))}}
	pod.Spec.Volumes[0].ConfigMap.DefaultMode = new(int32(0o777))
	pod.Spec.Volumes[3].EmptyDir.SizeLimit = new(resource.MustParse("768Mi"))
	if err := validateVolumes(pod); err != nil {
		t.Fatal(err)
	}
	pod.Spec.Volumes[3].EmptyDir.SizeLimit = nil
	if err := validateVolumes(pod); err != nil {
		t.Fatal(err)
	}
	if got := guestVolumePath("scratch"); got != "/var/lib/floci-volumes/scratch" {
		t.Fatalf("unexpected guest volume path %q", got)
	}
}

func TestMemoryVolumeValidationUsesThePodGuestAllocation(t *testing.T) {
	pod := volumeSpecPod()
	pod.Spec.Containers[0].Resources.Requests = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")}
	pod.Spec.Volumes[3].EmptyDir.SizeLimit = new(resource.MustParse("1Gi"))
	if err := validateVolumes(pod); err != nil {
		t.Fatalf("larger guest rejected a fitting memory volume: %v", err)
	}
	pod.Spec.Volumes[3].EmptyDir.SizeLimit = new(resource.MustParse("1281Mi"))
	if err := validateVolumes(pod); err == nil {
		t.Fatal("memory volume larger than its guest was accepted")
	}
}

func TestValidateVolumesRejectsUnsupportedOrUnsafeSpecs(t *testing.T) {
	scenarios := map[string]func(*corev1.Pod){
		"invalid volume name":    func(p *corev1.Pod) { p.Spec.Volumes[0].Name = "../outside" },
		"duplicate volume":       func(p *corev1.Pod) { p.Spec.Volumes = append(p.Spec.Volumes, p.Spec.Volumes[0]) },
		"missing source":         func(p *corev1.Pod) { p.Spec.Volumes[0].VolumeSource = corev1.VolumeSource{} },
		"host path":              func(p *corev1.Pod) { p.Spec.Volumes[0].HostPath = &corev1.HostPathVolumeSource{Path: "/"} },
		"projected":              func(p *corev1.Pod) { p.Spec.Volumes[0].Projected = &corev1.ProjectedVolumeSource{} },
		"multiple sources":       func(p *corev1.Pod) { p.Spec.Volumes[0].EmptyDir = &corev1.EmptyDirVolumeSource{} },
		"unsupported medium":     func(p *corev1.Pod) { p.Spec.Volumes[2].EmptyDir.Medium = "HugePages" },
		"disk quota":             func(p *corev1.Pod) { p.Spec.Volumes[2].EmptyDir.SizeLimit = new(resource.MustParse("32Mi")) },
		"emptyDir mode":          func(p *corev1.Pod) { p.Spec.Volumes[2].EmptyDir.Mode = new(int32(0o700)) },
		"memory too large":       func(p *corev1.Pod) { p.Spec.Volumes[3].EmptyDir.SizeLimit = new(resource.MustParse("769Mi")) },
		"memory overflow":        func(p *corev1.Pod) { p.Spec.Volumes[3].EmptyDir.SizeLimit = new(resource.MustParse("100Ei")) },
		"memory zero":            func(p *corev1.Pod) { p.Spec.Volumes[3].EmptyDir.SizeLimit = new(resource.MustParse("0")) },
		"memory negative":        func(p *corev1.Pod) { p.Spec.Volumes[3].EmptyDir.SizeLimit = new(resource.MustParse("-1")) },
		"missing source name":    func(p *corev1.Pod) { p.Spec.Volumes[0].ConfigMap.Name = "" },
		"source name traversal":  func(p *corev1.Pod) { p.Spec.Volumes[1].Secret.SecretName = "../secret" },
		"ConfigMap ownership":    func(p *corev1.Pod) { p.Spec.Volumes[0].ConfigMap.DefaultUser = new(int64(1000)) },
		"Secret ownership":       func(p *corev1.Pod) { p.Spec.Volumes[1].Secret.DefaultUser = new(int64(1000)) },
		"negative mode":          func(p *corev1.Pod) { p.Spec.Volumes[0].ConfigMap.DefaultMode = new(int32(-1)) },
		"setuid mode":            func(p *corev1.Pod) { p.Spec.Volumes[1].Secret.DefaultMode = new(int32(0o4755)) },
		"missing mounted volume": func(p *corev1.Pod) { p.Spec.Containers[0].VolumeMounts[0].Name = "missing" },
		"unsafe subPath":         func(p *corev1.Pod) { p.Spec.Containers[0].VolumeMounts[0].SubPath = "../app.conf" },
		"subPathExpr":            func(p *corev1.Pod) { p.Spec.Containers[0].VolumeMounts[0].SubPathExpr = "$(POD_NAME)" },
		"mount propagation": func(p *corev1.Pod) {
			p.Spec.Containers[0].VolumeMounts[0].MountPropagation = new(corev1.MountPropagationBidirectional)
		},
		"recursive read only": func(p *corev1.Pod) {
			p.Spec.Containers[0].VolumeMounts[0].RecursiveReadOnly = new(corev1.RecursiveReadOnlyEnabled)
		},
		"bind options": func(p *corev1.Pod) { p.Spec.Containers[0].VolumeMounts[0].BindMountOptions = []string{"noexec"} },
		"volume device": func(p *corev1.Pod) {
			p.Spec.Containers[0].VolumeDevices = []corev1.VolumeDevice{{Name: "scratch", DevicePath: "/dev/data"}}
		},
		"duplicate mount": func(p *corev1.Pod) { p.Spec.Containers[0].VolumeMounts[1].MountPath = "/etc/config" },
		"nested mount":    func(p *corev1.Pod) { p.Spec.Containers[0].VolumeMounts[1].MountPath = "/etc/config/subdir" },
	}
	for name, mutate := range scenarios {
		t.Run(name, func(t *testing.T) {
			pod := volumeSpecPod()
			mutate(pod)
			if err := validateVolumes(pod); err == nil {
				t.Fatal("unsafe or unsupported volume was accepted")
			}
		})
	}
	for _, value := range []string{"", "/", "relative", "/data/", "/data/../tmp", "/data//tmp", "/data,options=rw", "/data\"quoted", "/data\n", "/data\x00", "/etc", "/etc/resolv.conf", "/proc", "/sys/config", "/dev/extra"} {
		t.Run("mount path "+value, func(t *testing.T) {
			pod := volumeSpecPod()
			pod.Spec.Containers[0].VolumeMounts[0].MountPath = value
			if err := validateVolumes(pod); err == nil {
				t.Fatalf("mount path %q was accepted", value)
			}
		})
	}
}

func TestValidateProjectionPathsAndModes(t *testing.T) {
	for _, value := range []string{"", ".", "..", "../out", "/absolute", "a/../b", "a//b", "a/./b", "a/", "..data", "a/..generation/key", "a\x00b", "a\nb"} {
		t.Run(value, func(t *testing.T) {
			pod := volumeSpecPod()
			pod.Spec.Volumes[0].ConfigMap.Items = []corev1.KeyToPath{{Key: "key", Path: value}}
			if err := validateVolumes(pod); err == nil {
				t.Fatalf("unsafe projected path %q was accepted", value)
			}
		})
	}
	for _, items := range [][]corev1.KeyToPath{
		{{Key: "first", Path: "same"}, {Key: "second", Path: "same"}},
		{{Key: "first", Path: "a"}, {Key: "second", Path: "a-b"}, {Key: "third", Path: "a/b/c"}},
		{{Key: "first", Path: "a/b/c"}, {Key: "second", Path: "a"}},
		{{Key: "../invalid", Path: "file"}},
		{{Key: "valid", Path: "file", Mode: new(int32(0o1000))}},
		{{Key: "valid", Path: "file", User: new(int64(1000))}},
	} {
		pod := volumeSpecPod()
		pod.Spec.Volumes[1].Secret.Items = items
		if err := validateVolumes(pod); err == nil {
			t.Fatalf("invalid projection items were accepted: %#v", items)
		}
	}
}

func TestResolveConfigMapPreservesBinaryDataAndProjectionModes(t *testing.T) {
	config := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "settings", Namespace: "default"},
		Data: map[string]string{"text": "hello\n", "unused": "excluded"}, BinaryData: map[string][]byte{"binary": {0, 0xff, 0x80, '\n'}}}
	client := fake.NewClientset(config)
	volume := volumeSpecPod().Spec.Volumes[0]
	files, err := resolveProjectedVolume(t.Context(), client, "default", volume)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]volumeFile{
		"text": {Data: []byte("hello\n"), Mode: 0o644}, "unused": {Data: []byte("excluded"), Mode: 0o644},
		"binary": {Data: []byte{0, 0xff, 0x80, '\n'}, Mode: 0o644},
	}
	if !reflect.DeepEqual(files, want) {
		t.Fatalf("unexpected files: %#v", files)
	}
	volume.ConfigMap.DefaultMode = new(int32(0o440))
	volume.ConfigMap.Items = []corev1.KeyToPath{{Key: "text", Path: "nested/app.conf", Mode: new(int32(0))}, {Key: "binary", Path: ".binary"}}
	files, err = resolveProjectedVolume(t.Context(), client, "default", volume)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 || files["nested/app.conf"].Mode != 0 || files[".binary"].Mode != 0o440 || !bytes.Equal(files[".binary"].Data, config.BinaryData["binary"]) {
		t.Fatalf("projection selection or modes were lost: %#v", files)
	}
	files[".binary"].Data[0] = 42
	stored, err := client.CoreV1().ConfigMaps("default").Get(t.Context(), "settings", metav1.GetOptions{})
	if err != nil || stored.BinaryData["binary"][0] != 0 {
		t.Fatalf("projection mutated source: %v", err)
	}
}

func TestResolveSecretRetainsBinaryDataAndRefreshesSource(t *testing.T) {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "default"}, Data: map[string][]byte{"password": {0xff, 0, '\n'}}}
	client := fake.NewClientset(secret)
	volume := volumeSpecPod().Spec.Volumes[1]
	volume.Secret.DefaultMode = new(int32(0o400))
	volume.Secret.Items = []corev1.KeyToPath{{Key: "password", Path: "nested/credential"}}
	old, err := resolveProjectedVolume(t.Context(), client, "default", volume)
	if err != nil || old["nested/credential"].Mode != 0o400 || !bytes.Equal(old["nested/credential"].Data, secret.Data["password"]) {
		t.Fatalf("secret projection failed: %v", err)
	}
	secret.Data["password"] = []byte("updated")
	if _, err := client.CoreV1().Secrets("default").Update(t.Context(), secret, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	fresh, err := resolveProjectedVolume(t.Context(), client, "default", volume)
	if err != nil || string(fresh["nested/credential"].Data) != "updated" || !bytes.Equal(old["nested/credential"].Data, []byte{0xff, 0, '\n'}) {
		t.Fatalf("refresh mutated prior data or missed new data: %v", err)
	}
	if _, err := resolveProjectedVolume(t.Context(), client, "other-namespace", volume); !apierrors.IsNotFound(err) {
		t.Fatalf("namespace was not respected: %v", err)
	}
}

func TestResolveProjectedVolumeOptionalOnlySuppressesMissingObjectsAndKeys(t *testing.T) {
	for _, kind := range []string{"ConfigMap", "Secret"} {
		t.Run(kind, func(t *testing.T) {
			volume := volumeSpecPod().Spec.Volumes[0]
			resourceName := "configmaps"
			if kind == "Secret" {
				volume = volumeSpecPod().Spec.Volumes[1]
				resourceName = "secrets"
			}
			client := fake.NewClientset()
			if _, err := resolveProjectedVolume(t.Context(), client, "default", volume); !apierrors.IsNotFound(err) {
				t.Fatalf("missing required source did not fail: %v", err)
			}
			if volume.ConfigMap != nil {
				volume.ConfigMap.Optional = new(true)
				volume.ConfigMap.Items = []corev1.KeyToPath{{Key: "missing", Path: "file"}}
			} else {
				volume.Secret.Optional = new(true)
				volume.Secret.Items = []corev1.KeyToPath{{Key: "missing", Path: "file"}}
			}
			files, err := resolveProjectedVolume(t.Context(), client, "default", volume)
			if err != nil || len(files) != 0 {
				t.Fatalf("optional missing source failed: %v", err)
			}
			client = fake.NewClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "settings", Namespace: "default"}},
				&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "default"}})
			files, err = resolveProjectedVolume(t.Context(), client, "default", volume)
			if err != nil || len(files) != 0 {
				t.Fatalf("optional missing key failed: %v", err)
			}
			if volume.ConfigMap != nil {
				volume.ConfigMap.Optional = new(false)
			} else {
				volume.Secret.Optional = new(false)
			}
			if _, err := resolveProjectedVolume(t.Context(), client, "default", volume); err == nil {
				t.Fatal("missing required key was accepted")
			}
			if volume.ConfigMap != nil {
				volume.ConfigMap.Optional = new(true)
			} else {
				volume.Secret.Optional = new(true)
			}
			client.PrependReactor("get", resourceName, func(action k8stesting.Action) (bool, runtime.Object, error) {
				return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: resourceName}, "source", errors.New("denied"))
			})
			if _, err := resolveProjectedVolume(t.Context(), client, "default", volume); !apierrors.IsForbidden(err) {
				t.Fatalf("optional source hid an authorization error: %v", err)
			}
		})
	}
}

func TestResolveProjectedVolumeRejectsMalformedSourcesWithoutLeakingContents(t *testing.T) {
	for _, key := range []string{"../escape", "..data", "nested/file"} {
		volume := volumeSpecPod().Spec.Volumes[1]
		client := fake.NewClientset(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credentials", Namespace: "default"},
			Data: map[string][]byte{key: []byte("secret-content-marker")}})
		_, err := resolveProjectedVolume(t.Context(), client, "default", volume)
		if err == nil || strings.Contains(err.Error(), "secret-content-marker") {
			t.Fatalf("malformed source accepted or exposed content: %v", err)
		}
	}
	volume := volumeSpecPod().Spec.Volumes[0]
	client := fake.NewClientset(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "settings", Namespace: "default"},
		Data: map[string]string{"duplicate": "text"}, BinaryData: map[string][]byte{"duplicate": []byte("binary")}})
	if _, err := resolveProjectedVolume(t.Context(), client, "default", volume); err == nil {
		t.Fatal("duplicate text and binary keys were accepted")
	}
	if _, err := resolveProjectedVolume(t.Context(), nil, "default", volume); err == nil {
		t.Fatal("missing Kubernetes client was accepted")
	}
	if _, err := resolveProjectedVolume(t.Context(), client, "default", volumeSpecPod().Spec.Volumes[2]); err == nil {
		t.Fatal("emptyDir was treated as a projected source")
	}
}
