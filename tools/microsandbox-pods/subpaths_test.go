package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestSubpathExpressionUsesResolvedContainerEnvironment(t *testing.T) {
	container := corev1.Container{Name: "app", Env: []corev1.EnvVar{{Name: "POD_NAME", Value: "orders-123"}, {Name: "DIRECTORY", Value: "nested/data"}, {Name: "LITERAL", Value: "$(MISSING)"}}}
	for expression, want := range map[string]string{
		"$(POD_NAME)/$(DIRECTORY)": "orders-123/nested/data",
		"$$(POD_NAME)":             "$(POD_NAME)",
		"$(LITERAL)":               "$(MISSING)",
		"comma,quote'and\"space ":  "comma,quote'and\"space ",
		"${POD_NAME}":              "${POD_NAME}",
	} {
		got, err := resolveMountSubpath(corev1.VolumeMount{SubPathExpr: expression}, container)
		if err != nil || got != want {
			t.Fatalf("expand %q: got %q, want %q: %v", expression, got, want, err)
		}
	}
	for _, expression := range []string{"$(MISSING)", "$(EMPTY)", "$(POD_NAME)/../escape", "/$(POD_NAME)", "$(DIRECTORY)//extra"} {
		container.Env = append(container.Env, corev1.EnvVar{Name: "EMPTY"})
		if _, err := resolveMountSubpath(corev1.VolumeMount{SubPathExpr: expression}, container); err == nil {
			t.Fatalf("accepted unsafe or undefined subPathExpr %q", expression)
		}
	}
}

func TestSubpathsValidateBeforeAllocatingAGuest(t *testing.T) {
	for _, value := range []string{".", "..", "../outside", "a/../outside", "a/./b", "a//b", "/absolute", "trailing/", "control\n", "nul\x00"} {
		pod := volumeSpecPod()
		pod.Spec.Containers[0].VolumeMounts[0].SubPath = value
		if err := validateVolumes(pod); err == nil {
			t.Fatalf("accepted unsafe subPath %q", value)
		}
	}
	pod := volumeSpecPod()
	container := &pod.Spec.Containers[0]
	container.Env = []corev1.EnvVar{{Name: "FILE", Value: "app.conf"}}
	container.VolumeMounts[0].SubPathExpr = "$(FILE)"
	container.VolumeMounts[1].SubPath = "nested/token"
	if err := validateVolumes(pod); err != nil {
		t.Fatal(err)
	}
	container.VolumeMounts[0].SubPath = "other.conf"
	if err := validateVolumes(pod); err == nil {
		t.Fatal("simultaneous subPath and subPathExpr were accepted")
	}
	container.VolumeMounts[0].SubPath = ""
	container.Env[0].Value = "../sensitive-do-not-log"
	if err := validateVolumes(pod); err == nil || strings.Contains(err.Error(), "sensitive-do-not-log") {
		t.Fatalf("invalid expanded value leaked or was accepted: %v", err)
	}
}

func TestContainerSpecificSubpathsPreserveReadOnlyAndSharedRoots(t *testing.T) {
	pod := volumeSpecPod()
	app := &pod.Spec.Containers[0]
	app.VolumeMounts = []corev1.VolumeMount{{Name: "config", MountPath: "/etc/app.conf", SubPath: "app.conf"}, {Name: "scratch", MountPath: "/data", SubPath: "shared"}}
	init := corev1.Container{Name: "initialize", VolumeMounts: []corev1.VolumeMount{{Name: "secret", MountPath: "/token", SubPath: "token"}, {Name: "scratch", MountPath: "/all"}}}
	pod.Spec.InitContainers = []corev1.Container{init}
	args := volumeMountArgsForContainer(pod, *app)
	if !strings.Contains(args[1], "src="+guestSubpathPath(app.Name, 0)) || !strings.HasSuffix(args[1], ":ro") || !strings.HasSuffix(args[3], ":rw") {
		t.Fatalf("application mount sources or permissions changed: %v", args)
	}
	args = volumeMountArgsForContainer(pod, init)
	if !strings.Contains(args[1], "src="+guestSubpathPath(init.Name, 0)) || !strings.HasSuffix(args[1], ":ro") || !strings.Contains(args[3], "src="+guestVolumePath("scratch")) {
		t.Fatalf("init mount sources or permissions changed: %v", args)
	}
	var names []string
	for _, volume := range mountedVolumes(pod) {
		names = append(names, volume.Name)
	}
	if !reflect.DeepEqual(names, []string{"config", "secret", "scratch"}) {
		t.Fatalf("mounted volumes did not include init-only sources: %v", names)
	}
}

func runSubpathScript(t *testing.T, root, target, subpath string, create bool, beforeMount string) (string, error) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("descriptor paths require Linux; native VM qualification covers macOS")
	}
	// Record what mount(2) would receive without requiring CAP_SYS_ADMIN in CI.
	// A hook may replace a pathname just before binding to verify inode pinning.
	prefix := "mountpoint() { return 1; }; busybox() { test \"$1\" = mount; test \"$2\" = -o; test \"$3\" = bind; " + beforeMount +
		"printf 'source=%s\\n' \"$(readlink \"$4\")\"; if test -f \"$4\"; then cat \"$4\"; fi; }; "
	script := strings.ReplaceAll(subpathSetupScript(root, target, subpath, create), "/bin/busybox ", "busybox ")
	output, err := exec.CommandContext(t.Context(), "sh", "-ec", prefix+script).CombinedOutput()
	return string(output), err
}

func TestSubpathPinsFilesAndDirectoriesAndCreatesMissingEmptyDirPaths(t *testing.T) {
	for _, scenario := range []struct {
		name, subpath string
		create, file  bool
	}{
		{name: "emptyDir nested creation", subpath: "missing/nested", create: true},
		{name: "emptyDir existing file", subpath: "file", create: true, file: true},
		{name: "projected file", subpath: "nested/file", file: true},
		{name: "projected directory", subpath: "nested"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "volume")
			target := filepath.Join(t.TempDir(), "pins", "app", "0")
			if err := os.Mkdir(root, 0755); err != nil {
				t.Fatal(err)
			}
			if scenario.create && scenario.file {
				if err := os.WriteFile(filepath.Join(root, "file"), []byte("pinned-bytes"), 0640); err != nil {
					t.Fatal(err)
				}
			} else if !scenario.create {
				if err := os.MkdirAll(filepath.Join(root, "..floci-first", "nested"), 0755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(root, "..floci-first", "nested", "file"), []byte("pinned-bytes"), 0440); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink("..floci-first/nested", filepath.Join(root, "nested")); err != nil {
					t.Fatal(err)
				}
			}
			output, err := runSubpathScript(t, root, target, scenario.subpath, scenario.create, "")
			if err != nil {
				t.Fatalf("prepare: %v: %s", err, output)
			}
			info, err := os.Stat(target)
			if err != nil || info.IsDir() == scenario.file {
				t.Fatalf("staging target has wrong type: %v", err)
			}
			if scenario.file && !strings.Contains(output, "pinned-bytes") {
				t.Fatalf("mounted descriptor lost file contents: %s", output)
			}
			if !scenario.create {
				generation, err := os.Readlink(target + ".generation")
				if err != nil || generation != filepath.Join(root, "..floci-first") {
					t.Fatalf("projection generation was not pinned: %s: %v", generation, err)
				}
			}
		})
	}
}

func TestSubpathRejectsSymlinkEscapesAndMissingProjectionSources(t *testing.T) {
	for _, scenario := range []struct {
		name, subpath string
		create        bool
	}{
		{name: "outside file", subpath: "escape", create: true},
		{name: "outside directory", subpath: "outside/new", create: true},
		{name: "prefix sibling", subpath: "sibling", create: true},
		{name: "dangling symlink", subpath: "dangling/new", create: true},
		{name: "missing projection", subpath: "missing"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			base := t.TempDir()
			root, outside := filepath.Join(base, "volume"), filepath.Join(base, "volume-other")
			if err := os.MkdirAll(root, 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(outside, 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(outside, "file"), []byte("outside-secret"), 0600); err != nil {
				t.Fatal(err)
			}
			for name, destination := range map[string]string{"escape": filepath.Join(outside, "file"), "outside": outside, "sibling": filepath.Join(outside, "file"), "dangling": filepath.Join(outside, "absent")} {
				if err := os.Symlink(destination, filepath.Join(root, name)); err != nil {
					t.Fatal(err)
				}
			}
			output, err := runSubpathScript(t, root, filepath.Join(base, "pins", "app", "0"), scenario.subpath, scenario.create, "")
			if err == nil || strings.Contains(output, "source=") || strings.Contains(output, "outside-secret") {
				t.Fatalf("unsafe subPath reached mount: %s: %v", output, err)
			}
			if _, err := os.Stat(filepath.Join(outside, "new")); !os.IsNotExist(err) {
				t.Fatalf("directory created outside volume: %v", err)
			}
		})
	}
}

func TestSubpathOpenedDescriptorSurvivesPathReplacement(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "volume")
	if err := os.Mkdir(root, 0755); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(root, "file")
	if err := os.WriteFile(file, []byte("old-inode"), 0600); err != nil {
		t.Fatal(err)
	}
	hook := "mv " + quote(file) + " " + quote(file+".old") + "; printf replacement >" + quote(file) + "; "
	output, err := runSubpathScript(t, root, filepath.Join(base, "pins", "app", "0"), "file", true, hook)
	if err != nil || !strings.Contains(output, "old-inode") || strings.Contains(output, "replacement") {
		t.Fatalf("bind mount re-resolved the pathname: %s: %v", output, err)
	}
}

func TestProjectionRefreshPreservesPinnedDirectoryGeneration(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("guest projection writer requires Linux mv")
	}
	root, pins := t.TempDir(), t.TempDir()
	write := func(generation, content string) {
		t.Helper()
		files := map[string]volumeFile{"nested/file": {Data: []byte(content), Mode: 0440}}
		archive, err := projectionArchive(files)
		if err != nil {
			t.Fatal(err)
		}
		script := strings.ReplaceAll(projectionScript(root, generation, files), guestSubpathsRoot, pins)
		cmd := exec.CommandContext(t.Context(), "sh", "-ec", script)
		cmd.Stdin = bytes.NewReader(archive)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("project generation: %v: %s", err, output)
		}
	}
	write("..floci-first", "old")
	containerPins := filepath.Join(pins, "app")
	if err := os.Mkdir(containerPins, 0700); err != nil {
		t.Fatal(err)
	}
	pin := filepath.Join(containerPins, "0.generation")
	if err := os.Symlink(filepath.Join(root, "..floci-first"), pin); err != nil {
		t.Fatal(err)
	}
	write("..floci-second", "updated")
	old, err := os.ReadFile(filepath.Join(root, "..floci-first", "nested", "file"))
	if err != nil || string(old) != "old" {
		t.Fatalf("refresh deleted a pinned directory's contents: %s: %v", old, err)
	}
	current, err := os.ReadFile(filepath.Join(root, "nested", "file"))
	if err != nil || string(current) != "updated" {
		t.Fatalf("normal projection did not refresh: %s: %v", current, err)
	}
	if err := os.Remove(pin); err != nil {
		t.Fatal(err)
	}
	write("..floci-third", "latest")
	for _, generation := range []string{"..floci-first", "..floci-second"} {
		if _, err := os.Stat(filepath.Join(root, generation)); !os.IsNotExist(err) {
			t.Fatalf("unused generation was not reclaimed: %s: %v", generation, err)
		}
	}
}

func TestProjectedSubpathsRequireExistingFileOrDirectoryBeforeWake(t *testing.T) {
	pod := volumeSpecPod()
	pod.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "config", MountPath: "/config", SubPath: "nested"}}
	pod.Spec.InitContainers = []corev1.Container{{Name: "initialize", VolumeMounts: []corev1.VolumeMount{{Name: "secret", MountPath: "/token", SubPath: "token"}}}}
	projections := map[string]map[string]volumeFile{"config": {"nested/file": {}}, "secret": {"token": {}}}
	if err := validateProjectedSubpaths(pod, projections); err != nil {
		t.Fatal(err)
	}
	delete(projections["secret"], "token")
	if err := validateProjectedSubpaths(pod, projections); err == nil || !strings.Contains(err.Error(), "initialize") {
		t.Fatalf("missing init subPath was accepted: %v", err)
	}
	projections["secret"]["token"] = volumeFile{}
	pod.Spec.Containers[0].VolumeMounts[0].SubPath = "nest"
	if err := validateProjectedSubpaths(pod, projections); err == nil {
		t.Fatal("prefix of an existing directory was accepted")
	}
}

func TestSubpathRejectsFIFOsWithoutOpeningThem(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux guest script")
	}
	base := t.TempDir()
	root := filepath.Join(base, "volume")
	if err := os.Mkdir(root, 0755); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.CommandContext(t.Context(), "mkfifo", filepath.Join(root, "fifo")).CombinedOutput(); err != nil {
		t.Fatalf("create FIFO: %v: %s", err, output)
	}
	output, err := runSubpathScript(t, root, filepath.Join(base, "pins", "app", "0"), "fifo", true, "")
	if err == nil || strings.Contains(output, "source=") {
		t.Fatalf("FIFO reached mount: %v: %s", err, output)
	}
}

func TestSubpathAcceptsSymlinksThatRemainInsideVolume(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "volume")
	if err := os.MkdirAll(filepath.Join(root, "directory"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "directory", "file"), []byte("safe-content"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("directory", filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	output, err := runSubpathScript(t, root, filepath.Join(base, "pins", "app", "0"), "link/file", true, "")
	if err != nil || !strings.Contains(output, "safe-content") {
		t.Fatalf("safe symlink was rejected: %v: %s", err, output)
	}
}

func TestSubpathCleanupRecognizesFileAndDirectoryBindMounts(t *testing.T) {
	for _, scenario := range []struct {
		name       string
		directory  bool
		mounted    bool
		extraField bool
	}{
		{name: "mounted file", mounted: true},
		{name: "mounted directory", directory: true, mounted: true},
		{name: "unmounted file"},
		{name: "unmounted directory", directory: true},
		{name: "path in root field only", extraField: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			base := t.TempDir()
			target := filepath.Join(base, "0")
			if scenario.directory {
				if err := os.Mkdir(target, 0755); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(target, []byte("mounted-data"), 0600); err != nil {
				t.Fatal(err)
			}
			mountinfo := filepath.Join(base, "mountinfo")
			data := "22 1 0:20 / " + target + "-prefix rw - tmpfs tmpfs rw\n"
			if scenario.mounted {
				data += "23 1 0:20 /source " + target + " rw - tmpfs tmpfs rw\n"
			}
			if scenario.extraField {
				data += "24 1 0:20 " + target + " /unrelated rw - tmpfs tmpfs rw\n"
			}
			if err := os.WriteFile(mountinfo, []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			// BusyBox's directory-only mountpoint result must not control cleanup.
			prefix := "mountpoint() { return 1; }; busybox() { test \"$1\" = umount; test -e \"$2\"; echo unmounted; }; "
			script := strings.ReplaceAll(subpathCleanupScript(target), "/proc/self/mountinfo", mountinfo)
			script = strings.ReplaceAll(script, "/bin/busybox ", "busybox ")
			output, err := exec.CommandContext(t.Context(), "sh", "-ec", prefix+script).CombinedOutput()
			if err != nil || strings.Contains(string(output), "unmounted") != scenario.mounted {
				t.Fatalf("wrong unmount decision: mounted=%t output=%s error=%v", scenario.mounted, output, err)
			}
			if _, err := os.Stat(target); !os.IsNotExist(err) {
				t.Fatalf("target was not removed after unmount: %v", err)
			}
		})
	}
}

func TestSubpathCleanupRetainsTargetWhenMountTableCannotBeRead(t *testing.T) {
	base := t.TempDir()
	target := filepath.Join(base, "0")
	if err := os.WriteFile(target, []byte("must-remain"), 0600); err != nil {
		t.Fatal(err)
	}
	script := strings.ReplaceAll(subpathCleanupScript(target), "/proc/self/mountinfo", filepath.Join(base, "missing-mountinfo"))
	if output, err := exec.CommandContext(t.Context(), "sh", "-ec", script).CombinedOutput(); err == nil {
		t.Fatalf("unreadable mount table was ignored: %s", output)
	}
	data, err := os.ReadFile(target)
	if err != nil || string(data) != "must-remain" {
		t.Fatalf("uncertain mount target was removed: %s: %v", data, err)
	}
}

func TestSubpathRestartUnmountsExistingFileBeforeRebinding(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux guest descriptor script")
	}
	base := t.TempDir()
	root := filepath.Join(base, "volume")
	target := filepath.Join(base, "pins", "app", "0")
	for _, directory := range []string{root, filepath.Dir(target)} {
		if err := os.MkdirAll(directory, 0755); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{target, filepath.Join(root, "file")} {
		if err := os.WriteFile(name, []byte("data"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	mountinfo := filepath.Join(base, "mountinfo")
	if err := os.WriteFile(mountinfo, []byte("23 1 0:20 /file "+target+" rw - tmpfs tmpfs rw\n"), 0600); err != nil {
		t.Fatal(err)
	}
	prefix := "mountpoint() { return 1; }; busybox() { echo \"$1\"; }; "
	script := strings.ReplaceAll(subpathSetupScript(root, target, "file", true), "/proc/self/mountinfo", mountinfo)
	script = strings.ReplaceAll(script, "/bin/busybox ", "busybox ")
	output, err := exec.CommandContext(t.Context(), "sh", "-ec", prefix+script).CombinedOutput()
	if err != nil || string(output) != "umount\nmount\n" {
		t.Fatalf("file mount was not unmounted before replacement: %s: %v", output, err)
	}
}
