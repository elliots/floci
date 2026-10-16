package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

type oomFixture struct {
	tracking           applicationOOMTracking
	cgroups, state     string
	parent, mounts     string
	kernelFileEmulator string
}

func newOOMFixture(t *testing.T) oomFixture {
	t.Helper()
	root := t.TempDir()
	f := oomFixture{cgroups: filepath.Join(root, "cgroups"), state: filepath.Join(root, "state"), mounts: filepath.Join(root, "mounts")}
	for _, dir := range []string{f.cgroups, f.state} {
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	f.tracking = buildApplicationOOMTracking("floci-application-g7-unit", f.cgroups, f.state, f.mounts)
	f.parent = filepath.Join(f.cgroups, f.tracking.Name)
	writeOOMFixtureFile(t, f.mounts, "none "+f.cgroups+" cgroup2 rw,relatime 0 0\n")
	writeOOMFixtureFile(t, filepath.Join(f.cgroups, "cgroup.controllers"), "cpuset cpu io memory pids\n")
	writeOOMFixtureFile(t, filepath.Join(f.cgroups, "cgroup.subtree_control"), "")
	// A normal directory cannot create or remove kernel virtual files. These
	// shell functions emulate those two operations while running the actual
	// generated setup/completion scripts against ordinary fixture files.
	f.kernelFileEmulator = `
mkdir() {
    command mkdir "$@" || return
    for last do :; done
    if test "$last" = ` + quote(f.parent) + `; then
        printf '%s\n' 'low 0' 'high 0' 'max 0' 'oom 0' 'oom_kill 0' >"$last/memory.events"
        : >"$last/cgroup.subtree_control"
    fi
}
rmdir() {
    test "$1" = ` + quote(f.parent) + ` || return 1
    if test -e "$1/application"; then return 1; fi
    command rm -f "$1/memory.events" "$1/cgroup.subtree_control"
    command rmdir "$1"
}
`
	return f
}

func writeOOMFixtureFile(t *testing.T, name, content string) {
	t.Helper()
	if err := os.WriteFile(name, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func (f oomFixture) run(t *testing.T, script string) (string, error) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "sh", "-c", f.kernelFileEmulator+script)
	output, err := cmd.CombinedOutput()
	return string(output), err
}

func (f oomFixture) setup(t *testing.T) {
	t.Helper()
	if output, err := f.run(t, f.tracking.Setup); err != nil {
		t.Fatalf("setup: %v: %s", err, output)
	}
}

func assertOOMFileAbsent(t *testing.T, name string) {
	t.Helper()
	if _, err := os.Stat(name); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected %s absent, got %v", name, err)
	}
}

func TestOOMTrackingNamesAreUniqueWithinLifecycleGeneration(t *testing.T) {
	first, second := newApplicationOOMTracking(23), newApplicationOOMTracking(23)
	if first.Name == second.Name || !strings.HasPrefix(first.Name, "floci-application-g23-") {
		t.Fatalf("application starts reused an identity: %q / %q", first.Name, second.Name)
	}
	if !slices.Equal(first.Args, []string{
		"--cgroup", "/" + first.Name + "/application",
		"--with-ns", "cgroup:",
		"--mount", "type=cgroup,src=cgroup,dst=/sys/fs/cgroup,options=nosuid:noexec:nodev:relatime:ro",
	}) {
		t.Fatalf("ctr task requires an isolated cgroup namespace and read-only view of its own limits: %v", first.Args)
	}
}

func TestOOMTrackingContainerCgroupOptionsSurviveSupervisorShell(t *testing.T) {
	tracking := newApplicationOOMTracking(2)
	command := append([]string{"printf", "%s\\n"}, tracking.Args...)
	cmd := exec.CommandContext(t.Context(), "sh", "-ec", joinCommand(command))
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("supervisor shell: %v: %s", err, output)
	}
	if !slices.Equal(strings.Split(strings.TrimSuffix(string(output), "\n"), "\n"), tracking.Args) {
		t.Fatalf("namespace or mount flags changed while passing through the shell: %s", output)
	}
}

func TestOOMTrackingSetupUsesFreshCountersAndClearsPriorResult(t *testing.T) {
	f := newOOMFixture(t)
	for _, name := range []string{"application.oom", "application.oom.pending", "application.oom-error", "application.oom-error.pending"} {
		writeOOMFixtureFile(t, filepath.Join(f.state, name), "prior application result")
	}
	f.setup(t)
	for _, dir := range []string{f.cgroups, f.parent} {
		data, err := os.ReadFile(filepath.Join(dir, "cgroup.subtree_control"))
		if err != nil || string(data) != "+cpu +memory\n" {
			t.Fatalf("CPU/memory controllers were not enabled in %s: %q (%v)", dir, data, err)
		}
	}
	for _, name := range []string{"application.oom", "application.oom.pending", "application.oom-error", "application.oom-error.pending"} {
		assertOOMFileAbsent(t, filepath.Join(f.state, name))
	}
}

func TestOOMTrackingCountsRealOOMBeforePublishingExit(t *testing.T) {
	for _, count := range []string{"1", "3", "18446744073709551615"} {
		t.Run(count, func(t *testing.T) {
			f := newOOMFixture(t)
			f.setup(t)
			writeOOMFixtureFile(t, filepath.Join(f.parent, "memory.events"), "oom 9\nmax 100\noom_kill "+count+"\noom_group_kill 0\n")
			script := "code=137\n" + f.tracking.Complete + "test -f " + quote(filepath.Join(f.state, "application.oom")) + " || exit 2\n" +
				"printf '%s\\n' \"$code\" >" + quote(filepath.Join(f.state, "application.exit"))
			if output, err := f.run(t, script); err != nil {
				t.Fatalf("completion: %v: %s", err, output)
			}
			data, err := os.ReadFile(filepath.Join(f.state, "application.oom"))
			if err != nil || string(data) != f.tracking.Name+" "+count+"\n" {
				t.Fatalf("wrong OOM evidence %q: %v", data, err)
			}
			assertOOMFileAbsent(t, filepath.Join(f.state, "application.oom-error"))
			assertOOMFileAbsent(t, f.parent)
			if output, err := f.run(t, f.tracking.Setup); err == nil {
				t.Fatalf("completed start identity was reused: %s", output)
			}
		})
	}
}

func TestOOMTrackingDoesNotMistakeSIGKILLOrPressureForOOMKill(t *testing.T) {
	f := newOOMFixture(t)
	f.setup(t)
	writeOOMFixtureFile(t, filepath.Join(f.parent, "memory.events"), "oom 2\nmax 123\nhigh 100\noom_kill 0\n")
	// A real SIGKILL exit has the same status as an OOM kill. Only the kernel
	// counter is evidence; pressure and allocation failures alone are not.
	script := "set +e\nsh -c 'kill -KILL $$'\ncode=$?\n" + f.tracking.Complete +
		"printf '%s\\n' \"$code\" >" + quote(filepath.Join(f.state, "application.exit"))
	if output, err := f.run(t, script); err != nil {
		t.Fatalf("completion: %v: %s", err, output)
	}
	data, err := os.ReadFile(filepath.Join(f.state, "application.exit"))
	if err != nil || string(data) != "137\n" {
		t.Fatalf("SIGKILL status %q: %v", data, err)
	}
	assertOOMFileAbsent(t, filepath.Join(f.state, "application.oom"))
	assertOOMFileAbsent(t, filepath.Join(f.state, "application.oom-error"))
}

func TestOOMTrackingMalformedCounterReportsUnknownWithoutInventingOOM(t *testing.T) {
	for _, counter := range []string{"", "oom 1\n", "oom_kill nope\n", "oom_kill -1\n", "oom_kill 1 extra\n", "oom_kill 0\noom_kill 1\n", "missing-file"} {
		t.Run(counter, func(t *testing.T) {
			f := newOOMFixture(t)
			f.setup(t)
			file := filepath.Join(f.parent, "memory.events")
			if counter == "missing-file" {
				if err := os.Remove(file); err != nil {
					t.Fatal(err)
				}
			} else {
				writeOOMFixtureFile(t, file, counter)
			}
			if output, err := f.run(t, f.tracking.Complete); err != nil {
				t.Fatalf("completion must publish explicit attribution error: %v: %s", err, output)
			}
			data, err := os.ReadFile(filepath.Join(f.state, "application.oom-error"))
			if err != nil || !strings.Contains(string(data), f.tracking.Name) {
				t.Fatalf("missing diagnostic %q: %v", data, err)
			}
			assertOOMFileAbsent(t, filepath.Join(f.state, "application.oom"))
			if _, err := os.Stat(f.parent); err != nil {
				t.Fatalf("unreadable counter was removed: %v", err)
			}
		})
	}
}

func TestOOMTrackingSetupRejectsIncompatibleHierarchyAndMissingControllers(t *testing.T) {
	for _, scenario := range []string{"cgroup-v1", "local-events", "no-cpu", "no-memory", "existing-parent"} {
		t.Run(scenario, func(t *testing.T) {
			f := newOOMFixture(t)
			switch scenario {
			case "cgroup-v1":
				writeOOMFixtureFile(t, f.mounts, "none "+f.cgroups+" cgroup rw 0 0\n")
			case "local-events":
				writeOOMFixtureFile(t, f.mounts, "none "+f.cgroups+" cgroup2 rw,memory_localevents,relatime 0 0\n")
			case "no-cpu":
				writeOOMFixtureFile(t, filepath.Join(f.cgroups, "cgroup.controllers"), "cpuset memory pids\n")
			case "no-memory":
				writeOOMFixtureFile(t, filepath.Join(f.cgroups, "cgroup.controllers"), "cpu pids\n")
			case "existing-parent":
				if err := os.Mkdir(f.parent, 0o700); err != nil {
					t.Fatal(err)
				}
				writeOOMFixtureFile(t, filepath.Join(f.parent, "memory.events"), "oom_kill 7\n")
			}
			if output, err := f.run(t, f.tracking.Setup); err == nil {
				t.Fatalf("unsafe setup succeeded: %s", output)
			}
			if scenario == "existing-parent" {
				data, err := os.ReadFile(filepath.Join(f.parent, "memory.events"))
				if err != nil || string(data) != "oom_kill 7\n" {
					t.Fatalf("existing cgroup was reset: %q (%v)", data, err)
				}
			}
		})
	}
}

func TestOOMTrackingDoesNotRemoveNonemptyCgroup(t *testing.T) {
	f := newOOMFixture(t)
	f.setup(t)
	child := filepath.Join(f.parent, "application")
	if err := os.Mkdir(child, 0o700); err != nil {
		t.Fatal(err)
	}
	output, err := f.run(t, f.tracking.Complete)
	if err != nil || !strings.Contains(output, "could not remove completed application cgroup") {
		t.Fatalf("cleanup failure was not diagnosed: %v: %s", err, output)
	}
	if _, err := os.Stat(child); err != nil {
		t.Fatalf("nonempty owned cgroup was deleted: %v", err)
	}
}

func TestOOMTrackingCounterAfterRestoreKeepsOriginalStartIdentity(t *testing.T) {
	f := newOOMFixture(t)
	f.setup(t)
	// RAM restore resumes the same supervisor script and cgroup. A controller
	// lifecycle generation change must not select another counter at completion.
	other := newApplicationOOMTracking(99)
	writeOOMFixtureFile(t, filepath.Join(f.parent, "memory.events"), "oom_kill 1\n")
	if output, err := f.run(t, f.tracking.Complete); err != nil {
		t.Fatalf("completion after restore: %v: %s", err, output)
	}
	data, err := os.ReadFile(filepath.Join(f.state, "application.oom"))
	if err != nil || strings.Contains(string(data), other.Name) || string(data) != fmt.Sprintf("%s 1\n", f.tracking.Name) {
		t.Fatalf("termination lost its application-start identity: %q (%v)", data, err)
	}
}
