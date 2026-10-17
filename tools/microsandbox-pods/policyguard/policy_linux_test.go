package policyguard

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

// This fixture runs only in an explicitly owned, privileged Linux container or
// disposable guest. Its cgroup root must belong to that isolated fixture.
func TestKernelNodeSocketMarkAndApplicationEscapeGuard(t *testing.T) {
	if mode := os.Getenv("FLOCI_POLICY_CHILD"); mode != "" {
		kernelSocketChild(t, mode)
		return
	}
	if os.Getenv("FLOCI_POLICY_KERNEL_TEST") != "1" {
		t.Skip("requires an owned privileged Linux fixture with a private cgroup namespace")
	}
	const application = "floci-application-g1-kerneltest"
	for _, name := range []string{"microsandbox-workload", application} {
		path := filepath.Join("/sys/fs/cgroup", name)
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := os.Remove(path); err != nil {
				t.Errorf("remove owned cgroup: %v", err)
			}
		})
	}
	for range 2 {
		if err := InstallNode(); err != nil {
			t.Fatal(err)
		}
		if err := InstallApplication(application, 45678); err != nil {
			t.Fatal(err)
		}
	}
	for _, mode := range []string{"node", "application"} {
		command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestKernelNodeSocketMarkAndApplicationEscapeGuard$", "-test.v")
		command.Env = append(os.Environ(), "FLOCI_POLICY_CHILD="+mode)
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("%s: %v\n%s", mode, err, output)
		}
	}
}

func kernelSocketChild(t *testing.T, mode string) {
	t.Helper()
	group := "floci-application-g1-kerneltest"
	if mode == "node" {
		group = "microsandbox-workload"
	}
	if err := os.WriteFile(filepath.Join("/sys/fs/cgroup", group, "cgroup.procs"), []byte(fmt.Sprint(os.Getpid())), 0600); err != nil {
		t.Fatal(err)
	}
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_DGRAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	mark, err := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_MARK)
	if err != nil {
		t.Fatal(err)
	}
	if mode == "node" {
		if mark != NodeMark {
			t.Fatalf("trusted node socket mark=%x", mark)
		}
		return
	}
	if mark != 0 {
		t.Fatalf("application inherited trusted node mark=%x", mark)
	}
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_MARK, NodeMark); !errors.Is(err, unix.EPERM) {
		t.Fatalf("application SO_MARK escape was not blocked: %v", err)
	}
	for _, port := range []int{8472, 45678} {
		address := &unix.SockaddrInet4{Port: port, Addr: [4]byte{127, 0, 0, 1}}
		if err := unix.Connect(fd, address); !errors.Is(err, unix.EPERM) {
			t.Fatalf("connected UDP transport escape on port %d: %v", port, err)
		}
		if err := unix.Sendto(fd, []byte("forged VXLAN"), 0, address); !errors.Is(err, unix.EPERM) {
			t.Fatalf("unconnected UDP transport escape on port %d: %v", port, err)
		}
	}
	if err := unix.Sendto(fd, []byte("allowed"), 0, &unix.SockaddrInet4{Port: 45679, Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		t.Fatalf("ordinary application UDP was blocked: %v", err)
	}
}
